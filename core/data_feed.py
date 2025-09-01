"""Módulo para gestionar el flujo de datos desde Binance."""
from __future__ import annotations
import asyncio
import random
from typing import Awaitable, Callable, Dict, Iterable, Any
from datetime import datetime, timezone, timedelta
import time
from binance_api.websocket import escuchar_velas
from binance_api.cliente import fetch_ohlcv_async
from core.utils.logger import configurar_logger
from core.utils import intervalo_a_segundos
from core.registro_metrico import registro_metrico
from core.supervisor import (
    tick,
    tick_data,
    supervised_task,
    registrar_reinicio_inactividad,
    registrar_reconexion_datafeed,
    beat,
    task_cooldown,
)
from core.notificador import crear_notificador_desde_env
from ccxt.base.errors import AuthenticationError, NetworkError

UTC = timezone.utc
log = configurar_logger('datafeed', modo_silencioso=False)

BACKFILL_MAX_CANDLES = 100

def validar_integridad_velas(symbol: str, tf: str, candles: Iterable[dict]) -> bool:
    timestamps = sorted(int(float(c['timestamp'])) for c in candles if 'timestamp' in c)
    if len(timestamps) < 2:
        return True
    intervalo_ms = intervalo_a_segundos(tf) * 1000
    dupes = gaps = desalineados = 0
    prev = timestamps[0]
    for curr in timestamps[1:]:
        diff = curr - prev
        if diff == 0:
            dupes += 1
        elif diff > intervalo_ms:
            if diff % intervalo_ms == 0:
                gaps += diff // intervalo_ms - 1
            else:
                desalineados += 1
        elif diff % intervalo_ms != 0:
            desalineados += 1
        prev = curr
    if dupes:
        registro_metrico.registrar('velas_duplicadas', {'symbol': symbol, 'tf': tf, 'count': dupes})
        log.warning(f'[{symbol}] {dupes} velas duplicadas detectadas en {tf}')
    if gaps:
        registro_metrico.registrar('velas_gap', {'symbol': symbol, 'tf': tf, 'count': gaps})
        log.warning(f'[{symbol}] Gap de {gaps} velas en {tf}')
    if desalineados:
        log.error(f'[{symbol}] Timestamps desalineados en {tf}: {desalineados}')
    return dupes == 0 and gaps == 0 and desalineados == 0


class DataFeed:
    """Maneja la recepción de velas de Binance en tiempo real."""

    def __init__(
        self,
        intervalo: str,
        monitor_interval: int = 5,
        max_restarts: int = 5,
        inactivity_intervals: int = 3,
        handler_timeout: float = 5,
        cancel_timeout: float = 5,
        queue_put_timeout: float = 2,
    ) -> None:
        self.intervalo = intervalo
        self.intervalo_segundos = intervalo_a_segundos(intervalo)
        self.inactivity_intervals = inactivity_intervals
        self.tiempo_inactividad = (
            self.intervalo_segundos * self.inactivity_intervals
        )
        self.cancel_timeout = cancel_timeout
        self.ping_interval = 60  # frecuencia fija de ping en segundos
        self.monitor_interval = max(1, monitor_interval)
        self.max_stream_restarts = max_restarts
        self._tasks: Dict[str, asyncio.Task] = {}
        self._last: Dict[str, datetime] = {}
        self._last_monotonic: Dict[str, float] = {}
        self._last_close_ts: Dict[str, int] = {}
        self._monitor_global_task: asyncio.Task | None = None
        self._ultimo_candle: Dict[str, dict] = {}
        self._handler_actual: Callable[[dict], Awaitable[None]] | None = None
        self._running = False
        self._cliente: Any | None = None
        self.notificador = crear_notificador_desde_env()
        self._symbols: list[str] = []
        self.reinicios_forzados_total = 0
        self.handler_timeout = handler_timeout
        self._reiniciando = False
        self._handler_timeouts: Dict[str, int] = {}
        self._mensajes_recibidos: Dict[str, int] = {}
        self._queue_discards: Dict[str, int] = {}
        self._reinicios_inactividad: Dict[str, int] = {}
        self._queues: Dict[str, asyncio.Queue] = {}
        self._consumer_tasks: Dict[str, asyncio.Task] = {}
        self.queue_put_timeout = queue_put_timeout
        registrar_reconexion_datafeed(self._reconectar_por_supervisor)

    @property
    def activos(self) ->list[str]:
        """Lista de símbolos con streams activos."""
        return list(self._symbols)
    
    @property
    def handler_timeouts(self) -> Dict[str, int]:
        """Contador de velas descartadas por exceder ``handler_timeout``."""
        return dict(self._handler_timeouts)
    

    async def stream(self, symbol: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        """Escucha las velas de ``symbol`` y reintenta ante fallos de conexión."""

        async def wrapper(candle: dict) -> None:
            await self._handle_candle(symbol, candle)

        await self._relanzar_stream(symbol, wrapper)

    def _should_enqueue_candle(self, symbol: str, candle: dict, force: bool = False) -> bool:
        """Valida que la vela esté cerrada y no se haya procesado ya."""
        if not candle.get('is_closed', True):
            log.debug(f'[{symbol}] Ignorando vela abierta: {candle}')
            return False
        ts = candle.get('timestamp')
        if ts is None:
            log.debug(f'[{symbol}] Vela sin timestamp: {candle}')
            return False
        if not force and self._last_close_ts.get(symbol) == ts:
            log.debug(f'[{symbol}] Vela duplicada ignorada: {ts}')
            return False
        self._last_close_ts[symbol] = ts
        return True
    
    async def _handle_candle(self, symbol: str, candle: dict, force: bool = False) -> None:
        """Valida y encola la vela actualizando heartbeats solo al éxito."""
        if not self._should_enqueue_candle(symbol, candle, force=force):
            return
        queue = self._queues[symbol]
        qsize = queue.qsize()
        if queue.maxsize and qsize > queue.maxsize * 0.8:
            log.warning(
                f"Queue {symbol} cerca del límite: {qsize}/{queue.maxsize}"
            )
        try:
            await asyncio.wait_for(
                queue.put(candle), timeout=self.queue_put_timeout
            )
        except asyncio.TimeoutError:
            log.warning("Queue bloqueada; descartando 1")
            self._queue_discards[symbol] = (
                self._queue_discards.get(symbol, 0) + 1
            )
            try:
                queue.get_nowait()
                queue.task_done()
            except asyncio.QueueEmpty:
                pass
            try:
                await asyncio.wait_for(
                    queue.put(candle), timeout=self.queue_put_timeout
                )
            except asyncio.TimeoutError:
                log.error(
                    f"Timeout en cola de {symbol}; reiniciando stream"
                )
                try:
                    await self.notificador.enviar_async(
                        f'⚠️ Timeout en cola de {symbol}; reiniciando stream',
                        'WARN',
                    )
                except Exception:
                    log.exception('Error enviando notificación de timeout de cola')
                    tick('data_feed')
                await self._reiniciar_stream(symbol)
                return
        self._last[symbol] = datetime.now(UTC)
        self._last_monotonic[symbol] = time.monotonic()
        self._mensajes_recibidos[symbol] = (
            self._mensajes_recibidos.get(symbol, 0) + 1
        )
        self._ultimo_candle[symbol] = {
            'timestamp': candle.get('timestamp'),
            'open': candle.get('open'),
            'high': candle.get('high'),
            'low': candle.get('low'),
            'close': candle.get('close'),
            'volume': candle.get('volume'),
        }
        log.debug(
            f'[{symbol}] Recibida vela: timestamp={candle.get("timestamp")}')
        tick_data(symbol)
    async def _backfill_candles(self, symbol: str) -> None:
        """Recupera y procesa velas faltantes tras una reconexión."""
        if not self._cliente:
            return
        last_ts = self._last_close_ts.get(symbol)
        if last_ts is None:
            return
        intervalo_ms = self.intervalo_segundos * 1000
        ahora = int(datetime.now(UTC).timestamp() * 1000)
        faltan = max(0, (ahora - last_ts) // intervalo_ms)
        if faltan <= 0:
            return
        limit = min(faltan + 1, BACKFILL_MAX_CANDLES)
        try:
            ohlcv = await fetch_ohlcv_async(
                self._cliente,
                symbol,
                self.intervalo,
                since=last_ts + 1,
                limit=limit,
            )
        except (AuthenticationError, NetworkError) as e:
            log.warning(f'❌ Error obteniendo backfill para {symbol}: {e}')
            return
        except Exception:
            log.exception(f'❌ Error inesperado obteniendo backfill para {symbol}')
            return
        registro_metrico.registrar(
            'velas_backfill',
            {'symbol': symbol, 'tf': self.intervalo, 'count': len(ohlcv)},
        )
        registro_metrico.registrar(
            'delta_backfill',
            {'symbol': symbol, 'tf': self.intervalo, 'delta': faltan},
        )
        for o in ohlcv:
            candle = {
                'symbol': symbol,
                'timestamp': o[0],
                'open': float(o[1]),
                'high': float(o[2]),
                'low': float(o[3]),
                'close': float(o[4]),
                'volume': float(o[5]),
                'is_closed': True,
            }
            prev = self._ultimo_candle.get(symbol)
            if prev and prev.get('timestamp') == candle['timestamp']:
                if any(prev.get(k) != candle[k] for k in ('open', 'high', 'low', 'close', 'volume')):
                    await self._handle_candle(symbol, candle, force=True)
            else:
                await self._handle_candle(symbol, candle)


    async def _consumer(
        self, symbol: str, handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Procesa las velas encoladas para ``symbol`` de forma asíncrona."""
        queue = self._queues[symbol]
        while self._running:
            candle = await queue.get()
            try:
                await asyncio.wait_for(
                    handler(candle), timeout=self.handler_timeout
                )
            except asyncio.TimeoutError:
                self._handler_timeouts[symbol] = (
                    self._handler_timeouts.get(symbol, 0) + 1
                )
                log.error(
                    f"Handler de {symbol} superó {self.handler_timeout}s; omitiendo vela"
                    f" (total {self._handler_timeouts[symbol]})"
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"Error procesando vela en {symbol}: {e}")
            finally:
                queue.task_done()

    async def _relanzar_stream(
        self, symbol: str, handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Mantiene un loop de conexión para ``symbol`` con backoff exponencial."""
        fallos_consecutivos = 0
        backoff = 1
        while True:
            try:
                beat(f'stream_{symbol}', 'backfill_start')
                await asyncio.wait_for(
                    self._backfill_candles(symbol), timeout=self.tiempo_inactividad
                )
                beat(f'stream_{symbol}', 'backfill_end')
                beat(f'stream_{symbol}', 'listen_start')
                await asyncio.wait_for(
                    escuchar_velas(
                        symbol,
                        self.intervalo,
                        handler,
                        self._last,
                        self.tiempo_inactividad,
                        self.ping_interval,
                        cliente=self._cliente,
                        mensaje_timeout=self.tiempo_inactividad,
                    ),
                    timeout=self.tiempo_inactividad + self.ping_interval + 5,
                )
                beat(f'stream_{symbol}', 'listen_end')
                log.warning(f'🔁 Conexión de {symbol} finalizada; reintentando en {backoff}s')
                fallos_consecutivos = 0
                backoff = 1
                await asyncio.sleep(backoff + random.random())
            except asyncio.TimeoutError:
                beat(f'stream_{symbol}', 'timeout')
                log.warning(
                    f'⌛ Timeout en stream {symbol}; reintentando en {backoff}s'
                )
                fallos_consecutivos += 1
                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, 60)
                continue
            except asyncio.CancelledError:
                beat(f'stream_{symbol}', 'cancel')
                raise
            except Exception as e:
                beat(f'stream_{symbol}', 'error')
                fallos_consecutivos += 1
                try:
                    if fallos_consecutivos == 1 or fallos_consecutivos % 5 == 0:
                        await self.notificador.enviar_async(
                            f'⚠️ Stream {symbol} en reconexión (intento {fallos_consecutivos})',
                            'WARN',
                        )
                except Exception:
                    log.exception('Error enviando notificación de reconexión')
                    tick('data_feed')
                    
                tick('data_feed')
                if isinstance(e, AuthenticationError) and (
                    getattr(e, 'code', None) == -2015 or '-2015' in str(e)
                ):
                    beat(f'stream_{symbol}', 'auth')
                    task_cooldown[f'stream_{symbol}'] = datetime.now(UTC) + timedelta(minutes=10)
                    log.error(f'❌ Auth fallida para {symbol}; esperando 600s')
                    await asyncio.sleep(600)
                    backoff = 1
                    continue

                if isinstance(e, (AuthenticationError, NetworkError)):
                    log.warning(
                        f'⚠️ Stream {symbol} falló: {e}. Reintentando en {backoff}s'
                    )
                else:
                    log.exception(
                        f'⚠️ Stream {symbol} falló de forma inesperada. Reintentando en {backoff}s'
                    )
                    
                if fallos_consecutivos >= self.max_stream_restarts:
                    log.error(
                        f'❌ Stream {symbol} superó el límite de {self.max_stream_restarts} intentos'
                    )
                    log.debug(
                        f"Stream {symbol} detenido tras {fallos_consecutivos} intentos"
                    )
                    try:
                        await self.notificador.enviar_async(
                            f'❌ Stream {symbol} superó el límite de {self.max_stream_restarts} intentos',
                            'CRITICAL',
                        )
                    except Exception:
                        log.exception('Error enviando notificación de límite de reconexiones')
                        tick('data_feed')
                    raise
                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, 60)


    async def iniciar(self) -> None:
        """Inicia el DataFeed usando la configuración almacenada."""
        if not self._symbols or not self._handler_actual:
            log.warning(
                "No se puede iniciar DataFeed: faltan símbolos o handler previo"
            )
            return
        await self.escuchar(self._symbols, self._handler_actual, self._cliente)

    
    async def _reconectar_por_supervisor(self, symbol: str) -> None:
        """Reinicia completamente el DataFeed ante falta global de datos."""
        if not self._running or self._reiniciando:
            return
        self._reiniciando = True
        log.critical(
            "🔁 Reinicio de DataFeed solicitado por supervisor (%s)", symbol
        )
        self.reinicios_forzados_total += 1
        try:
            symbols_previos = list(self._symbols)
            handler_prev = self._handler_actual
            cliente_prev = self._cliente
            await self.detener()
            if symbols_previos and handler_prev:
                self._symbols = symbols_previos
                self._handler_actual = handler_prev
                self._cliente = cliente_prev
                supervised_task(self.iniciar, "data_feed", max_restarts=0)
                log.info(
                    "✅ DataFeed reconectado con símbolos: %s", symbols_previos
                )
        finally:
            self._reiniciando = False


    async def _reiniciar_stream(self, symbol: str) -> None:
        """Reinicia el stream de ``symbol`` tras problemas en la cola."""
        task = self._tasks.get(symbol)
        if not task or not self._running:
            return
        if not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=self.cancel_timeout)
            except asyncio.TimeoutError:
                log.warning(f"⏱️ Timeout cancelando stream {symbol}")
            except Exception:
                log.exception(f'Error cancelando stream {symbol}')
        self._tasks[symbol] = supervised_task(
            lambda sym=symbol: self.stream(sym, self._handler_actual),
            f"stream_{symbol}",
            max_restarts=0,
        )
        log.info(f"📡 stream reiniciado para {symbol} por timeout de cola")
        tick_data(symbol, reinicio=True)
                

    async def _monitor_global_inactividad(self) -> None:
        """Vigila la actividad de los streams y los reinicia cuando es necesario."""
        try:
            while True:
                tick('data_feed')
                await asyncio.sleep(self.monitor_interval)
                if not self._running:
                    break
                if not self._tasks:
                    continue
                ahora = time.monotonic()

                for sym, task in list(self._tasks.items()):
                    ultimo = self._last_monotonic.get(sym)
                    inactivo = (
                        ultimo is not None
                        and (ahora - ultimo) > self.tiempo_inactividad
                    )
                    if task.done() or inactivo:
                        log.warning(
                            f"🔄 Stream {sym} inactivo o finalizado; relanzando",
                        )
                        log.debug(
                            f"Tareas antes de reinicio: {list(self._tasks.keys())}"
                        )
                        if not task.done():
                            task.cancel()
                            log.warning(f"Stream {sym} cancelado; se reiniciará")
                            try:
                                await asyncio.wait_for(
                                    task,
                                    timeout=self.cancel_timeout,
                                )
                            except asyncio.TimeoutError:
                                log.warning(
                                    f"⏱️ Timeout cancelando stream {sym}"
                                )
                            except Exception:
                                log.exception(f'Error cancelando stream {sym}')
                        self._tasks[sym] = supervised_task(
                            lambda sym=sym: self.stream(sym, self._handler_actual),
                            f"stream_{sym}",
                            max_restarts=0,
                        )
                        log.info("📡 stream reiniciado para %s", sym)
                        log.debug(
                            f"Tareas después de reinicio: {list(self._tasks.keys())}"
                        )
                        tick_data(sym, reinicio=True)
                        if inactivo:
                            registrar_reinicio_inactividad(sym)
                            self._reinicios_inactividad[sym] = (
                                self._reinicios_inactividad.get(sym, 0) + 1
                            )
        
        except asyncio.CancelledError:
            tick('data_feed')
            raise
        except Exception:
            log.exception("Error inesperado en _monitor_global_inactividad")
            tick('data_feed')

    async def escuchar(
        self,
        symbols: Iterable[str],
        handler: Callable[[dict], Awaitable[None]],
        cliente: Any | None = None,
    ) -> None:
        """Inicia un stream independiente por símbolo y espera a que finalicen.

        Si ``cliente`` se proporciona, se usará para recuperar velas perdidas tras
        una reconexión.
        """
        symbols_list = list(symbols)
        log.info("🎯 Símbolos recibidos: %s", symbols_list)
        await self.detener()
        self._handler_actual = handler
        self._symbols = symbols_list
        if cliente is not None:
            self._cliente = cliente
        self._running = True
        self._queues = {sym: asyncio.Queue(maxsize=100) for sym in self._symbols}
        for sym in self._symbols:
            self._consumer_tasks[sym] = supervised_task(
                lambda sym=sym: self._consumer(sym, handler),
                f"consumer_{sym}",
                max_restarts=0,
            )
        if (
            self._monitor_global_task is None
            or self._monitor_global_task.done()
        ):
            self._monitor_global_task = asyncio.create_task(
                self._monitor_global_inactividad()
            )
        for sym in self._symbols:
            if sym in self._tasks:
                log.warning(f'⚠️ Stream duplicado para {sym}. Ignorando.')
                continue
            self._tasks[sym] = supervised_task(
                lambda sym=sym: self.stream(sym, handler),
                f'stream_{sym}',
                max_restarts=0,
            )
        if self._tasks:
            while self._running and any(
                not t.done() for t in self._tasks.values()
            ):
                await asyncio.sleep(0.1)
        for nombre, tarea in self._tasks.items():
            estado = 'done' if tarea.done() else 'pending'
            if tarea.done() and tarea.exception():
                log.debug(
                    f"Tarea {nombre} finalizó con excepción: {tarea.exception()}"
                )
            else:
                log.debug(f"Tarea {nombre} estado: {estado}")
        consumer_list = list(self._consumer_tasks.values())
        for task in consumer_list:
            task.cancel()
        await asyncio.gather(*consumer_list, return_exceptions=True)
        self._consumer_tasks.clear()
        self._queues.clear()
        self._running = False
    async def detener(self) -> None:
        """Cancela todos los streams en ejecución."""
        tasks: list[asyncio.Task] = []
        if self._monitor_global_task:
            tasks.append(self._monitor_global_task)
        tasks.extend(self._tasks.values())
        tasks.extend(self._consumer_tasks.values())

        for task in tasks:
            task.cancel()

        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=self.cancel_timeout,
                )
            except asyncio.TimeoutError:
                log.warning("🧟 Timeout cancelando tareas (tarea zombie)")
            except Exception:
                log.exception('Error cancelando tareas')
            tasks.clear()

        self._monitor_global_task = None
        self._tasks.clear()
        for sym in set(
            list(self._mensajes_recibidos.keys())
            + list(self._handler_timeouts.keys())
            + list(self._queue_discards.keys())
            + list(self._reinicios_inactividad.keys())
        ):
            log.info(
                "📊 %s: mensajes=%d, timeouts=%d, descartes=%d, reinicios_inactividad=%d",
                sym,
                self._mensajes_recibidos.get(sym, 0),
                self._handler_timeouts.get(sym, 0),
                self._queue_discards.get(sym, 0),
                self._reinicios_inactividad.get(sym, 0),
            )
        self._consumer_tasks.clear()
        self._queues.clear()
        self._last.clear()
        self._last_monotonic.clear()
        self._mensajes_recibidos.clear()
        self._queue_discards.clear()
        self._reinicios_inactividad.clear()
        self._symbols = []
        self._running = False
