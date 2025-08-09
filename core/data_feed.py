"""Módulo para gestionar el flujo de datos desde Binance."""
from __future__ import annotations
import asyncio
from typing import Awaitable, Callable, Dict, Iterable, Any
from datetime import datetime
from binance_api.websocket import escuchar_velas
from core.utils.logger import configurar_logger
from core.utils import intervalo_a_segundos
from core.supervisor import (
    tick,
    tick_data,
    supervised_task,
    registrar_reinicio_inactividad,
    registrar_reconexion_datafeed,
)
from core.notificador import crear_notificador_desde_env
log = configurar_logger('datafeed', modo_silencioso=True)


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
    ) -> None:
        log.info('➡️ Entrando en __init__()')
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
        self._monitor_global_task: asyncio.Task | None = None
        self._handler_actual: Callable[[dict], Awaitable[None]] | None = None
        self._running = False
        self._cliente: Any | None = None
        self.notificador = crear_notificador_desde_env()
        self._symbols: list[str] = []
        self.reinicios_forzados_total = 0
        self.handler_timeout = handler_timeout
        self._reiniciando = False
        self._handler_timeouts: Dict[str, int] = {}
        self._queues: Dict[str, asyncio.Queue] = {}
        self._consumer_tasks: Dict[str, asyncio.Task] = {}
        registrar_reconexion_datafeed(self._reconectar_por_supervisor)

    @property
    def activos(self) ->list[str]:
        log.info('➡️ Entrando en activos()')
        """Lista de símbolos con streams activos."""
        return list(self._symbols)
    
    @property
    def handler_timeouts(self) -> Dict[str, int]:
        """Contador de velas descartadas por exceder ``handler_timeout``."""
        return dict(self._handler_timeouts)
    

    async def stream(self, symbol: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        log.info('➡️ Entrando en stream()')
        """Escucha las velas de ``symbol`` y reintenta ante fallos de conexión."""

        async def wrapper(candle: dict) -> None:
            self._last[symbol] = datetime.utcnow()
            log.info(f'[{symbol}] Recibida vela: timestamp={candle.get("timestamp")}')
            tick_data(symbol)
            try:
                self._queues[symbol].put_nowait(candle)
            except asyncio.QueueFull:
                self._handler_timeouts[symbol] = (
                    self._handler_timeouts.get(symbol, 0) + 1
                )
                log.error(
                    f"Queue de {symbol} llena; omitiendo vela"
                    f" (total {self._handler_timeouts[symbol]})"
                )

        await self._relanzar_stream(symbol, wrapper)


    async def _consumer(
        self, symbol: str, handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Procesa las velas encoladas para ``symbol`` de forma asíncrona."""
        queue = self._queues[symbol]
        loop = asyncio.get_running_loop()
        while self._running:
            candle = await queue.get()
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: asyncio.run(handler(candle))),
                    timeout=self.handler_timeout,
                )
            except asyncio.TimeoutError:
                self._handler_timeouts[symbol] = (
                    self._handler_timeouts.get(symbol, 0) + 1
                )
                log.error(
                    f"Handler de {symbol} superó {self.handler_timeout}s; omitiendo vela"
                    f" (total {self._handler_timeouts[symbol]})"
                )
            finally:
                queue.task_done()

    async def _relanzar_stream(
        self, symbol: str, handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Mantiene un loop de conexión para ``symbol`` reiniciando automáticamente."""
        fallos_consecutivos = 0
        while True:
            try:
                await escuchar_velas(
                    symbol,
                    self.intervalo,
                    handler,
                    self._last,
                    self.tiempo_inactividad,
                    self.ping_interval,
                    cliente=self._cliente,
                    mensaje_timeout=self.tiempo_inactividad,
                )
                log.warning(f'🔁 Conexión de {symbol} finalizada; reintentando en 1s')
                fallos_consecutivos = 0
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f'⚠️ Stream {symbol} falló: {e}. Reintentando en 5s')
                fallos_consecutivos += 1
                try:
                    if fallos_consecutivos == 1 or fallos_consecutivos % 5 == 0:
                        await self.notificador.enviar_async(
                            f'⚠️ Stream {symbol} en reconexión (intento {fallos_consecutivos})',
                            'WARN',
                        )
                except Exception:
                    tick('data_feed')
                    pass
                tick('data_feed')
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
                        tick('data_feed')
                        pass
                    raise
                await asyncio.sleep(5)

    
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
            await self.detener()
        finally:
            self._reiniciando = False
                

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
                ahora = datetime.utcnow()

                for sym, task in list(self._tasks.items()):
                    ultimo = self._last.get(sym)
                    inactivo = (
                        ultimo
                        and (ahora - ultimo).total_seconds() > self.tiempo_inactividad
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
                            log.debug(f"Stream {sym} cancelado; se reiniciará")
                            try:
                                await asyncio.wait_for(
                                    asyncio.gather(task, return_exceptions=True),
                                    timeout=self.cancel_timeout,
                                )
                            except asyncio.TimeoutError:
                                log.warning(
                                    f"⏱️ Timeout cancelando stream {sym}"
                                )
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
        
        except asyncio.CancelledError:
            tick('data_feed')
            raise
        except Exception as e:
            log.exception(
                "Error inesperado en _monitor_global_inactividad: %s", e
            )
            tick('data_feed')

    async def escuchar(
        self,
        symbols: Iterable[str],
        handler: Callable[[dict], Awaitable[None]],
        cliente: Any | None = None,
    ) -> None:
        log.info('➡️ Entrando en escuchar()')
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
        for task in self._consumer_tasks.values():
            task.cancel()
        await asyncio.gather(*self._consumer_tasks.values(), return_exceptions=True)
        self._consumer_tasks.clear()
        self._queues.clear()
        self._running = False
    async def detener(self) ->None:
        log.info('➡️ Entrando en detener()')
        """Cancela todos los streams en ejecución."""
        if self._monitor_global_task and not self._monitor_global_task.done():
            self._monitor_global_task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        self._monitor_global_task, return_exceptions=True
                    ),
                    timeout=self.cancel_timeout,
                )
            except asyncio.TimeoutError:
                log.warning("🧟 Timeout cancelando monitor global (tarea zombie)")
        self._monitor_global_task = None

        for task in self._tasks.values():
            task.cancel()
        for nombre, task in list(self._tasks.items()):
            if task.done():
                continue
            try:
                await asyncio.wait_for(
                    asyncio.gather(task, return_exceptions=True),
                    timeout=self.cancel_timeout,
                )
            except asyncio.TimeoutError:
                log.warning(
                    f"🧟 Timeout cancelando stream {nombre} (tarea zombie)"
                )
        self._tasks.clear()
        for task in self._consumer_tasks.values():
            task.cancel()
        for nombre, task in list(self._consumer_tasks.items()):
            if task.done():
                continue
            try:
                await asyncio.wait_for(
                    asyncio.gather(task, return_exceptions=True),
                    timeout=self.cancel_timeout,
                )
            except asyncio.TimeoutError:
                log.warning(
                    f"🧟 Timeout cancelando consumer {nombre} (tarea zombie)"
                )
        self._consumer_tasks.clear()
        self._queues.clear()
        self._last.clear()
        self._symbols = []
        self._running = False
