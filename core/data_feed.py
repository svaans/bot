"""Módulo para gestionar el flujo de datos desde Binance."""
from __future__ import annotations
import asyncio
from typing import Awaitable, Callable, Dict, Iterable, Any
from datetime import datetime
from binance_api.websocket import escuchar_velas, escuchar_velas_combinado
from core.utils.logger import configurar_logger
from core.utils import intervalo_a_segundos
from core.supervisor import tick, tick_data, supervised_task
from core.notificador import crear_notificador_desde_env
log = configurar_logger('datafeed', modo_silencioso=True)


class DataFeed:
    """Maneja la recepción de velas de Binance en tiempo real."""

    def __init__(
        self,
        intervalo: str,
        monitor_interval: int = 5,
        max_restarts: int = 5,
        inactivity_intervals: int = 4,
        usar_stream_combinado: bool = False,
    ) -> None:
        log.info('➡️ Entrando en __init__()')
        self.intervalo = intervalo
        self.intervalo_segundos = intervalo_a_segundos(intervalo)
        self.inactivity_intervals = inactivity_intervals
        self.tiempo_inactividad = max(
            self.intervalo_segundos * self.inactivity_intervals, 60
        )
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
        self.usar_stream_combinado = usar_stream_combinado
        self._symbols: list[str] = []

    @property
    def activos(self) ->list[str]:
        log.info('➡️ Entrando en activos()')
        """Lista de símbolos con streams activos."""
        return list(self._symbols)
    

    async def stream(self, symbol: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        log.info('➡️ Entrando en stream()')
        """Escucha las velas de ``symbol`` y reintenta ante fallos de conexión."""

        async def wrapper(candle: dict) -> None:
            self._last[symbol] = datetime.utcnow()
            log.info(f'[{symbol}] Recibida vela: timestamp={candle.get("timestamp")}')
            tick_data(symbol)
            await handler(candle)

        monitor = asyncio.create_task(self._monitor_activity(symbol))
        try:
            await self._relanzar_stream(symbol, wrapper)
        finally:
            monitor.cancel()

    async def _stream_combinado(
        self, symbols: Iterable[str], handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Escucha múltiples símbolos en un único WebSocket."""

        async def wrapper(symbol: str, candle: dict) -> None:
            self._last[symbol] = datetime.utcnow()
            log.info(f'[{symbol}] Recibida vela: timestamp={candle.get("timestamp")}')
            tick_data(symbol)
            await handler(candle)

        handlers: Dict[str, Callable[[dict], Awaitable[None]]] = {}
        for sym in symbols:
            async def h(candle, s=sym):
                await wrapper(s, candle)

            handlers[sym] = h

        await escuchar_velas_combinado(
            list(symbols),
            self.intervalo,
            handlers,
            self._last,
            self.tiempo_inactividad,
            self.ping_interval,
            cliente=self._cliente,
        )

    async def _relanzar_stream(
        self, symbol: str, handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Mantiene un loop de conexión para ``symbol`` reiniciando automáticamente."""
        attempts = 0
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
                )
                log.warning(f'🔁 Conexión de {symbol} finalizada; reintentando en 1s')
                attempts = 0
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f'⚠️ Stream {symbol} falló: {e}. Reintentando en 5s')
                attempts += 1
                try:
                    if attempts == 1 or attempts % 5 == 0:
                        await self.notificador.enviar_async(
                            f'⚠️ Stream {symbol} en reconexión (intento {attempts})',
                            'WARN',
                        )
                except Exception:
                    pass
                if attempts >= self.max_stream_restarts:
                    log.error(
                        f'❌ Stream {symbol} superó el límite de {self.max_stream_restarts} intentos'
                    )
                    log.debug(
                        f"Stream {symbol} detenido tras {attempts} intentos"
                    )
                    try:
                        await self.notificador.enviar_async(
                            f'❌ Stream {symbol} superó el límite de {self.max_stream_restarts} intentos',
                            'CRITICAL',
                        )
                    except Exception:
                        pass
                    raise
                await asyncio.sleep(5)
                
    async def _monitor_activity(self, symbol: str) -> None:
        """Verifica periódicamente que se sigan recibiendo velas.

        Si pasan ``self.tiempo_inactividad`` segundos (≈ varios intervalos, configurable)
        sin nuevas velas, cancela la tarea del stream para que ``escuchar`` lo reinicie.
        """
        while True:
            await asyncio.sleep(self.monitor_interval)
            ultimo = self._last.get(symbol)
            if (
                ultimo
                and (datetime.utcnow() - ultimo).total_seconds() > self.tiempo_inactividad
            ):
                log.warning(
                    f'⚠️ Sin velas de {symbol} desde hace más de {self.tiempo_inactividad}s; reiniciando'
                )
                task = self._tasks.get(symbol)
                if task and not task.done():
                    task.cancel()
                break

    async def _monitor_global_inactividad(self) -> None:
        """Reinicia todos los streams si ninguno recibe datos."""
        try:
            while True:
                await asyncio.sleep(self.monitor_interval)
                if not self._running:
                    break
                if not self._tasks:
                    continue
                ahora = datetime.utcnow()
                if self.usar_stream_combinado:
                    task = self._tasks.get('combined')
                    inactivos = any(
                        (
                            self._last.get(sym)
                            and (ahora - self._last.get(sym)).total_seconds()
                            > self.tiempo_inactividad
                        )
                        for sym in self._symbols
                    )
                    if not task:
                        continue
                    if task.done() or inactivos:
                        log.warning(
                            '🔄 Stream combinado inactivo o finalizado; relanzando'
                        )
                        if task and not task.done():
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                        self._tasks['combined'] = supervised_task(
                            lambda: self._stream_combinado(
                                self._symbols, self._handler_actual
                            ),
                            'stream_combined',
                            max_restarts=self.max_stream_restarts,
                        )
                else:
                    for sym, task in list(self._tasks.items()):
                        ultimo = self._last.get(sym)
                        if task.done() or (
                            ultimo
                            and (ahora - ultimo).total_seconds()
                            > self.tiempo_inactividad
                        ):
                            log.warning(
                                f'🔄 Stream {sym} inactivo o finalizado; relanzando'
                            )
                            if task and not task.done():
                                task.cancel()
                                await asyncio.gather(task, return_exceptions=True)
                            self._tasks[sym] = supervised_task(
                                lambda sym=sym: self.stream(
                                    sym, self._handler_actual
                                ),
                                f'stream_{sym}',
                                max_restarts=self.max_stream_restarts,
                            )
                if self._last and all(
                    (
                        ahora - ts
                    ).total_seconds() > self.tiempo_inactividad
                    for ts in self._last.values()
                ):
                    log.critical(
                        "\u26a0\ufe0f Todos los streams inactivos; reiniciando"
                    )
                    await self.detener()
                    self._monitor_global_task = None
                    await self.escuchar(self._last.keys(), self._handler_actual)
                    break
        except asyncio.CancelledError:
            pass

    async def escuchar(
        self,
        symbols: Iterable[str],
        handler: Callable[[dict], Awaitable[None]],
        cliente: Any | None = None,
    ) -> None:
        log.info('➡️ Entrando en escuchar()')
        """Inicia un stream por símbolo o uno combinado y espera a que finalicen.

        Si ``cliente`` se proporciona, se usará para recuperar velas perdidas tras
        una reconexión.
        """
        await self.detener()
        self._handler_actual = handler
        self._symbols = list(symbols)
        if cliente is not None:
            self._cliente = cliente
        self._running = True
        if (
            self._monitor_global_task is None
            or self._monitor_global_task.done()
        ):
            self._monitor_global_task = asyncio.create_task(
                self._monitor_global_inactividad()
            )
        if self.usar_stream_combinado:
            self._tasks['combined'] = supervised_task(
                lambda: self._stream_combinado(self._symbols, handler),
                'stream_combined',
                max_restarts=self.max_stream_restarts,
            )
        else:
            for sym in self._symbols:
                if sym in self._tasks:
                    log.warning(f'⚠️ Stream duplicado para {sym}. Ignorando.')
                    continue
                self._tasks[sym] = supervised_task(
                    lambda sym=sym: self.stream(sym, handler),
                    f'stream_{sym}',
                    max_restarts=self.max_stream_restarts,
                )
        if self._tasks:
            await asyncio.gather(*self._tasks.values())
        for nombre, tarea in self._tasks.items():
            estado = 'done' if tarea.done() else 'pending'
            if tarea.done() and tarea.exception():
                log.debug(
                    f"Tarea {nombre} finalizó con excepción: {tarea.exception()}"
                )
            else:
                log.debug(f"Tarea {nombre} estado: {estado}")
        self._running = False
    async def detener(self) ->None:
        log.info('➡️ Entrando en detener()')
        """Cancela todos los streams en ejecución."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
        self._symbols = []
        if self._monitor_global_task and not self._monitor_global_task.done():
            self._monitor_global_task.cancel()
            await asyncio.gather(self._monitor_global_task, return_exceptions=True)
        self._monitor_global_task = None
        self._running = False
