"""Módulo para gestionar el flujo de datos desde Binance."""
from __future__ import annotations
import asyncio
from typing import Awaitable, Callable, Dict, Iterable, Any
from datetime import datetime
from binance_api.websocket import escuchar_velas, escuchar_velas_combinado
from core.utils.logger import configurar_logger
from core.utils import intervalo_a_segundos
from core.supervisor import (
    tick,
    tick_data,
    supervised_task,
    registrar_reinicio_inactividad,
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
        inactivity_intervals: int = 12,
        usar_stream_combinado: bool = False,
        handler_timeout: float = 5,
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
        self.reinicios_forzados_total = 0
        self.handler_timeout = handler_timeout

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
            try:
                await asyncio.wait_for(handler(candle), timeout=self.handler_timeout)
            except asyncio.TimeoutError:
                log.error(
                    f'Handler de {symbol} superó {self.handler_timeout}s; omitiendo vela'
                )

        await self._relanzar_stream(symbol, wrapper)

    async def _stream_combinado(
        self, symbols: Iterable[str], handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Escucha múltiples símbolos en un único WebSocket."""

        async def wrapper(symbol: str, candle: dict) -> None:
            self._last[symbol] = datetime.utcnow()
            log.info(f'[{symbol}] Recibida vela: timestamp={candle.get("timestamp")}')
            tick_data(symbol)
            try:
                await asyncio.wait_for(handler(candle), timeout=self.handler_timeout)
            except asyncio.TimeoutError:
                log.error(
                    f'Handler de {symbol} superó {self.handler_timeout}s; omitiendo vela'
                )

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
            mensaje_timeout=self.tiempo_inactividad,
        )

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
                if self.usar_stream_combinado:
                    task = self._tasks.get("combined")
                    if not task:
                        continue
                    inactivos = [
                        sym
                        for sym in self._symbols
                        if (
                            self._last.get(sym)
                            and (ahora - self._last[sym]).total_seconds() > self.tiempo_inactividad
                        )
                    ]
                    if inactivos:
                        log.warning(f"⏸ Símbolos inactivos detectados: {inactivos}")
                    all_inactivos = len(inactivos) == len(self._symbols) and bool(self._symbols)
                    if task.done() or inactivos:
                        log.debug(f"Estado task.done() antes de cancelar: {task.done()}")
                        if all_inactivos:
                            log.critical(
                                "⛔ Todos los símbolos sin datos — Forzando reinicio completo de stream combinado"
                            )
                            self.reinicios_forzados_total += 1
                            try:
                                await self.notificador.enviar_async(
                                    '🔄 Reinicio forzado de stream combinado por inactividad global',
                                    'CRITICAL',
                                )
                            except Exception:
                                tick('data_feed')
                                pass
                        if self._handler_actual is None:
                            log.error("Handler actual es None; no se puede reiniciar stream combinado")
                            continue
                        log.debug(f"Tareas antes de reinicio: {list(self._tasks.keys())}")
                        if not task.done():
                            log.info("Cancelando tarea 'combined'")
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                            log.debug(
                                f"Tarea 'combined' cancelada: cancelled={task.cancelled()} done={task.done()}"
                            )
                        nueva = supervised_task(
                            lambda: self._stream_combinado(self._symbols, self._handler_actual),
                            'stream_combined',
                            max_restarts=0,
                        )
                        self._tasks['combined'] = nueva
                        inicio = datetime.utcnow()
                        log.info("📡 _stream_combinado reiniciado para %s", self._symbols)
                        log.debug(
                            f"Inicio: {inicio.isoformat()} nombre={nueva.get_name()} done={nueva.done()} id={id(nueva)}"
                        )
                        log.debug(
                            f"Tareas después de reinicio: {list(self._tasks.keys())}"
                        )
                        for sym in inactivos:
                            registrar_reinicio_inactividad(sym)
                    continue

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
                            await asyncio.gather(task, return_exceptions=True)
                        self._tasks[sym] = supervised_task(
                            lambda sym=sym: self.stream(sym, self._handler_actual),
                            f"stream_{sym}",
                            max_restarts=0,
                        )
                        log.info("📡 stream reiniciado para %s", sym)
                        log.debug(
                            f"Tareas después de reinicio: {list(self._tasks.keys())}"
                        )
                        if inactivo:
                            registrar_reinicio_inactividad(sym)
        
        except asyncio.CancelledError:
            tick('data_feed')
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
        symbols_list = list(symbols)
        log.info("🎯 Símbolos recibidos: %s", symbols_list)
        await self.detener()
        self._handler_actual = handler
        self._symbols = symbols_list
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
            tarea = supervised_task(
                lambda: self._stream_combinado(self._symbols, handler),
                'stream_combined',
                max_restarts=0,
            )
            self._tasks['combined'] = tarea
            inicio = datetime.utcnow()
            log.info(
                f"🚀 _stream_combinado lanzado {inicio.isoformat()} nombre={tarea.get_name()} done={tarea.done()} id={id(tarea)}"
            )
        else:
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
        self._running = False
    async def detener(self) ->None:
        log.info('➡️ Entrando en detener()')
        """Cancela todos los streams en ejecución."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
        self._last.clear()
        self._symbols = []
        if self._monitor_global_task and not self._monitor_global_task.done():
            self._monitor_global_task.cancel()
            await asyncio.gather(self._monitor_global_task, return_exceptions=True)
        self._monitor_global_task = None
        self._running = False
