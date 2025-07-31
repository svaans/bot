"""Módulo para gestionar el flujo de datos desde Binance."""
from __future__ import annotations
import asyncio
from typing import Awaitable, Callable, Dict, Iterable
from datetime import datetime
from binance_api.websocket import escuchar_velas
from core.utils.logger import configurar_logger
from core.utils import intervalo_a_segundos
from core.supervisor import tick, tick_data, supervised_task
log = configurar_logger('datafeed', modo_silencioso=True)


class DataFeed:
    """Maneja la recepción de velas de Binance en tiempo real."""

    def __init__(self, intervalo: str, monitor_interval: int = 5, max_restarts: int = 5) -> None:
        log.info('➡️ Entrando en __init__()')
        self.intervalo = intervalo
        self.intervalo_segundos = intervalo_a_segundos(intervalo)
        self.tiempo_inactividad = max(self.intervalo_segundos * 2, 60)
        self.ping_interval = self.intervalo_segundos
        self.monitor_interval = monitor_interval
        self.max_stream_restarts = max_restarts
        self._tasks: Dict[str, asyncio.Task] = {}
        self._last: Dict[str, datetime] = {}

    @property
    def activos(self) ->list[str]:
        log.info('➡️ Entrando en activos()')
        """Lista de símbolos con streams activos."""
        return list(self._tasks.keys())

    async def stream(self, symbol: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        log.info('➡️ Entrando en stream()')
        """Escucha las velas de ``symbol`` y reintenta ante fallos de conexión."""

        async def wrapper(candle: dict) -> None:
            self._last[symbol] = datetime.utcnow()
            log.info(f'[{symbol}] Recibida vela: timestamp={candle.get("timestamp")}')
            tick_data(symbol)
            await handler(candle)

        monitor = asyncio.create_task(self._monitor_activity(symbol))
        attempts = 0
        try:
            while True:
                try:
                    await escuchar_velas(
                        symbol,
                        self.intervalo,
                        wrapper,
                        self._last,
                        self.tiempo_inactividad,
                        self.ping_interval,
                    )
                    break
                except Exception as e:
                    log.warning(
                        f'⚠️ Stream {symbol} falló: {e}. Reintentando en 5s'
                    )
                    attempts += 1
                    if attempts >= self.max_stream_restarts:
                        log.error(
                            f'❌ Stream {symbol} superó el límite de {self.max_stream_restarts} intentos'
                        )
                        raise
                    await asyncio.sleep(5)
        finally:
            monitor.cancel()

    async def _monitor_activity(self, symbol: str) -> None:
        """Verifica periódicamente que se sigan recibiendo velas.

        Si pasan ``self.tiempo_inactividad`` segundos sin nuevas velas, cancela
        la tarea del stream para que ``escuchar`` lo reinicie.
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

    async def escuchar(self, symbols: Iterable[str], handler: Callable[[
        dict], Awaitable[None]]) ->None:
        log.info('➡️ Entrando en escuchar()')
        """Inicia un stream por cada símbolo y espera a que todos finalicen."""
        for sym in symbols:
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
    async def detener(self) ->None:
        log.info('➡️ Entrando en detener()')
        """Cancela todos los streams en ejecución."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
