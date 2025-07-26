"""Módulo para gestionar el flujo de datos desde Binance."""
from __future__ import annotations
import asyncio
from typing import Awaitable, Callable, Dict, Iterable
from datetime import datetime
from binance_api.websocket import escuchar_velas
from core.utils.logger import configurar_logger
from core.supervisor import tick, tick_data
log = configurar_logger('datafeed', modo_silencioso=True)


class DataFeed:
    """Maneja la recepción de velas de Binance en tiempo real."""

    def __init__(self, intervalo: str) ->None:
        log.info('➡️ Entrando en __init__()')
        self.intervalo = intervalo
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
        try:
            while True:
                try:
                    await escuchar_velas(symbol, self.intervalo, wrapper)
                    break
                except Exception as e:
                    log.warning(
                        f'⚠️ Stream {symbol} falló: {e}. Reintentando en 5s'
                    )
                    await asyncio.sleep(5)
        finally:
            monitor.cancel()

    async def _monitor_activity(self, symbol: str) -> None:
        """Verifica periódicamente que se sigan recibiendo velas.

        Si pasan 60 segundos sin nuevas velas, cancela la tarea del stream para
        que ``escuchar`` lo reinicie.
        """
        while True:
            await asyncio.sleep(5)
            ultimo = self._last.get(symbol)
            if ultimo and (datetime.utcnow() - ultimo).total_seconds() > 60:
                log.warning(
                    f'⚠️ Sin velas de {symbol} desde hace más de 60s; reiniciando'
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
            self._tasks[sym] = asyncio.create_task(self.stream(sym, handler))
        while self._tasks:
            tareas_actuales = list(self._tasks.items())
            resultados = await asyncio.gather(
                *[t for _, t in tareas_actuales], return_exceptions=True
            )
            reiniciar = {}
            for (sym, task), resultado in zip(tareas_actuales, resultados):
                if isinstance(resultado, asyncio.CancelledError):
                    log.warning(
                        f'⚠️ Stream {sym} cancelado. Reiniciando en 5s'
                    )
                    await asyncio.sleep(5)
                    reiniciar[sym] = asyncio.create_task(self.stream(sym, handler))
                    continue
                if isinstance(resultado, Exception):
                    log.error(
                        f'⚠️ Stream {sym} finalizó con excepción: {resultado}. Reiniciando en 5s'
                    )
                    await asyncio.sleep(5)
                    reiniciar[sym] = asyncio.create_task(self.stream(sym, handler))
            if reiniciar:
                self._tasks.update(reiniciar)
            else:
                break
    async def detener(self) ->None:
        log.info('➡️ Entrando en detener()')
        """Cancela todos los streams en ejecución."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
