"""Módulo para gestionar el flujo de datos desde Binance."""
from __future__ import annotations
import asyncio
from typing import Awaitable, Callable, Dict, Iterable
from binance_api.websocket import escuchar_velas
from core.utils.logger import configurar_logger
log = configurar_logger('datafeed', modo_silencioso=True)


class DataFeed:
    """Maneja la recepción de velas de Binance en tiempo real."""

    def __init__(self, intervalo: str) ->None:
        self.intervalo = intervalo
        self._tasks: Dict[str, asyncio.Task] = {}

    @property
    def activos(self) ->list[str]:
        """Lista de símbolos con streams activos."""
        return list(self._tasks.keys())

    async def stream(self, symbol: str, handler: Callable[[dict], Awaitable
        [None]]) ->None:
        """Escucha las velas de ``symbol`` y reintenta ante fallos de conexión."""
        while True:
            try:
                await escuchar_velas(symbol, self.intervalo, handler)
                break
            except Exception as e:
                log.warning(
                    f'⚠️ Stream {symbol} falló: {e}. Reintentando en 5s')
                await asyncio.sleep(5)

    async def escuchar(self, symbols: Iterable[str], handler: Callable[[
        dict], Awaitable[None]]) ->None:
        """Inicia un stream por cada símbolo y espera a que todos finalicen."""
        for sym in symbols:
            if sym in self._tasks:
                log.warning(f'⚠️ Stream duplicado para {sym}. Ignorando.')
                continue
            self._tasks[sym] = asyncio.create_task(self.stream(sym, handler))
        while self._tasks:
            tareas_actuales = list(self._tasks.items())
            resultados = await asyncio.gather(*[t for _, t in
                tareas_actuales], return_exceptions=True)
            reiniciar = {}
            for (sym, task), resultado in zip(tareas_actuales, resultados):
                if isinstance(resultado, asyncio.CancelledError):
                    continue
                if isinstance(resultado, Exception):
                    log.error(
                        f'⚠️ Stream {sym} finalizó con excepción: {resultado}. Reiniciando en 5s'
                        )
                    await asyncio.sleep(5)
                    reiniciar[sym] = asyncio.create_task(self.stream(sym,
                        handler))
                else:
                    self._tasks[sym] = task
            if reiniciar:
                self._tasks.update(reiniciar)
            else:
                break

    async def detener(self) ->None:
        """Cancela todos los streams en ejecución."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
