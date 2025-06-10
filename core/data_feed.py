"""Módulo para gestionar el flujo de datos desde Binance."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Dict, Iterable
from binance_api.websocket import escuchar_velas
from core.logger import configurar_logger


log = configurar_logger("datafeed", modo_silencioso=True)


class DataFeed:
    """Maneja la recepción de velas de Binance en tiempo real."""

    def __init__(self, intervalo: str) -> None:
        self.intervalo = intervalo
        self._tasks: Dict[str, asyncio.Task] = {}

    async def stream(self, symbol: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        """Escucha las velas de ``symbol`` y reintenta ante fallos de conexión."""
        while True:
            try:
                await escuchar_velas(symbol, self.intervalo, handler)
                break
            except Exception as e:  # pragma: no cover - conexión externa
                log.warning(f"⚠️ Stream {symbol} falló: {e}. Reintentando en 5s")
                await asyncio.sleep(5)

    async def escuchar(self, symbols: Iterable[str], handler: Callable[[dict], Awaitable[None]]) -> None:
        """Inicia un stream por cada símbolo y espera a que todos finalicen."""
        for sym in symbols:
            self._tasks[sym] = asyncio.create_task(self.stream(sym, handler))
        await asyncio.gather(*self._tasks.values())

    async def detener(self) -> None:
        """Cancela todos los streams en ejecución."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)