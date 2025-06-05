import asyncio
from typing import Callable, Awaitable
from binance_api.websocket import escuchar_velas
from core.logger import configurar_logger


log = configurar_logger("datafeed", modo_silencioso=True)


class DataFeed:
    """Maneja el stream de velas de Binance."""

    def __init__(self, intervalo: str) -> None:
        self.intervalo = intervalo

    async def stream(self, symbol: str, handler: Callable[[dict], Awaitable[None]]):
        """Escucha las velas de ``symbol`` y envía cada cierre al ``handler``."""
        await escuchar_velas(symbol, self.intervalo, handler)