from __future__ import annotations
from core.orders.order_manager import OrderManager
from core.event_bus import EventBus


class PositionManager:
    """Gestión de posiciones abiertas usando :class:`OrderManager`."""

    def __init__(self, modo_real: bool, bus: EventBus | None = None) -> None:
        self._manager = OrderManager(modo_real, bus)

    @property
    def ordenes(self):
        return self._manager.ordenes

    @ordenes.setter
    def ordenes(self, value):
        self._manager.ordenes = value

    def obtener(self, symbol):
        return self._manager.obtener(symbol)

    async def abrir_async(self, *args, **kwargs):
        return await self._manager.abrir_async(*args, **kwargs)

    async def cerrar_async(self, *args, **kwargs):
        return await self._manager.cerrar_async(*args, **kwargs)

    async def cerrar_parcial_async(self, *args, **kwargs):
        return await self._manager.cerrar_parcial_async(*args, **kwargs)

