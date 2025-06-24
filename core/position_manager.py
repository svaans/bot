from __future__ import annotations

from core.orders.order_manager import OrderManager


class PositionManager:
    """Gestión de posiciones abiertas usando :class:`OrderManager`."""

    def __init__(self, modo_real: bool, risk=None, notificador=None) -> None:
        self._manager = OrderManager(modo_real, risk, notificador)

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

    def abrir(self, *args, **kwargs):
        return self._manager.abrir(*args, **kwargs)

    async def cerrar_async(self, *args, **kwargs):
        return await self._manager.cerrar_async(*args, **kwargs)

    def cerrar(self, *args, **kwargs):
        return self._manager.cerrar(*args, **kwargs)

    async def cerrar_parcial_async(self, *args, **kwargs):
        return await self._manager.cerrar_parcial_async(*args, **kwargs)

    def cerrar_parcial(self, *args, **kwargs):
        return self._manager.cerrar_parcial(*args, **kwargs)