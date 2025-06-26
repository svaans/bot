from __future__ import annotations

import asyncio
from typing import Dict, Optional, List

from .order_manager import OrderManager
from .order_model import Order
from . import real_orders


class OrderService:
    """Interface para la gestión de órdenes."""

    def __init__(self, modo_real: bool, risk=None, notificador=None) -> None:
        self.modo_real = modo_real
        self._manager = OrderManager(modo_real, risk, notificador)

    # ------------------------------------------------------------------
    # Propiedades
    # ------------------------------------------------------------------
    @property
    def ordenes(self) -> Dict[str, Order]:
        return self._manager.ordenes

    @ordenes.setter
    def ordenes(self, value: Dict[str, Order]) -> None:
        self._manager.ordenes = value

    def obtener(self, symbol: str) -> Optional[Order]:
        return self._manager.obtener(symbol)

    # ------------------------------------------------------------------
    # Operaciones
    # ------------------------------------------------------------------
    async def abrir_async(self, *args, **kwargs) -> None:
        await self._manager.abrir_async(*args, **kwargs)

    def abrir(self, *args, **kwargs) -> None:
        self._manager.abrir(*args, **kwargs)

    async def agregar_parcial_async(self, *args, **kwargs) -> bool:
        return await self._manager.agregar_parcial_async(*args, **kwargs)

    def agregar_parcial(self, *args, **kwargs) -> bool:
        return self._manager.agregar_parcial(*args, **kwargs)

    async def cerrar_async(self, *args, **kwargs) -> bool:
        return await self._manager.cerrar_async(*args, **kwargs)

    def cerrar(self, *args, **kwargs) -> bool:
        return self._manager.cerrar(*args, **kwargs)

    async def cerrar_parcial_async(self, *args, **kwargs) -> bool:
        return await self._manager.cerrar_parcial_async(*args, **kwargs)

    def cerrar_parcial(self, *args, **kwargs) -> bool:
        return self._manager.cerrar_parcial(*args, **kwargs)

    # ------------------------------------------------------------------
    # Carga y persistencia
    # ------------------------------------------------------------------
    def cargar_ordenes(self, symbols: Optional[List[str]] = None) -> Dict[str, Order]:
        return {}

    async def flush_periodico(self, interval: int = 300) -> None:
        while True:
            await asyncio.sleep(interval)

class OrderServiceSimulado(OrderService):
    def __init__(self, risk=None, notificador=None) -> None:
        super().__init__(False, risk, notificador)


class OrderServiceReal(OrderService):
    def __init__(self, risk=None, notificador=None) -> None:
        super().__init__(True, risk, notificador)

    def cargar_ordenes(self, symbols: Optional[List[str]] = None) -> Dict[str, Order]:
        ordenes = real_orders.obtener_todas_las_ordenes()
        if not ordenes and symbols:
            ordenes = real_orders.sincronizar_ordenes_binance(symbols)
        return ordenes

    async def flush_periodico(self, interval: int = real_orders._FLUSH_INTERVAL) -> None:
        await real_orders.flush_periodico(interval)