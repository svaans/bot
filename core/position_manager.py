from __future__ import annotations
from typing import Any

from core.orders.order_manager import OrderManager
from core.orders.storage_simulado import sincronizar_ordenes_simuladas
from core.event_bus import EventBus


class PositionManager:
    """Gestión de posiciones abiertas usando :class:`OrderManager`."""

    def __init__(self, modo_real: bool, bus: EventBus | None = None) -> None:
        self._manager = OrderManager(modo_real, bus)
        self.capital_manager: Any | None = None
        self.risk_manager: Any | None = None
        if not modo_real:
            sincronizar_ordenes_simuladas(self._manager)

    @property
    def ordenes(self):
        return self._manager.ordenes

    @ordenes.setter
    def ordenes(self, value):
        self._manager.ordenes = value

    def obtener(self, symbol):
        return self._manager.obtener(symbol)

    def tiene_posicion(self, symbol) -> bool:
        """Indica si existe una posición abierta para ``symbol``."""
        return self.obtener(symbol) is not None
    
    async def abrir_async(self, *args, **kwargs):
        return await self._manager.abrir_async(*args, **kwargs)

    async def cerrar_async(self, *args, **kwargs):
        return await self._manager.cerrar_async(*args, **kwargs)

    async def cerrar_parcial_async(self, *args, **kwargs):
        return await self._manager.cerrar_parcial_async(*args, **kwargs)
    
    def configurar_capital_manager(self, capital_manager: Any | None) -> None:
        """Inyecta ``capital_manager`` en el :class:`OrderManager` interno.

        Tras la asignación se recalcula el capital disponible de todas las
        órdenes vivas para que el gestor reciba el nuevo comprometido.
        """

        self.capital_manager = capital_manager
        self._manager.capital_manager = capital_manager
        risk = getattr(self._manager, "risk_manager", None)
        if risk is not None and capital_manager is not None:
            try:
                setattr(risk, "capital_manager", capital_manager)
            except Exception:
                pass
        self._sincronizar_capital_activo()

    def configurar_risk_manager(self, risk_manager: Any | None) -> None:
        """Inyecta ``risk_manager`` en el :class:`OrderManager` interno.

        Garantiza que el :class:`RiskManager` reciba el capital comprometido de
        todas las órdenes abiertas y comparta el mismo ``capital_manager``.
        """

        self.risk_manager = risk_manager
        self._manager.risk_manager = risk_manager
        capital_manager = getattr(self._manager, "capital_manager", None)
        if risk_manager is not None and capital_manager is not None:
            try:
                setattr(risk_manager, "capital_manager", capital_manager)
            except Exception:
                pass
        self._sincronizar_capital_activo()

    def _sincronizar_capital_activo(self) -> None:
        if not self._manager.ordenes:
            return
        for symbol, orden in list(self._manager.ordenes.items()):
            self._manager._actualizar_capital_disponible(symbol, orden)

