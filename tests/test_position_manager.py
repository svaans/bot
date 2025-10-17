from __future__ import annotations

from core.orders.order_model import Order
from core.position_manager import PositionManager


class DummyCapitalManager:
    def __init__(self, asignado: float = 150.0) -> None:
        self._asignado = asignado
        self.actualizaciones: list[tuple[str, float]] = []

    def exposure_asignada(self, symbol: str) -> float:
        return self._asignado

    def actualizar_exposure(self, symbol: str, disponible: float) -> None:
        self.actualizaciones.append((symbol, disponible))


class DummyRiskManager:
    def __init__(self) -> None:
        self.capital_manager: DummyCapitalManager | None = None
        self.sincronizaciones: list[tuple[str, float]] = []

    def sincronizar_exposure(self, symbol: str, comprometido: float) -> None:
        self.sincronizaciones.append((symbol, comprometido))
        if self.capital_manager is not None:
            asignado = self.capital_manager.exposure_asignada(symbol)
            disponible = max(asignado - comprometido, 0.0)
            self.capital_manager.actualizar_exposure(symbol, disponible)


def _crear_orden(symbol: str = "BTCUSDT", comprometido: float = 20.0) -> Order:
    precio = 10.0
    cantidad = comprometido / precio
    orden = Order(
        symbol=symbol,
        precio_entrada=precio,
        cantidad=cantidad,
        stop_loss=precio * 0.95,
        take_profit=precio * 1.05,
        timestamp="2024-01-01T00:00:00Z",
        estrategias_activas={},
        tendencia="long",
        max_price=precio,
    )
    orden.cantidad_abierta = cantidad
    return orden


def test_configurar_capital_manager_actualiza_capital_disponible() -> None:
    manager = PositionManager(modo_real=False)
    orden = _crear_orden()
    manager.ordenes = {orden.symbol: orden}
    capital_manager = DummyCapitalManager(asignado=150.0)

    manager.configurar_capital_manager(capital_manager)

    assert manager.capital_manager is capital_manager
    assert manager._manager.capital_manager is capital_manager
    assert capital_manager.actualizaciones == [(orden.symbol, 130.0)]


def test_configurar_risk_manager_sincroniza_comprometido() -> None:
    manager = PositionManager(modo_real=False)
    orden = _crear_orden()
    manager.ordenes = {orden.symbol: orden}
    capital_manager = DummyCapitalManager(asignado=150.0)
    manager.configurar_capital_manager(capital_manager)
    risk_manager = DummyRiskManager()

    manager.configurar_risk_manager(risk_manager)

    assert manager.risk_manager is risk_manager
    assert manager._manager.risk_manager is risk_manager
    assert risk_manager.capital_manager is capital_manager
    assert risk_manager.sincronizaciones == [(orden.symbol, 20.0)]
    # El RiskManager debe propagar la actualización al capital manager
    assert capital_manager.actualizaciones[-1] == (orden.symbol, 130.0)


def test_configurar_capital_manager_con_risk_previo_sincroniza() -> None:
    manager = PositionManager(modo_real=False)
    orden = _crear_orden()
    manager.ordenes = {orden.symbol: orden}
    risk_manager = DummyRiskManager()
    manager.configurar_risk_manager(risk_manager)
    capital_manager = DummyCapitalManager(asignado=200.0)

    manager.configurar_capital_manager(capital_manager)

    assert risk_manager.capital_manager is capital_manager
    assert risk_manager.sincronizaciones == [(orden.symbol, 20.0)]
    assert capital_manager.actualizaciones[-1] == (orden.symbol, 180.0)