"""Tests for order_manager_cerrar — cierre total/parcial."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from core.orders.order_manager import OrderManager
from core.orders.order_model import Order
from core.orders.market_retry_executor import ExecutionResult

UTC = timezone.utc


class _DummyBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    def subscribe(self, event: str, callback: Any) -> None:  # pragma: no cover
        pass

    async def publish(self, event: str, payload: dict[str, Any]) -> None:
        self.published.append((event, dict(payload)))

    def emit(self, *_args: Any, **_kwargs: Any) -> None:  # pragma: no cover
        pass


def _make_order(
    *,
    symbol: str = "BTCUSDT",
    precio_entrada: float = 100.0,
    cantidad: float = 1.0,
    cantidad_abierta: float = 1.0,
    direccion: str = "long",
    pnl_realizado: float = 0.0,
    pnl_latente: float = 0.0,
) -> Order:
    orden = Order(
        symbol=symbol,
        precio_entrada=precio_entrada,
        cantidad=cantidad,
        stop_loss=95.0,
        take_profit=120.0,
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas={},
        tendencia="alcista",
        max_price=precio_entrada,
        direccion=direccion,
        cantidad_abierta=cantidad_abierta,
    )
    orden.pnl_realizado = pnl_realizado
    orden.pnl_latente = pnl_latente
    return orden


# ---------------------------------------------------------------------------
# CERRAR-PNL-DOUBLE-01: execution.pnl debe contarse exactamente una vez
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cerrar_parcial_pnl_applied_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CERRAR-PNL-DOUBLE-01: execution.pnl se aplica UNA sola vez a pnl_realizado.

    Antes del fix la línea:
        orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) + execution.pnl
    causaba doble conteo porque el getter de pnl_operaciones ya incluía el delta
    que _apply_realized_pnl_delta acababa de añadir, más el pnl_latente vigente.
    Resultado: pnl_realizado = inicial + 2*Δ + pnl_latente en lugar de inicial + Δ.
    """
    bus = _DummyBus()
    manager = OrderManager(modo_real=True, bus=bus)
    manager._partial_close_retry_delay = 0.0  # type: ignore[attr-defined]

    # pnl_latente != 0 para que el bug anterior absorbiera el latente en realizado
    orden = _make_order(
        cantidad=1.0,
        cantidad_abierta=1.0,
        pnl_realizado=0.0,
        pnl_latente=5.0,  # simula MTM previo
    )
    manager.ordenes["BTCUSDT"] = orden

    execution_pnl = 10.0

    async def fake_execute(*_args: Any, **_kwargs: Any) -> ExecutionResult:
        return ExecutionResult(
            executed=0.5,
            fee=0.1,
            pnl=execution_pnl,
            status="FILLED",
            precio_fill_promedio=110.0,
        )

    mtm_calls: list[tuple[str, float]] = []

    def fake_mtm(symbol: str, precio: float) -> None:
        mtm_calls.append((symbol, precio))

    monkeypatch.setattr(manager, "_execute_real_order", fake_execute)
    monkeypatch.setattr(manager, "_generar_operation_id", lambda _: "op-test")
    monkeypatch.setattr(manager, "_actualizar_capital_disponible", lambda *_: None)
    monkeypatch.setattr(manager, "actualizar_mark_to_market", fake_mtm)
    monkeypatch.setattr(
        "core.orders.order_manager_cerrar.registrar_orden", lambda *_: None
    )
    monkeypatch.setattr(
        "core.orders.order_manager_cerrar.registrar_partial_close_collision",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "core.orders.real_orders.actualizar_orden", lambda *_: None
    )

    result = await manager.cerrar_parcial_async("BTCUSDT", 0.5, 110.0, "test")

    assert result is True

    # pnl_realizado debe ser exactamente inicial (0) + execution.pnl (10) = 10
    # Sin fix: 0 + 10 + 10 + 5 (latente absorbido) = 25
    assert orden.pnl_realizado == pytest.approx(10.0), (
        f"pnl_realizado={orden.pnl_realizado} — se esperaba {execution_pnl} "
        "(el doble conteo o absorción de latente indicaría que el fix falló)"
    )

    # actualizar_mark_to_market debe haberse llamado con el fill price
    assert mtm_calls, "actualizar_mark_to_market debe llamarse tras un cierre parcial exitoso"
    assert mtm_calls[-1] == ("BTCUSDT", pytest.approx(110.0))


@pytest.mark.asyncio
async def test_cerrar_parcial_pnl_acumulado_multiples_partials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CERRAR-PNL-DOUBLE-01: acumulación correcta a través de N cierres parciales.

    Dos cierres parciales con pnl=5.0 cada uno deben sumar pnl_realizado=10.0,
    no 40+ (doble conteo compuesto).
    """
    bus = _DummyBus()
    manager = OrderManager(modo_real=True, bus=bus)
    manager._partial_close_retry_delay = 0.0  # type: ignore[attr-defined]

    orden = _make_order(
        cantidad=1.0,
        cantidad_abierta=1.0,
        pnl_realizado=0.0,
        pnl_latente=3.0,
    )
    manager.ordenes["BTCUSDT"] = orden

    call_count = 0

    async def fake_execute(*_args: Any, **_kwargs: Any) -> ExecutionResult:
        nonlocal call_count
        call_count += 1
        return ExecutionResult(
            executed=0.3,
            fee=0.05,
            pnl=5.0,
            status="FILLED",
            precio_fill_promedio=105.0,
        )

    monkeypatch.setattr(manager, "_execute_real_order", fake_execute)
    monkeypatch.setattr(manager, "_generar_operation_id", lambda _: "op-test")
    monkeypatch.setattr(manager, "_actualizar_capital_disponible", lambda *_: None)
    monkeypatch.setattr(manager, "actualizar_mark_to_market", lambda *_: None)
    monkeypatch.setattr(
        "core.orders.order_manager_cerrar.registrar_orden", lambda *_: None
    )
    monkeypatch.setattr(
        "core.orders.order_manager_cerrar.registrar_partial_close_collision",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "core.orders.real_orders.actualizar_orden", lambda *_: None
    )

    # Primer cierre parcial
    await manager.cerrar_parcial_async("BTCUSDT", 0.3, 105.0, "parcial-1")
    assert orden.pnl_realizado == pytest.approx(5.0), (
        f"Tras primer parcial: esperado 5.0, obtenido {orden.pnl_realizado}"
    )

    # Reset latente para simular MTM intermedio
    orden.pnl_latente = 2.0

    # Segundo cierre parcial
    await manager.cerrar_parcial_async("BTCUSDT", 0.3, 105.0, "parcial-2")
    assert orden.pnl_realizado == pytest.approx(10.0), (
        f"Tras segundo parcial: esperado 10.0, obtenido {orden.pnl_realizado} "
        "(bug de doble conteo activo si el valor es mayor)"
    )

    assert call_count == 2
