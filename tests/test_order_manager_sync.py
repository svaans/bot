"""Tests for order_manager_sync — reconciliación periódica con el exchange."""
from __future__ import annotations

from typing import Any

import pytest

from core.orders.order_manager import OrderManager
from core.orders.order_model import Order
from datetime import datetime, timezone

UTC = timezone.utc


class _DummyBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    def subscribe(self, *_args: Any, **_kwargs: Any) -> None:  # pragma: no cover
        pass

    async def publish(self, event: str, payload: dict[str, Any]) -> None:
        self.published.append((event, dict(payload)))

    def emit(self, *_args: Any, **_kwargs: Any) -> None:  # pragma: no cover
        pass


def _make_order(symbol: str = "BTCUSDT") -> Order:
    return Order(
        symbol=symbol,
        precio_entrada=50_000.0,
        cantidad=0.01,
        stop_loss=47_000.0,
        take_profit=55_000.0,
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas={},
        tendencia="alcista",
        max_price=50_000.0,
        direccion="long",
        cantidad_abierta=0.01,
    )


# ---------------------------------------------------------------------------
# SYNC-CAPITAL-LEAK-01: capital liberado cuando local_only es removida
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_local_only_releases_capital(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SYNC-CAPITAL-LEAK-01: una orden cerrada externamente libera capital.

    Antes del fix, cuando una orden existía localmente pero no en el exchange
    (local_only), la orden se descartaba de manager.ordenes vía merged pero
    _actualizar_capital_disponible nunca se llamaba → capital permanentemente bloqueado.
    """
    bus = _DummyBus()
    manager = OrderManager(modo_real=True, bus=bus)

    # Orden local activa
    orden = _make_order("BTCUSDT")
    manager.ordenes["BTCUSDT"] = orden

    capital_updates: list[tuple[str, Any]] = []

    def fake_actualizar_capital(symbol: str, ord_arg: Any | None = None) -> None:
        capital_updates.append((symbol, ord_arg))

    monkeypatch.setattr(
        manager, "_actualizar_capital_disponible", fake_actualizar_capital
    )

    # Exchange no tiene BTCUSDT → local_only
    # NOTA: reconciliar_ordenes se llama vía asyncio.to_thread → debe ser función síncrona
    def fake_reconciliar() -> dict[str, Order]:
        return {}  # exchange devuelve vacío

    monkeypatch.setattr(
        "core.orders.order_manager_sync.real_orders.reconciliar_ordenes",
        fake_reconciliar,
    )
    monkeypatch.setattr(
        "core.orders.order_manager_sync.registrar_orders_sync_success", lambda: None
    )
    monkeypatch.setattr(
        "core.orders.order_manager_sync.registrar_orders_sync_failure", lambda *_: None
    )
    monkeypatch.setattr(
        "core.orders.order_manager_sync.limpiar_registro_pendiente", lambda *_: None
    )

    from core.orders.order_manager_sync import run_sync_once

    result = await run_sync_once(manager)

    assert result is True
    # [SYNC-CAPITAL-LEAK-01] _actualizar_capital_disponible debe llamarse para BTCUSDT
    symbols_updated = [sym for sym, _ in capital_updates]
    assert "BTCUSDT" in symbols_updated, (
        "Capital no fue liberado para la orden cerrada externamente. "
        f"Capital updates: {capital_updates}"
    )
    # La orden debe haber sido removida de manager.ordenes
    assert "BTCUSDT" not in manager.ordenes


@pytest.mark.asyncio
async def test_sync_local_only_capital_called_after_removal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SYNC-CAPITAL-LEAK-01: _actualizar_capital_disponible se llama DESPUÉS de
    manager.ordenes = merged, garantizando vigente=None → comprometido=0 → capital liberado.
    """
    bus = _DummyBus()
    manager = OrderManager(modo_real=True, bus=bus)

    orden = _make_order("ETHUSDT")
    manager.ordenes["ETHUSDT"] = orden

    # Captura el estado de manager.ordenes en el momento de la llamada
    ordenes_en_momento_update: list[dict] = []

    def fake_actualizar_capital(symbol: str, ord_arg: Any | None = None) -> None:
        ordenes_en_momento_update.append(dict(manager.ordenes))

    monkeypatch.setattr(
        manager, "_actualizar_capital_disponible", fake_actualizar_capital
    )

    def fake_reconciliar() -> dict[str, Order]:
        return {}

    monkeypatch.setattr(
        "core.orders.order_manager_sync.real_orders.reconciliar_ordenes",
        fake_reconciliar,
    )
    monkeypatch.setattr(
        "core.orders.order_manager_sync.registrar_orders_sync_success", lambda: None
    )
    monkeypatch.setattr(
        "core.orders.order_manager_sync.registrar_orders_sync_failure", lambda *_: None
    )
    monkeypatch.setattr(
        "core.orders.order_manager_sync.limpiar_registro_pendiente", lambda *_: None
    )

    from core.orders.order_manager_sync import run_sync_once

    await run_sync_once(manager)

    assert ordenes_en_momento_update, "fake_actualizar_capital nunca fue llamado"
    snapshot = ordenes_en_momento_update[0]
    # En el momento en que se llama, ETHUSDT ya debe estar fuera de manager.ordenes
    # → _capital_comprometido devuelve 0 → capital liberado completamente
    assert "ETHUSDT" not in snapshot, (
        "La orden todavía estaba en manager.ordenes cuando se llamó "
        "_actualizar_capital_disponible — comprometido no sería 0"
    )


# ---------------------------------------------------------------------------
# SYNC-RACE-NEW-ORDER-01: órdenes abiertas durante sync no se pierden
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_preserves_order_opened_during_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SYNC-RACE-NEW-ORDER-01: orden abierta mientras reconciliar_ordenes bloquea no se pierde.

    Antes del fix, manager.ordenes = merged sobreescribía cualquier orden añadida
    durante el await asyncio.to_thread(reconciliar_ordenes). La posición quedaba
    en el exchange pero no en memoria hasta el siguiente ciclo de sync.
    """
    bus = _DummyBus()
    manager = OrderManager(modo_real=True, bus=bus)

    # BTCUSDT existe al inicio del sync
    btc_orden = _make_order("BTCUSDT")
    manager.ordenes["BTCUSDT"] = btc_orden

    eth_orden = _make_order("ETHUSDT")

    def fake_reconciliar() -> dict[str, Order]:
        # Simula que durante la reconciliación se abre ETHUSDT
        manager.ordenes["ETHUSDT"] = eth_orden
        return {"BTCUSDT": _make_order("BTCUSDT")}  # solo BTCUSDT en exchange

    monkeypatch.setattr(
        "core.orders.order_manager_sync.real_orders.reconciliar_ordenes",
        fake_reconciliar,
    )
    monkeypatch.setattr(manager, "_actualizar_capital_disponible", lambda *_: None)
    monkeypatch.setattr(
        "core.orders.order_manager_sync.registrar_orders_sync_success", lambda: None
    )
    monkeypatch.setattr(
        "core.orders.order_manager_sync.registrar_orders_sync_failure", lambda *_: None
    )
    monkeypatch.setattr(
        "core.orders.order_manager_sync.limpiar_registro_pendiente", lambda *_: None
    )

    from core.orders.order_manager_sync import run_sync_once

    result = await run_sync_once(manager)

    assert result is True
    # BTCUSDT debe seguir en memoria (estaba en exchange)
    assert "BTCUSDT" in manager.ordenes
    # [SYNC-RACE-NEW-ORDER-01] ETHUSDT debe preservarse — fue abierta durante el sync
    assert "ETHUSDT" in manager.ordenes, (
        "La orden abierta durante la ventana de sync fue eliminada. "
        "SYNC-RACE-NEW-ORDER-01 fix no aplicado."
    )


@pytest.mark.asyncio
async def test_sync_does_not_preserve_local_only_as_new(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SYNC-RACE-NEW-ORDER-01: local_only legítimas NO son tratadas como nuevas.

    local_only = presentes al inicio pero ausentes en exchange → deben descartarse.
    El fix solo preserva órdenes que NO estaban en local_only.
    """
    bus = _DummyBus()
    manager = OrderManager(modo_real=True, bus=bus)

    btc_orden = _make_order("BTCUSDT")
    manager.ordenes["BTCUSDT"] = btc_orden

    def fake_reconciliar() -> dict[str, Order]:
        return {}  # BTCUSDT ausente en exchange → local_only

    monkeypatch.setattr(
        "core.orders.order_manager_sync.real_orders.reconciliar_ordenes",
        fake_reconciliar,
    )
    monkeypatch.setattr(manager, "_actualizar_capital_disponible", lambda *_: None)
    monkeypatch.setattr(
        "core.orders.order_manager_sync.registrar_orders_sync_success", lambda: None
    )
    monkeypatch.setattr(
        "core.orders.order_manager_sync.registrar_orders_sync_failure", lambda *_: None
    )
    monkeypatch.setattr(
        "core.orders.order_manager_sync.limpiar_registro_pendiente", lambda *_: None
    )

    from core.orders.order_manager_sync import run_sync_once

    await run_sync_once(manager)

    # BTCUSDT era local_only → debe descartarse (cerrada en exchange)
    assert "BTCUSDT" not in manager.ordenes, (
        "La orden local_only fue incorrectamente preservada como si fuera nueva."
    )
