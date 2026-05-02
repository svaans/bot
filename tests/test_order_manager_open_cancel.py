from __future__ import annotations

import asyncio

import pytest

from core.orders import real_orders
from core.orders import order_manager_abrir
from core.orders.order_manager import OrderManager
from core.orders.order_open_status import OrderOpenStatus


def _abrir_kwargs(symbol: str = "BTC/USDT") -> dict:
    return {
        "symbol": symbol,
        "precio": 20_000.0,
        "sl": 19_000.0,
        "tp": 21_000.0,
        "estrategias": {},
        "tendencia": "alcista",
        "direccion": "long",
        "cantidad": 0.01,
        "puntaje": 0.0,
        "umbral": 0.0,
        "score_tecnico": 0.0,
    }


class _HangingBus:
    """Bus mínimo compatible con OrderManager.subscribe (no usa EventBus completo)."""

    def subscribe(self, *_args: object, **_kwargs: object) -> None:
        return None

    async def publish(self, *_args: object, **_kwargs: object) -> None:
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_cancel_mid_open_clears_abriendo_and_ghost_order() -> None:
    """Timeout/cancel durante el primer publish: no queda abriendo ni orden huérfana."""

    symbol = "BTC/USDT"
    manager = OrderManager(modo_real=False, bus=_HangingBus())

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(manager.abrir_async(**_abrir_kwargs(symbol)), timeout=0.15)

    assert symbol not in manager.abriendo
    assert symbol not in manager.ordenes


@pytest.mark.asyncio
async def test_cancel_mid_open_schedules_reconcile_when_modo_real(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """En modo real, cancelación durante ejecución remota programa un sync vía _sync_once."""

    monkeypatch.setattr(
        real_orders,
        "sincronizar_ordenes_binance",
        lambda *_a, **_k: {},
        raising=False,
    )
    monkeypatch.setattr(
        order_manager_abrir,
        "remainder_executable",
        lambda *_a, **_k: True,
    )

    async def fake_fetch_balance(_client: object) -> dict:
        return {"free": {"USDT": 1_000_000.0}}

    monkeypatch.setattr(order_manager_abrir, "obtener_cliente", lambda: object())
    monkeypatch.setattr(order_manager_abrir, "_fetch_balance_non_blocking", fake_fetch_balance)

    symbol = "BTC/USDT"
    manager = OrderManager(modo_real=True, bus=None)

    sync_done = asyncio.Event()

    async def hang_execute(*_a: object, **_k: object) -> None:
        await asyncio.Event().wait()

    async def track_sync() -> bool:
        sync_done.set()
        return True

    manager._execute_real_order = hang_execute  # type: ignore[method-assign]
    manager._sync_once = track_sync  # type: ignore[method-assign]

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(manager.abrir_async(**_abrir_kwargs(symbol)), timeout=0.15)

    assert symbol not in manager.abriendo
    assert symbol not in manager.ordenes

    await asyncio.wait_for(sync_done.wait(), timeout=2.0)


@pytest.mark.asyncio
async def test_successful_open_clears_abriendo() -> None:
    manager = OrderManager(modo_real=False, bus=None)
    symbol = "BTC/USDT"
    status = await manager.abrir_async(**_abrir_kwargs(symbol))
    assert status is OrderOpenStatus.OPENED
    assert symbol in manager.ordenes
    assert symbol not in manager.abriendo
