"""OrderManager.crear devuelve estado; _abrir_orden debe respetarlo."""

from __future__ import annotations

import pytest

from core.orders.order_open_status import OrderOpenStatus
from core.procesar_vela import _abrir_orden


@pytest.mark.asyncio
async def test_abrir_orden_returns_failed_when_crear_returns_failed() -> None:
    class OM:
        async def crear(self, **_kwargs):
            return OrderOpenStatus.FAILED

    status = await _abrir_orden(
        OM(),
        "BTC/EUR",
        "short",
        50000.0,
        49000.0,
        52000.0,
        {},
    )
    assert status is OrderOpenStatus.FAILED


@pytest.mark.asyncio
async def test_abrir_orden_returns_opened_when_crear_returns_opened() -> None:
    class OM:
        async def crear(self, **_kwargs):
            return OrderOpenStatus.OPENED

    status = await _abrir_orden(
        OM(),
        "BTC/EUR",
        "short",
        50000.0,
        49000.0,
        52000.0,
        {},
    )
    assert status is OrderOpenStatus.OPENED


@pytest.mark.asyncio
async def test_abrir_orden_returns_pending_registration() -> None:
    class OM:
        async def crear(self, **_kwargs):
            return OrderOpenStatus.PENDING_REGISTRATION

    status = await _abrir_orden(
        OM(),
        "ETH/EUR",
        "long",
        3000.0,
        2900.0,
        3200.0,
        {},
    )
    assert status is OrderOpenStatus.PENDING_REGISTRATION
