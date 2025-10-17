from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from core.event_bus import EventBus
from core.orders.order_manager import OrderManager
from core.orders.order_model import Order
from core.risk.risk_manager import RiskManager


UTC = timezone.utc


@pytest.mark.asyncio
async def test_mark_to_market_emits_pnl_event() -> None:
    bus = EventBus()
    events: list[dict[str, float]] = []

    async def _listener(payload):
        events.append(payload)

    bus.subscribe('orders.pnl_update', _listener)
    manager = OrderManager(modo_real=False, bus=bus)
    order = Order(
        symbol='BTCUSDT',
        precio_entrada=100.0,
        cantidad=1.0,
        stop_loss=95.0,
        take_profit=120.0,
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas={},
        tendencia='alcista',
        max_price=100.0,
        direccion='long',
        cantidad_abierta=1.0,
    )
    manager.ordenes['BTCUSDT'] = order

    manager.actualizar_mark_to_market('BTCUSDT', 110.0)
    await asyncio.sleep(0.01)

    assert events, 'No se emitió evento de PnL'
    payload = events[-1]
    assert payload['symbol'] == 'BTCUSDT'
    assert payload['pnl_latente'] == pytest.approx(110.0)
    assert payload['pnl_total'] == pytest.approx(payload['pnl_realizado'] + payload['pnl_latente'])
    assert payload['precio_mark'] == pytest.approx(110.0)


@pytest.mark.asyncio
async def test_risk_manager_triggers_latent_stop_event() -> None:
    bus = EventBus()
    triggered: list[dict[str, float]] = []

    async def _on_trigger(payload):
        triggered.append(payload)

    bus.subscribe('risk.latent_stop_triggered', _on_trigger)
    manager = RiskManager(umbral=0.1, bus=bus)

    pnl_payload = {
        'symbol': 'ETHUSDT',
        'pnl_realizado': 0.0,
        'pnl_latente': -45.0,
        'pnl_total': -45.0,
        'stop_loss': 1500.0,
        'precio_mark': 1499.0,
        'direccion': 'long',
        'timestamp': datetime.now(UTC).isoformat(),
    }
    bus.emit('orders.pnl_update', pnl_payload)
    await asyncio.sleep(0.01)

    assert triggered, 'RiskManager no emitió alerta de stop dinámico'
    event = triggered[-1]
    assert event['symbol'] == 'ETHUSDT'
    assert event['pnl_latente'] == pytest.approx(-45.0)
    assert event['precio_mark'] == pytest.approx(1499.0)
    assert event['stop_loss'] == pytest.approx(1500.0)
