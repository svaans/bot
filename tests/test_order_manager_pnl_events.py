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
    # [FIX MTM-WRONG-FORMULA] pnl_latente = (precio_actual - precio_entrada) × qty
    # entry=100, mark=110, qty=1, long → (110 - 100) × 1 = 10
    assert payload['pnl_latente'] == pytest.approx(10.0)
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


# ---------------------------------------------------------------------------
# MTM-WRONG-FORMULA: fórmula correcta por dirección
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_to_market_long_profit() -> None:
    """Long en ganancia: pnl_latente = (precio_actual - entrada) × qty."""
    bus = EventBus()
    events: list[dict] = []
    bus.subscribe('orders.pnl_update', lambda p: events.append(p))

    manager = OrderManager(modo_real=False, bus=bus)
    manager.ordenes['BTCUSDT'] = Order(
        symbol='BTCUSDT', precio_entrada=49_000.0, cantidad=0.01,
        stop_loss=47_000.0, take_profit=55_000.0,
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas={}, tendencia='alcista', max_price=49_000.0,
        direccion='long', cantidad_abierta=0.01,
    )

    manager.actualizar_mark_to_market('BTCUSDT', 50_000.0)
    await asyncio.sleep(0.01)

    payload = events[-1]
    # (50000 - 49000) * 0.01 = 10.0
    assert payload['pnl_latente'] == pytest.approx(10.0)
    assert payload['pnl_total'] == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_mark_to_market_long_loss() -> None:
    """Long en pérdida: pnl_latente negativo."""
    bus = EventBus()
    events: list[dict] = []
    bus.subscribe('orders.pnl_update', lambda p: events.append(p))

    manager = OrderManager(modo_real=False, bus=bus)
    manager.ordenes['BTCUSDT'] = Order(
        symbol='BTCUSDT', precio_entrada=50_000.0, cantidad=0.01,
        stop_loss=47_000.0, take_profit=55_000.0,
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas={}, tendencia='alcista', max_price=50_000.0,
        direccion='long', cantidad_abierta=0.01,
    )

    manager.actualizar_mark_to_market('BTCUSDT', 48_000.0)
    await asyncio.sleep(0.01)

    payload = events[-1]
    # (48000 - 50000) * 0.01 = -20.0
    assert payload['pnl_latente'] == pytest.approx(-20.0)


@pytest.mark.asyncio
async def test_mark_to_market_short_profit() -> None:
    """Short en ganancia: precio baja → pnl_latente positivo."""
    bus = EventBus()
    events: list[dict] = []
    bus.subscribe('orders.pnl_update', lambda p: events.append(p))

    manager = OrderManager(modo_real=False, bus=bus)
    manager.ordenes['ETHUSDT'] = Order(
        symbol='ETHUSDT', precio_entrada=3_000.0, cantidad=1.0,
        stop_loss=3_200.0, take_profit=2_500.0,
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas={}, tendencia='bajista', max_price=3_000.0,
        direccion='short', cantidad_abierta=1.0,
    )

    manager.actualizar_mark_to_market('ETHUSDT', 2_800.0)
    await asyncio.sleep(0.01)

    payload = events[-1]
    # -(2800 - 3000) * 1.0 = 200.0
    assert payload['pnl_latente'] == pytest.approx(200.0)


@pytest.mark.asyncio
async def test_mark_to_market_short_loss() -> None:
    """Short en pérdida: precio sube → pnl_latente negativo."""
    bus = EventBus()
    events: list[dict] = []
    bus.subscribe('orders.pnl_update', lambda p: events.append(p))

    manager = OrderManager(modo_real=False, bus=bus)
    manager.ordenes['ETHUSDT'] = Order(
        symbol='ETHUSDT', precio_entrada=3_000.0, cantidad=1.0,
        stop_loss=3_200.0, take_profit=2_500.0,
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas={}, tendencia='bajista', max_price=3_000.0,
        direccion='short', cantidad_abierta=1.0,
    )

    manager.actualizar_mark_to_market('ETHUSDT', 3_100.0)
    await asyncio.sleep(0.01)

    payload = events[-1]
    # -(3100 - 3000) * 1.0 = -100.0
    assert payload['pnl_latente'] == pytest.approx(-100.0)


@pytest.mark.asyncio
async def test_mark_to_market_invalid_entry_price_gives_zero() -> None:
    """Si precio_entrada es 0 o inválido, pnl_latente = 0 (no crash)."""
    bus = EventBus()
    events: list[dict] = []
    bus.subscribe('orders.pnl_update', lambda p: events.append(p))

    manager = OrderManager(modo_real=False, bus=bus)
    manager.ordenes['ADAUSDT'] = Order(
        symbol='ADAUSDT', precio_entrada=0.0, cantidad=100.0,
        stop_loss=0.0, take_profit=0.0,
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas={}, tendencia='', max_price=0.0,
        direccion='long', cantidad_abierta=100.0,
    )

    manager.actualizar_mark_to_market('ADAUSDT', 0.5)
    await asyncio.sleep(0.01)

    payload = events[-1]
    assert payload['pnl_latente'] == pytest.approx(0.0)
