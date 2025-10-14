from __future__ import annotations

import asyncio
import math
import logging
import sqlite3
import sys
import types

import pytest

from core.utils.feature_flags import reset_flag_cache

if 'core.notification_manager' not in sys.modules:
    notification_module = types.ModuleType('core.notification_manager')

    class _DummyNotifier:
        def enviar(self, *_args: object, **_kwargs: object) -> None:
            pass

    def _crear_notification_manager_desde_env(*_args, **_kwargs) -> _DummyNotifier:
        return _DummyNotifier()

    notification_module.crear_notification_manager_desde_env = (  # type: ignore[attr-defined]
        _crear_notification_manager_desde_env
    )
    sys.modules['core.notification_manager'] = notification_module

if 'indicators' not in sys.modules:
    indicators_pkg = types.ModuleType('indicators')
    indicators_pkg.__all__ = ['ema', 'rsi']
    sys.modules['indicators'] = indicators_pkg
    sys.modules['indicators.ema'] = types.ModuleType('indicators.ema')
    sys.modules['indicators.rsi'] = types.ModuleType('indicators.rsi')

from core.orders.order_manager import OrderManager
from core.orders import order_manager as order_manager_module


@pytest.mark.parametrize(
    "attempt,expected",
    [
        (1, 1.0),
        (2, 1.5),
        (3, 2.25),
        (6, 7.59375),
    ],
)
def test_market_retry_sleep_growth(attempt: int, expected: float) -> None:
    manager = OrderManager(modo_real=False, bus=None)
    manager._market_retry_backoff = 1.0
    manager._market_retry_backoff_multiplier = 1.5
    manager._market_retry_backoff_cap = 10.0

    delay = manager._market_retry_sleep(attempt)

    assert math.isclose(delay, min(expected, manager._market_retry_backoff_cap))


def test_market_retry_sleep_caps_to_max() -> None:
    manager = OrderManager(modo_real=False, bus=None)
    manager._market_retry_backoff = 2.0
    manager._market_retry_backoff_multiplier = 3.0
    manager._market_retry_backoff_cap = 5.0

    assert manager._market_retry_sleep(1) == 2.0
    assert manager._market_retry_sleep(2) == 5.0
    assert manager._market_retry_sleep(3) == 5.0


def test_compute_next_sync_delay_backoff_and_reset() -> None:
    manager = OrderManager(modo_real=False, bus=None)
    manager._sync_base_interval = 10.0
    manager._sync_interval = 10.0
    manager._sync_min_interval = 5.0
    manager._sync_max_interval = 60.0
    manager._sync_backoff_factor = 2.0
    manager._sync_jitter = 0.0

    delay_first_failure = manager._compute_next_sync_delay(success=False)
    assert delay_first_failure == pytest.approx(20.0)
    assert manager._sync_failures == 1

    delay_second_failure = manager._compute_next_sync_delay(success=False)
    assert delay_second_failure == pytest.approx(40.0)
    assert manager._sync_failures == 2

    delay_success = manager._compute_next_sync_delay(success=True)
    assert delay_success == pytest.approx(10.0)
    assert manager._sync_failures == 0


@pytest.mark.asyncio
async def test_schedule_registro_retry_on_persistence_failure(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setenv("ORDERS_RETRY_PERSISTENCIA_ENABLED", "true")
    monkeypatch.setenv("ORDERS_RETRY_PERSISTENCIA_BASE_DELAY", "0.01")
    monkeypatch.setenv("ORDERS_RETRY_PERSISTENCIA_BACKOFF", "2.0")
    monkeypatch.setenv("ORDERS_RETRY_PERSISTENCIA_MAX_DELAY", "0.05")
    reset_flag_cache()

    manager = OrderManager(modo_real=True, bus=None)
    symbol = "BTCUSDT"
    manager._registro_retry_base_delay = 0.02  # type: ignore[attr-defined]
    manager._registro_retry_max_delay = 0.05  # type: ignore[attr-defined]

    metric_calls: list[tuple[str, str]] = []

    def fake_registrar_metric(sym: str, reason: str) -> None:
        metric_calls.append((sym, reason))

    monkeypatch.setattr(
        "core.orders.order_manager.registrar_orders_retry_scheduled",
        fake_registrar_metric,
    )

    call_counter = {"count": 0}

    def fake_registrar_orden(*_args: object, **_kwargs: object) -> None:
        call_counter["count"] += 1
        if call_counter["count"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return None

    async def fake_to_thread(func, *args, **kwargs):  # type: ignore[no-redef]
        return func(*args, **kwargs)

    monkeypatch.setattr(
        "core.orders.order_manager.real_orders.registrar_orden",
        fake_registrar_orden,
    )
    monkeypatch.setattr("core.orders.order_manager.asyncio.to_thread", fake_to_thread)

    order = types.SimpleNamespace(
        precio_entrada=1.0,
        cantidad=1.0,
        cantidad_abierta=1.0,
        stop_loss=0.9,
        take_profit=1.1,
        estrategias_activas={},
        tendencia="alcista",
        direccion="long",
        operation_id="op-1",
        registro_pendiente=True,
    )

    manager.ordenes[symbol] = order
    caplog.set_level("INFO", logger="orders")

    captured_records: list[logging.LogRecord] = []

    class _CaptureHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - trivial
            captured_records.append(record)

    handler = _CaptureHandler()
    order_manager_module.log.addHandler(handler)
    try:
        manager._schedule_registro_retry(symbol, reason="InitialError")

        await asyncio.sleep(0.2)
    finally:
        order_manager_module.log.removeHandler(handler)

    assert call_counter["count"] == 2
    assert metric_calls == [(symbol, "InitialError"), (symbol, "OperationalError")]
    records = [rec for rec in captured_records if rec.msg == "orders.retry_schedule"]
    reasons = {
        getattr(rec, "retry_reason", None)
        or getattr(rec, "reason", None)
        or getattr(rec, "reason_", None)
        for rec in records
    }
    assert {"InitialError", "OperationalError"}.issubset(reasons)
    assert order.registro_pendiente is False
    assert manager._registro_retry_attempts.get(symbol) is None