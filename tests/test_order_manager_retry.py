from __future__ import annotations

import math
import sys
import types

import pytest

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