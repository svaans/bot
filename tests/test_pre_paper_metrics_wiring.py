from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from binance_api.websocket import _record_ws_dispatch_queue_metrics
from core.trader.trader_lite_processing import TraderLiteProcessingMixin


def test_ws_dispatch_queue_metrics_counter_when_above_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    sets: list[float] = []
    incs: list[int] = []

    class _G:
        def set(self, v: float) -> None:
            sets.append(v)

    class _C:
        def inc(self) -> None:
            incs.append(1)

    monkeypatch.setattr(
        "core.metrics.WS_DISPATCH_QUEUE_DEPTH",
        _G(),
        raising=False,
    )
    monkeypatch.setattr(
        "core.metrics.WS_DISPATCH_BACKLOG_EVENTS_TOTAL",
        _C(),
        raising=False,
    )

    _record_ws_dispatch_queue_metrics(10, 64)
    assert sets == [10.0]
    assert incs == []

    _record_ws_dispatch_queue_metrics(48, 64)
    assert sets[-1] == 48.0
    assert incs == [1]


@pytest.mark.asyncio
async def test_procesar_vela_records_pipeline_queue_wait_from_enqueue_stamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_hist = MagicMock()
    mock_labeled = MagicMock()
    mock_hist.labels.return_value = mock_labeled
    monkeypatch.setattr(
        "core.trader.trader_lite_processing.TRADER_PIPELINE_QUEUE_WAIT",
        mock_hist,
    )

    class _T(TraderLiteProcessingMixin):
        async def _handler_invoker(self, candle: dict) -> None:
            return None

        def _after_procesar_vela(self, symbol: str) -> None:
            return None

    t = _T()
    t.config = SimpleNamespace(intervalo_velas="5m")
    t.feed = SimpleNamespace(handler_timeout=300.0)

    t0 = time.perf_counter() - 0.1
    candle = {
        "symbol": "BTC/USDT",
        "timestamp": 1,
        "timeframe": "5m",
        "_df_enqueue_time": t0,
    }
    await _T._procesar_vela(t, candle)

    mock_hist.labels.assert_called()
    kw = mock_hist.labels.call_args[1]
    assert kw["symbol"] == "BTC/USDT"
    assert kw["timeframe"] == "5m"
    mock_labeled.observe.assert_called_once()
    wait_s = mock_labeled.observe.call_args[0][0]
    assert wait_s >= 0.09


@pytest.mark.asyncio
async def test_heartbeat_updates_trader_queue_size_gauge(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.trader_modular import TraderLite

    from tests.factories import DummyConfig, DummySupervisor

    sets: list[tuple[str, str, float]] = []

    def fake_safe_set(gauge: object, value: float, **labels: str) -> None:
        sets.append((labels.get("symbol", ""), labels.get("timeframe", ""), value))

    monkeypatch.setattr("core.trader.trader_lite.safe_set", fake_safe_set)

    q: asyncio.Queue[object] = asyncio.Queue()
    await q.put(object())
    await q.put(object())

    feed = SimpleNamespace(
        _queues={"ETH/USDT": q},
        _consumer_state={"ETH/USDT": SimpleNamespace(name="HEALTHY")},
        ws_connected_event=SimpleNamespace(is_set=lambda: True),
        _ws_failure_reason=None,
    )

    supervisor = DummySupervisor()
    config = DummyConfig(intervalo_velas="15m")

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)
    trader.feed = feed  # type: ignore[assignment]
    trader.orders = None

    trader._log_heartbeat_state()

    assert ("ETH/USDT", "15m", 2.0) in sets
