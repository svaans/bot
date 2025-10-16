from __future__ import annotations

import pytest

from core.trader_modular import TraderLite


from .factories import DummyConfig, DummySupervisor


@pytest.mark.asyncio
async def test_candle_filter_blocks_duplicates_and_ticks_supervisor() -> None:
    supervisor = DummySupervisor()
    config = DummyConfig()
    processed: list[dict] = []

    async def handler(candle: dict) -> None:
        processed.append(candle)

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    candle = {"symbol": "BTCUSDT", "timestamp": 1_000, "close": 1.0}
    assert trader._update_estado_con_candle(candle) is True
    await trader._handler_invoker(candle)

    duplicate = {"symbol": "BTCUSDT", "timestamp": 1_000, "close": 1.1}
    assert trader._update_estado_con_candle(duplicate) is False

    assert processed == [candle]
    assert supervisor.ticks == [("BTCUSDT", False)]


@pytest.mark.asyncio
async def test_data_feed_uses_config_values_over_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    supervisor = DummySupervisor()
    config = DummyConfig(
        handler_timeout=3.5,
        inactivity_intervals=12,
        df_queue_default_limit=123,
        df_queue_min_recommended=64,
        df_queue_policy="block",
        monitor_interval=2.5,
        df_backpressure=False,
    )

    # Valores de entorno que deben ser ignorados por la config explícita
    monkeypatch.setenv("DF_HANDLER_TIMEOUT_SEC", "7.0")
    monkeypatch.setenv("DF_INACTIVITY_INTERVALS", "99")
    monkeypatch.setenv("DF_QUEUE_MAX", "999")
    monkeypatch.setenv("DF_QUEUE_POLICY", "drop_oldest")
    monkeypatch.setenv("DF_MONITOR_INTERVAL", "9.0")
    monkeypatch.setenv("DF_BACKPRESSURE", "true")
    monkeypatch.setenv("DF_QUEUE_MIN_RECOMMENDED", "8")

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    assert trader.feed.handler_timeout == pytest.approx(3.5)
    assert trader.feed.inactivity_intervals == 12
    assert trader.feed.queue_max == 123
    assert trader.feed.queue_min_recommended == 64
    assert trader.feed.queue_policy == "block"
    assert trader.feed.monitor_interval == pytest.approx(2.5)
    assert trader.feed.backpressure is False
