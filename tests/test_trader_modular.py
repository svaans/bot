from __future__ import annotations

import pytest

from core.trader_modular import TraderLite


class DummySupervisor:
    def __init__(self) -> None:
        self.ticks: list[tuple[str, bool]] = []

    def start_supervision(self) -> None:
        return None

    def supervised_task(self, *args, **kwargs) -> None:  # pragma: no cover - interfaz dummy
        return None

    def beat(self, *args, **kwargs) -> None:  # pragma: no cover - interfaz dummy
        return None

    def tick_data(self, symbol: str, reinicio: bool = False) -> None:
        self.ticks.append((symbol, reinicio))

    async def shutdown(self) -> None:  # pragma: no cover - interfaz dummy
        return None


class DummyConfig:
    def __init__(self, **overrides) -> None:
        self.symbols = ["BTCUSDT"]
        self.intervalo_velas = "1m"
        self.modo_real = False
        self.max_spread_ratio = 0.0
        self.spread_dynamic = False
        self.spread_guard_window = 50
        self.spread_guard_hysteresis = 0.15
        self.spread_guard_max_limit = 0.05
        self.handler_timeout = 2.0
        self.inactivity_intervals = 10
        self.df_queue_default_limit = 2000
        self.df_queue_policy = "drop_oldest"
        self.monitor_interval = 5.0
        self.df_backpressure = True
        for key, value in overrides.items():
            setattr(self, key, value)


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

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    assert trader.feed.handler_timeout == pytest.approx(3.5)
    assert trader.feed.inactivity_intervals == 12
    assert trader.feed.queue_max == 123
    assert trader.feed.queue_policy == "block"
    assert trader.feed.monitor_interval == pytest.approx(2.5)
    assert trader.feed.backpressure is False