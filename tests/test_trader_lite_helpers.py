from __future__ import annotations

import asyncio
from typing import Any
from types import SimpleNamespace

import pytest

from core.trader_modular import TraderLite
from core.trader.trader_lite_processing import TraderLiteProcessingMixin

from .factories import DummyConfig, DummySupervisor


@pytest.mark.asyncio
async def test_data_feed_uses_environment_when_config_missing(
    monkeypatch: pytest.MonkeyPatch, stub_data_feed: list[Any]
) -> None:
    supervisor = DummySupervisor()
    config = DummyConfig(
        handler_timeout=None,
        inactivity_intervals=None,
        df_queue_default_limit=None,
        df_queue_min_recommended=None,
        df_queue_policy=None,
        monitor_interval=None,
        df_backpressure=None,
    )

    monkeypatch.setenv("DF_HANDLER_TIMEOUT_SEC", "6.5")
    monkeypatch.setenv("DF_INACTIVITY_INTERVALS", "7")
    monkeypatch.setenv("DF_QUEUE_MAX", "321")
    monkeypatch.setenv("DF_QUEUE_POLICY", "BLOCK")
    monkeypatch.setenv("DF_MONITOR_INTERVAL", "1.25")
    monkeypatch.setenv("DF_BACKPRESSURE", "false")
    monkeypatch.setenv("DF_CANCEL_TIMEOUT", "8.5")
    monkeypatch.setenv("DF_QUEUE_MIN_RECOMMENDED", "19")

    async def handler(_: dict) -> None:
        return None

    TraderLite(config, candle_handler=handler, supervisor=supervisor)

    feed = stub_data_feed[0]
    assert feed.handler_timeout == pytest.approx(6.5)
    assert feed.inactivity_intervals == 7
    assert feed.queue_max == 321
    assert feed.queue_min_recommended == 19
    assert feed.queue_policy == "block"
    assert feed.monitor_interval == pytest.approx(1.25)
    assert feed.backpressure is False
    assert feed.cancel_timeout == pytest.approx(8.5)


@pytest.mark.asyncio
async def test_backpressure_from_environment_when_config_none(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = DummySupervisor()
    config = DummyConfig(df_backpressure=None)

    monkeypatch.setenv("DF_BACKPRESSURE", "false")

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    assert trader.feed.backpressure is False


@pytest.mark.asyncio
async def test_update_estado_ignores_unknown_symbol() -> None:
    supervisor = DummySupervisor()
    config = DummyConfig()

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    other_symbol = {"symbol": "ETHUSDT", "timestamp": 1_000, "close": 1.0}
    assert trader._update_estado_con_candle(other_symbol) is True
    assert supervisor.ticks == []


@pytest.mark.asyncio
async def test_update_estado_respects_buffer_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    supervisor = DummySupervisor()
    monkeypatch.setenv("MAX_BUFFER_VELAS", "2")
    config = DummyConfig()

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    candles = [
        {"symbol": "BTCUSDT", "timestamp": 1_000, "close": 1.0},
        {"symbol": "BTCUSDT", "timestamp": 2_000, "close": 1.1},
        {"symbol": "BTCUSDT", "timestamp": 3_000, "close": 1.2},
    ]

    for candle in candles:
        assert trader._update_estado_con_candle(candle) is True

    buffer = list(trader.estado["BTCUSDT"].buffer)
    assert buffer == candles[-2:]
    assert supervisor.ticks == [("BTCUSDT", False)] * 3


@pytest.mark.asyncio
async def test_handler_invoker_infers_trader_argument() -> None:
    supervisor = DummySupervisor()
    config = DummyConfig()
    calls: list[tuple[Any, dict]] = []

    async def handler(bot, datos) -> None:  # type: ignore[no-untyped-def]
        calls.append((bot, datos))

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    candle = {"symbol": "BTCUSDT", "timestamp": 1_000}
    await trader._handler_invoker(candle)

    assert calls and calls[0][0] is trader
    assert calls[0][1] is candle


@pytest.mark.asyncio
async def test_handler_invoker_accepts_keyword_only_candle() -> None:
    supervisor = DummySupervisor()
    config = DummyConfig()
    calls: list[dict] = []

    async def handler(*, candle: dict) -> None:
        calls.append(candle)

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    candle = {"symbol": "BTCUSDT", "timestamp": 1_500}
    await trader._handler_invoker(candle)

    assert calls == [candle]


@pytest.mark.asyncio
async def test_handler_invoker_requires_candle_parameter() -> None:
    supervisor = DummySupervisor()
    config = DummyConfig()

    async def invalid_handler(trader) -> None:  # type: ignore[no-untyped-def]
        return None

    trader = TraderLite(config, candle_handler=invalid_handler, supervisor=supervisor)

    with pytest.raises(TypeError):
        await trader._handler_invoker({"symbol": "BTCUSDT", "timestamp": 1_000})


@pytest.mark.asyncio
async def test_create_spread_guard_with_dynamic_config() -> None:
    supervisor = DummySupervisor()
    config = DummyConfig(
        max_spread_ratio=0.002,
        spread_dynamic=True,
        spread_guard_window=1,
        spread_guard_hysteresis=1.5,
        spread_guard_max_limit=0.001,
    )

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    guard = trader.spread_guard
    assert guard is not None
    assert guard.base_limit == pytest.approx(0.002)
    assert guard.window == 5  # normalizado al mínimo permitido
    assert guard.hysteresis == pytest.approx(1.0)
    assert guard._max_limit == pytest.approx(0.002)


@pytest.mark.asyncio
async def test_create_spread_guard_disabled_when_limit_zero() -> None:
    supervisor = DummySupervisor()
    config = DummyConfig(max_spread_ratio=0.0, spread_dynamic=True)

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    assert trader.spread_guard is None


@pytest.mark.asyncio
async def test_stop_detiene_feed_y_cierra_supervisor(stub_data_feed: list[Any]) -> None:
    supervisor = DummySupervisor()
    config = DummyConfig()

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    trader.start()
    await asyncio.sleep(0)
    await trader.stop()

    feed = stub_data_feed[0]
    assert feed.detener_calls == 1
    assert supervisor.shutdown_calls == 1


@pytest.mark.asyncio
async def test_start_registra_tareas_en_supervisor() -> None:
    supervisor = DummySupervisor()
    config = DummyConfig()

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    trader._stop_event.set()
    trader.start()
    await asyncio.sleep(0)

    assert {task["name"] for task in supervisor.supervised} == {"heartbeat_loop", "data_feed"}


@pytest.mark.asyncio
async def test_procesar_vela_registra_latencia_y_avisa_si_es_lento(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class DummyTrader(TraderLiteProcessingMixin):
        def __init__(self) -> None:
            self.feed = SimpleNamespace(handler_timeout=1.0)
            self.config = SimpleNamespace(intervalo_velas="1m")
            self.estado = {}
            self._handler_invoker = self._fake_invoker
            self._after_calls: list[str] = []

        async def _fake_invoker(self, candle: dict) -> None:
            candle.setdefault("_df_stage_durations", {})
            candle["_df_stage_durations"].update({"parse": 0.4, "gating": 0.3, "strategy": 0.5})

        def _after_procesar_vela(self, symbol: str) -> None:
            self._after_calls.append(symbol)

    trader = DummyTrader()

    class RecordingHistogram:
        def __init__(self) -> None:
            self.label_calls: list[dict[str, str]] = []
            self.observed: list[float] = []

        def labels(self, *, symbol: str, timeframe: str) -> "RecordingHistogram":
            self.label_calls.append({"symbol": symbol, "timeframe": timeframe})
            return self

        def observe(self, value: float) -> None:
            self.observed.append(value)

    histogram = RecordingHistogram()
    monkeypatch.setattr(
        "core.trader.trader_lite_processing.TRADER_PIPELINE_LATENCY",
        histogram,
    )

    perf_values = iter([100.0, 101.5, 101.5, 101.5])

    def fake_perf_counter() -> float:
        return next(perf_values, 101.5)

    monkeypatch.setattr(
        "core.trader.trader_lite_processing.time.perf_counter",
        fake_perf_counter,
    )

    candle = {"symbol": "BTCUSDT", "timestamp": 1_000, "timeframe": "1m"}

    caplog.set_level("WARNING", logger="trader_modular")

    await trader._procesar_vela(candle)

    assert histogram.label_calls == [{"symbol": "BTCUSDT", "timeframe": "1m"}]
    assert histogram.observed and histogram.observed[0] == pytest.approx(1.5, rel=1e-6)

    slow_records = [rec for rec in caplog.records if rec.getMessage() == "procesar_vela.slow"]
    assert slow_records, "Debe registrarse un warning cuando la latencia excede el umbral"
    record = slow_records[0]
    assert getattr(record, "elapsed_secs", 0.0) == pytest.approx(1.5, rel=1e-6)
    assert getattr(record, "threshold_secs", 0.0) == pytest.approx(0.8, rel=1e-6)
    assert isinstance(getattr(record, "stages", {}), dict)


@pytest.mark.asyncio
async def test_connection_signal_task_terminates_on_ws_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor = DummySupervisor()
    config = DummyConfig()

    class _FailingFeed:
        def __init__(self, intervalo: str, **_: Any) -> None:
            self.intervalo = intervalo
            self.ws_connected_event = asyncio.Event()
            self.ws_failed_event = asyncio.Event()
            self.detener_calls = 0

        async def escuchar(
            self,
            symbols: list[str] | tuple[str, ...],
            handler,
            *,
            cliente=None,
        ) -> None:  # pragma: no cover - no-op stub
            return None

        async def detener(self) -> None:
            self.detener_calls += 1

    monkeypatch.setattr("core.trader_modular.DataFeed", _FailingFeed, raising=False)

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)
    trader.start()
    await asyncio.sleep(0)

    watch = trader._connection_signal_task
    assert watch is not None
    assert not watch.done()

    trader.feed.ws_failed_event.set()
    await asyncio.sleep(0.05)

    assert watch.done(), "La tarea de señal debe finalizar tras el fallo del feed"
    assert watch.exception() is None
    assert trader._datafeed_connected_emitted is False

    await trader.stop()
