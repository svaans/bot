from __future__ import annotations

from core.metrics import (
    BUFFER_SIZE_V2,
    CANDLES_DUPLICADAS_RATE,
    ENTRADAS_RECHAZADAS_V2,
    TRADER_PIPELINE_LATENCY,
    TRADER_PIPELINE_QUEUE_WAIT,
    TRADER_QUEUE_SIZE,
    VELAS_DUPLICADAS,
    VELAS_RECHAZADAS,
    VELAS_RECHAZADAS_PCT,
    VELAS_TOTAL,
)
from core.metrics_helpers import safe_inc, safe_set
from observability.metrics import (
    BOT_BACKFILL_WINDOW_RUNS_TOTAL,
    BOT_DATAFEED_WS_FAILURES_TOTAL,
    BOT_LIMIT_ORDERS_SUBMITTED_TOTAL,
    BOT_ORDERS_RETRY_SCHEDULED_TOTAL,
    BOT_TRADER_PURGE_RUNS_TOTAL,
    CAPITAL_CONFIGURED_GAUGE,
    CAPITAL_CONFIGURED_TOTAL,
    CAPITAL_DIVERGENCE_ABSOLUTE,
    CAPITAL_DIVERGENCE_RATIO,
    CAPITAL_DIVERGENCE_THRESHOLD,
    CAPITAL_REGISTERED_GAUGE,
    CAPITAL_REGISTERED_TOTAL,
)


def _labels_of(metric) -> set[str]:
    raw = getattr(metric, "_labelnames", None)
    if not raw:
        raw = getattr(metric, "labelnames", ())
    return set(raw or ())


def test_entradas_rechazadas_labels() -> None:
    assert _labels_of(ENTRADAS_RECHAZADAS_V2) == {"symbol", "timeframe", "reason"}


def test_buffer_size_v2_labels() -> None:
    assert _labels_of(BUFFER_SIZE_V2) == {"timeframe"}


def test_candle_metrics_include_timeframe() -> None:
    expected = {"symbol", "timeframe"}
    assert _labels_of(VELAS_TOTAL) == expected
    assert _labels_of(VELAS_DUPLICADAS) == expected
    assert _labels_of(CANDLES_DUPLICADAS_RATE) == expected
    assert _labels_of(VELAS_RECHAZADAS_PCT) == expected
    assert _labels_of(VELAS_RECHAZADAS) == expected | {"reason"}


def test_trader_pipeline_metrics_include_timeframe() -> None:
    expected = {"symbol", "timeframe"}
    assert _labels_of(TRADER_QUEUE_SIZE) == expected
    assert _labels_of(TRADER_PIPELINE_LATENCY) == expected
    assert _labels_of(TRADER_PIPELINE_QUEUE_WAIT) == expected


def test_safe_helpers_do_not_crash() -> None:
    safe_inc(
        ENTRADAS_RECHAZADAS_V2,
        symbol="BTCUSDT",
        timeframe="5m",
        reason="x",
    )
    safe_set(
        BUFFER_SIZE_V2,
        123,
        timeframe="5m",
    )


def test_bot_guardrail_metrics_labels() -> None:
    assert _labels_of(BOT_ORDERS_RETRY_SCHEDULED_TOTAL) == {"symbol"}
    assert _labels_of(BOT_TRADER_PURGE_RUNS_TOTAL) == {"symbol"}
    assert _labels_of(BOT_DATAFEED_WS_FAILURES_TOTAL) == {"reason"}
    assert _labels_of(BOT_LIMIT_ORDERS_SUBMITTED_TOTAL) == {"symbol", "side"}
    assert _labels_of(BOT_BACKFILL_WINDOW_RUNS_TOTAL) == {"symbol", "timeframe"}


def test_capital_metrics_labels() -> None:
    assert _labels_of(CAPITAL_REGISTERED_GAUGE) == {"symbol"}
    assert _labels_of(CAPITAL_CONFIGURED_GAUGE) == {"symbol"}
    assert _labels_of(CAPITAL_REGISTERED_TOTAL) == set()
    assert _labels_of(CAPITAL_CONFIGURED_TOTAL) == set()
    assert _labels_of(CAPITAL_DIVERGENCE_ABSOLUTE) == set()
    assert _labels_of(CAPITAL_DIVERGENCE_RATIO) == set()
    assert _labels_of(CAPITAL_DIVERGENCE_THRESHOLD) == set()
