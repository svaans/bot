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
