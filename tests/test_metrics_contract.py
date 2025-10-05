from __future__ import annotations

from core.metrics import BUFFERS_TAM, ENTRADAS_RECHAZADAS
from core.metrics_helpers import safe_inc, safe_set


def _labels_of(metric) -> set[str]:
    raw = getattr(metric, "_labelnames", None)
    if not raw:
        raw = getattr(metric, "labelnames", ())
    return set(raw or ())


def test_entradas_rechazadas_labels() -> None:
    assert _labels_of(ENTRADAS_RECHAZADAS) == {"symbol", "timeframe", "reason"}


def test_buffers_tam_labels() -> None:
    assert _labels_of(BUFFERS_TAM) == {"symbol", "timeframe"}


def test_safe_helpers_do_not_crash() -> None:
    safe_inc(
        ENTRADAS_RECHAZADAS,
        symbol="BTC/EUR",
        timeframe="5m",
        reason="x",
    )
    safe_set(
        BUFFERS_TAM,
        123,
        symbol="BTC/EUR",
        timeframe="5m",
    )