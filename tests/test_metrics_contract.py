from __future__ import annotations

from core.metrics import BUFFER_SIZE_V2, ENTRADAS_RECHAZADAS_V2
from core.metrics_helpers import safe_inc, safe_set


def _labels_of(metric) -> set[str]:
    raw = getattr(metric, "_labelnames", None)
    if not raw:
        raw = getattr(metric, "labelnames", ())
    return set(raw or ())


def test_entradas_rechazadas_labels() -> None:
    assert _labels_of(ENTRADAS_RECHAZADAS_V2) == {"timeframe", "reason"}


def test_buffer_size_v2_labels() -> None:
    assert _labels_of(BUFFER_SIZE_V2) == {"timeframe"}


def test_safe_helpers_do_not_crash() -> None:
    safe_inc(
        ENTRADAS_RECHAZADAS_V2,
        timeframe="5m",
        reason="x",
    )
    safe_set(
        BUFFER_SIZE_V2,
        123,
        timeframe="5m",
    )
