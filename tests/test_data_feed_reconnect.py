"""Pruebas unitarias para la política de reconexión del DataFeed."""

from __future__ import annotations

from typing import Callable, List, Tuple

import core.data_feed as data_feed_module
from core.data_feed import DataFeed


def _collect_event_sink(events: List[Tuple[str, dict]]) -> Callable[[str, dict], None]:
    def _sink(evento: str, data: dict) -> None:
        events.append((evento, data))

    return _sink


def test_register_reconnect_attempt_respects_max_attempts() -> None:
    eventos: List[Tuple[str, dict]] = []
    feed = DataFeed(
        "1m",
        max_reconnect_attempts=2,
        max_reconnect_time=None,
        on_event=_collect_event_sink(eventos),
    )

    assert feed._register_reconnect_attempt("BTCUSDT", "error temporal") is True
    assert feed._register_reconnect_attempt("BTCUSDT", "error temporal") is True
    assert feed._register_reconnect_attempt("BTCUSDT", "error temporal") is False

    assert feed.ws_failed_event.is_set()

    retry_events = [evt for evt in eventos if evt[0] == "ws_retry"]
    assert len(retry_events) == 2

    exceeded_events = [evt for evt in eventos if evt[0] == "ws_retries_exceeded"]
    assert exceeded_events
    assert exceeded_events[-1][1]["attempts"] == 3


def test_register_reconnect_attempt_respects_max_downtime(monkeypatch) -> None:
    reloj = {"valor": 0.0}

    def fake_monotonic() -> float:
        return reloj["valor"]

    monkeypatch.setattr(data_feed_module.time, "monotonic", fake_monotonic)

    eventos: List[Tuple[str, dict]] = []
    feed = DataFeed(
        "1m",
        max_reconnect_attempts=None,
        max_reconnect_time=1.0,
        on_event=_collect_event_sink(eventos),
    )

    assert feed._register_reconnect_attempt("ETHUSDT", "ws drop") is True
    reloj["valor"] = 1.5
    assert feed._register_reconnect_attempt("ETHUSDT", "ws drop") is False

    downtime_events = [evt for evt in eventos if evt[0] == "ws_downtime_exceeded"]
    assert downtime_events
    assert downtime_events[-1][1]["elapsed"] >= 1.0


def test_reset_reconnect_tracking_clears_counters() -> None:
    feed = DataFeed("1m", max_reconnect_attempts=1, on_event=lambda *_: None)

    assert feed._register_reconnect_attempt("BTCUSDT", "fallo") is True
    feed._reset_reconnect_tracking("BTCUSDT")
    assert feed._register_reconnect_attempt("BTCUSDT", "fallo") is True


def test_verify_reconnect_limits_guard_respects_max_downtime(monkeypatch) -> None:
    reloj = {"valor": 0.0}

    def fake_monotonic() -> float:
        return reloj["valor"]

    monkeypatch.setattr(data_feed_module.time, "monotonic", fake_monotonic)

    eventos: List[Tuple[str, dict]] = []
    feed = DataFeed(
        "1m",
        max_reconnect_attempts=None,
        max_reconnect_time=1.0,
        on_event=_collect_event_sink(eventos),
    )

    assert feed._register_reconnect_attempt("ETHUSDT", "ws drop") is True
    reloj["valor"] = 1.5

    assert feed._verify_reconnect_limits("ETHUSDT", "loop_guard") is False
    assert feed.ws_failed_event.is_set()

    downtime_events = [evt for evt in eventos if evt[0] == "ws_downtime_exceeded"]
    assert downtime_events
    assert downtime_events[-1][1]["reason"] == "loop_guard"
    assert downtime_events[-1][1]["elapsed"] >= 1.5
