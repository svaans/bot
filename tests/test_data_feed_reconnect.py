"""Pruebas unitarias para la política de reconexión del DataFeed."""

from __future__ import annotations

from typing import Callable, List, Tuple

import pytest

import core.data_feed as data_feed_module
import core.data_feed.events as events_module
import core.data_feed.streaming as streaming_module
from core.data_feed import DataFeed
from core.utils.feature_flags import reset_flag_cache


def _collect_event_sink(events: List[Tuple[str, dict]]) -> Callable[[str, dict], None]:
    def _sink(evento: str, data: dict) -> None:
        events.append((evento, data))

    return _sink



class _DummyBus:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def emit(self, event: str, payload: dict) -> None:
        self.events.append((event, dict(payload)))


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

@pytest.mark.flags("datafeed.signals.enabled")
def test_register_reconnect_attempt_emits_bus_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATAFEED_SIGNALS_ENABLED", "true")
    reset_flag_cache()

    bus = _DummyBus()
    feed = DataFeed(
        "1m",
        max_reconnect_attempts=3,
        max_reconnect_time=None,
        event_bus=bus,
    )

    assert feed._register_reconnect_attempt("BTCUSDT", "error temporal") is True
    assert bus.events, "Se esperaba señal en el bus"
    event, payload = bus.events[-1]
    assert event == "datafeed.ws.retry"
    assert payload["attempts"] == 1
    assert payload["reason"] == "error temporal"
    assert payload["intervalo"] == "1m"

    reset_flag_cache()


@pytest.mark.flags("datafeed.signals.enabled")
def test_signal_ws_failure_emits_bus_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATAFEED_SIGNALS_ENABLED", "true")
    reset_flag_cache()

    bus = _DummyBus()
    feed = DataFeed("1m", event_bus=bus)

    feed._signal_ws_failure("ws_drop")

    assert bus.events, "Se esperaba señal de falla"
    event, payload = bus.events[-1]
    assert event == "datafeed.ws.failure"
    assert payload["reason"] == "ws_drop"
    assert payload["intervalo"] == "1m"

    reset_flag_cache()


@pytest.mark.flags("datafeed.signals.enabled")
def test_limit_exceeded_emits_bus_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATAFEED_SIGNALS_ENABLED", "true")
    reset_flag_cache()

    bus = _DummyBus()
    feed = DataFeed("1m", event_bus=bus, max_reconnect_attempts=1, max_reconnect_time=None)

    assert feed._register_reconnect_attempt("BTCUSDT", "fallo") is True
    assert feed._register_reconnect_attempt("BTCUSDT", "fallo") is False

    limit_events = [evt for evt in bus.events if evt[0] == "datafeed.ws.limit_exceeded"]
    assert limit_events, "Se esperaba señal de límite superado"
    last_event = limit_events[-1][1]
    assert last_event["limit"] == "attempts"
    assert last_event["attempts"] == 2

    reset_flag_cache()


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


def test_retry_log_context_reports_attempts_and_downtime(monkeypatch) -> None:
    reloj = {"valor": 100.0}

    def fake_monotonic() -> float:
        return reloj["valor"]

    monkeypatch.setattr(streaming_module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(data_feed_module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(events_module.time, "monotonic", fake_monotonic)

    feed = DataFeed(
        "1m",
        max_reconnect_attempts=5,
        max_reconnect_time=30.0,
    )

    context = streaming_module._build_retry_log_context(  # type: ignore[attr-defined]
        feed,
        "BTCUSDT",
        "error",
        include_next_attempt=True,
    )

    assert context["retry_attempt"] == 1
    assert context["retry_downtime"] == 0.0
    assert context["retry_max_attempts"] == 5
    assert context["retry_max_downtime"] == 30.0
    assert context["reason"] == "error"

    assert feed._register_reconnect_attempt("BTCUSDT", "fallo temporal") is True

    reloj["valor"] = 112.5

    context_next = streaming_module._build_retry_log_context(  # type: ignore[attr-defined]
        feed,
        "BTCUSDT",
        "error",
        include_next_attempt=True,
    )

    assert context_next["retry_attempt"] == 2
    assert context_next["retry_downtime"] == pytest.approx(12.5, rel=1e-6)

    context_guard = streaming_module._build_retry_log_context(  # type: ignore[attr-defined]
        feed,
        "BTCUSDT",
        "loop_guard",
        include_next_attempt=False,
    )

    assert context_guard["retry_attempt"] == 1
    assert context_guard["reason"] == "loop_guard"
