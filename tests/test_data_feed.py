from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone
from typing import Any, Callable

import pytest

from core.utils.feature_flags import reset_flag_cache
from core.data_feed import DataFeed
from core.event_bus import EventBus


@pytest.fixture
def capture_events() -> list[tuple[str, dict[str, Any]]]:
    return []


def make_feed(
    *,
    events: list[tuple[str, dict[str, Any]]] | None = None,
    **kwargs: Any,
) -> DataFeed:
    on_event: Callable[[str, dict[str, Any]], None] | None = None
    if events is not None:
        def _collector(evt: str, data: dict[str, Any]) -> None:
            events.append((evt, dict(data)))

        on_event = _collector
    return DataFeed("1m", on_event=on_event, **kwargs)


def test_queue_min_recommended_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setenv("DF_QUEUE_MIN_RECOMMENDED", "5")

    feed = make_feed(
        events=events,
        queue_max=2,
        queue_autotune=False,
        queue_policy="drop_oldest",
    )

    assert feed.queue_min_recommended == 5
    assert feed._queue_capacity_warning_emitted is True
    assert any(evt == "queue_capacity_below_recommended" for evt, _ in events)


@pytest.mark.asyncio
async def test_queue_capacity_breach_event_emitted(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setenv("DF_QUEUE_MIN_RECOMMENDED", "6")

    feed = make_feed(
        events=events,
        queue_max=2,
        queue_autotune=False,
        queue_policy="drop_oldest",
    )
    assert feed.queue_policy == "drop_oldest"
    symbol = "BTCUSDT"
    feed._queues[symbol] = asyncio.Queue(maxsize=2)

    base_ts = 1_650_000_000_000
    for idx in range(3):
        candle = {
            "symbol": symbol,
            "timestamp": base_ts + idx * 60_000,
            "is_closed": True,
        }
        await feed._handle_candle(symbol, candle)

    assert any(evt == "queue_drop" for evt, _ in events)
    assert any(evt == "queue_capacity_breach" for evt, _ in events)
    assert feed._queue_capacity_breach_logged is True
    assert feed._stats[symbol]["dropped"] == 1


def test_queue_autotune_switches_to_block_when_required() -> None:
    events: list[tuple[str, dict[str, Any]]] = []

    feed = make_feed(
        events=events,
        queue_max=2,
        queue_policy="drop_oldest",
        queue_min_recommended=8,
        queue_burst_factor=2.0,
    )

    assert feed.queue_policy == "block"
    assert feed.queue_max == 16
    assert feed._queue_autotune_applied is True
    assert any(evt == "queue_autotune" for evt, _ in events)


def test_queue_autotune_respects_disable_flag() -> None:
    feed = make_feed(
        queue_max=2,
        queue_policy="drop_oldest",
        queue_min_recommended=8,
        queue_autotune=False,
        queue_burst_factor=2.0,
    )

    assert feed.queue_policy == "drop_oldest"
    assert feed.queue_max == 2
    assert feed._queue_autotune_applied is False


@pytest.mark.asyncio
async def test_event_bus_queue_drop_adjusts_backfill_window() -> None:
    feed = make_feed(queue_autotune=False)
    feed._queues["BTCUSDT"] = asyncio.Queue(maxsize=3)
    feed._backfill_window_target = 2
    feed._backfill_max = 50

    bus = EventBus()
    feed.event_bus = bus

    bus.emit("queue_drop", {"symbol": "BTCUSDT"})
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    resolver = getattr(feed, "_resolve_backfill_window_target")
    assert callable(resolver)
    assert resolver("BTCUSDT") == 3

    notifier = getattr(feed, "_note_backfill_window_recovery")
    assert callable(notifier)
    notifier("BTCUSDT", 1)
    assert resolver("BTCUSDT") == 2


@pytest.mark.asyncio
async def test_handle_candle_enqueues_closed_aligned(capture_events: list[tuple[str, dict[str, Any]]]) -> None:
    feed = make_feed(events=capture_events)
    symbol = "BTCUSDT"
    feed._queues[symbol] = asyncio.Queue()

    candle = {
        "symbol": symbol,
        "timestamp": 1_650_000_000_000,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 100.0,
        "is_closed": True,
    }

    await feed._handle_candle(symbol, candle)

    assert feed._queues[symbol].qsize() == 1
    queued = feed._queues[symbol].get_nowait()
    feed._queues[symbol].task_done()
    assert queued["timestamp"] == candle["timestamp"]
    assert feed._stats[symbol]["received"] == 1
    assert feed._last_close_ts[symbol] == candle["timestamp"]
    assert any(evt == "tick" for evt, _ in capture_events)


@pytest.mark.asyncio
async def test_handle_candle_accepts_close_time_when_timestamp_missing() -> None:
    feed = make_feed()
    symbol = "ETHUSDT"
    feed._queues[symbol] = asyncio.Queue()

    base_ts = 1_650_000_000_000
    candle = {
        "symbol": symbol,
        "close_time": base_ts,
        "open_time": base_ts - 60_000,
        "is_closed": True,
    }

    await feed._handle_candle(symbol, candle)

    assert feed._queues[symbol].qsize() == 1
    queued = feed._queues[symbol].get_nowait()
    feed._queues[symbol].task_done()
    assert queued["timestamp"] == base_ts
    assert feed._stats[symbol]["received"] == 1
    assert feed._last_close_ts[symbol] == base_ts


@pytest.mark.asyncio
async def test_handle_candle_ignores_non_closed() -> None:
    feed = make_feed()
    symbol = "ETHUSDT"
    feed._queues[symbol] = asyncio.Queue()

    candle = {
        "symbol": symbol,
        "timestamp": 1_650_000_000_000,
        "is_closed": False,
    }

    await feed._handle_candle(symbol, candle)

    assert feed._queues[symbol].empty()
    assert feed._stats[symbol] == {}


@pytest.mark.asyncio
async def test_handle_candle_normalizes_binance_payload() -> None:
    feed = make_feed()
    symbol = "BTCUSDT"
    feed._queues[symbol] = asyncio.Queue()

    base_open = 1_650_000_000_000
    base_close = base_open + 60_000
    raw_payload = {
        "e": "kline",
        "E": base_close,
        "s": symbol,
        "k": {
            "t": base_open,
            "T": base_close,
            "s": symbol,
            "i": "1m",
            "o": "100.0",
            "c": "101.5",
            "h": "102.0",
            "l": "99.5",
            "v": "12.5",
            "x": False,
        },
    }

    await feed._handle_candle(symbol, raw_payload)
    assert feed._queues[symbol].empty()

    raw_payload["k"]["x"] = True
    await feed._handle_candle(symbol, raw_payload)

    assert feed._queues[symbol].qsize() == 1
    queued = feed._queues[symbol].get_nowait()
    feed._queues[symbol].task_done()

    assert queued["timestamp"] == base_open
    assert queued["event_time"] == base_close
    assert queued["close"] == pytest.approx(101.5)
    assert queued["is_closed"] is True


@pytest.mark.asyncio
async def test_handle_candle_ignores_misaligned_timestamp() -> None:
    feed = make_feed()
    symbol = "BNBUSDT"
    feed._queues[symbol] = asyncio.Queue()

    candle = {
        "symbol": symbol,
        "timestamp": 1_650_000_000_123,
        "is_closed": True,
    }

    await feed._handle_candle(symbol, candle)

    assert feed._queues[symbol].empty()
    assert feed._stats[symbol] == {}


@pytest.mark.asyncio
async def test_handle_candle_rejects_duplicate_timestamp() -> None:
    feed = make_feed()
    symbol = "XRPUSDT"
    feed._queues[symbol] = asyncio.Queue()

    first = {
        "symbol": symbol,
        "timestamp": 1_650_000_000_000,
        "is_closed": True,
    }
    duplicate = {
        "symbol": symbol,
        "timestamp": 1_650_000_000_000,
        "is_closed": True,
    }

    await feed._handle_candle(symbol, first)
    await feed._handle_candle(symbol, duplicate)

    assert feed._queues[symbol].qsize() == 1
    assert feed._stats[symbol]["received"] == 1


@pytest.mark.asyncio
async def test_handle_candle_drops_oldest_when_queue_full() -> None:
    feed = make_feed()
    symbol = "ADAUSDT"
    feed._queues[symbol] = asyncio.Queue(maxsize=1)

    first = {
        "symbol": symbol,
        "timestamp": 1_650_000_000_000,
        "is_closed": True,
    }
    second = {
        "symbol": symbol,
        "timestamp": 1_650_000_060_000,
        "is_closed": True,
    }

    await feed._handle_candle(symbol, first)
    await feed._handle_candle(symbol, second)

    assert feed._queues[symbol].qsize() == 1
    queued = feed._queues[symbol].get_nowait()
    feed._queues[symbol].task_done()
    assert queued["timestamp"] == second["timestamp"]


@pytest.mark.asyncio
async def test_handle_candle_drop_newest_policy_drops_incoming() -> None:
    events: list[tuple[str, dict[str, Any]]] = []
    feed = make_feed(
        events=events,
        queue_policy="drop_newest",
        queue_max=1,
        queue_autotune=False,
    )
    symbol = "FTMUSDT"
    feed._queues[symbol] = asyncio.Queue(maxsize=1)

    first = {
        "symbol": symbol,
        "timestamp": 1_650_000_000_000,
        "is_closed": True,
    }
    second = {
        "symbol": symbol,
        "timestamp": 1_650_000_060_000,
        "is_closed": True,
    }

    await feed._handle_candle(symbol, first)
    await feed._handle_candle(symbol, second)

    assert feed._queues[symbol].qsize() == 1
    queued = feed._queues[symbol].get_nowait()
    feed._queues[symbol].task_done()
    assert queued["timestamp"] == first["timestamp"]
    assert feed._stats[symbol]["dropped"] == 1
    assert any(evt == "queue_drop" for evt, _ in events)


@pytest.mark.asyncio
async def test_handle_candle_records_metrics_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("METRICS_EXTENDED_ENABLED", "true")
    reset_flag_cache()

    recorded: dict[str, list[tuple[str, str | None]]] = {
        "received": [],
        "rejected": [],
    }

    def fake_recibida(symbol: str, timeframe: str | None = None) -> None:
        recorded["received"].append((symbol, timeframe))

    def fake_rechazada(symbol: str, reason: str, timeframe: str | None = None) -> None:
        recorded["rejected"].append((symbol, reason, timeframe))

    monkeypatch.setattr(
        "core.data_feed.handlers.registrar_vela_recibida",
        fake_recibida,
    )
    monkeypatch.setattr(
        "core.data_feed.handlers.registrar_vela_rechazada",
        fake_rechazada,
    )

    feed = make_feed()
    symbol = "BTCUSDT"
    feed._queues[symbol] = asyncio.Queue()

    candle = {
        "symbol": symbol,
        "timestamp": 1_650_000_000_000,
        "is_closed": True,
    }

    await feed._handle_candle(symbol, candle)
    assert recorded["received"] == [(symbol, "1m")]

    rejected = {
        "symbol": symbol,
        "timestamp": 1_650_000_060_000,
        "is_closed": False,
    }
    await feed._handle_candle(symbol, rejected)
    assert (symbol, "not_closed", "1m") in recorded["rejected"]
    reset_flag_cache()
    # Solo la vela cerrada se contabiliza como recibida.
    assert feed._stats[symbol]["received"] == 1


@pytest.mark.asyncio
async def test_handle_candle_realiza_backfill_de_ventana(monkeypatch: pytest.MonkeyPatch) -> None:
    feed = make_feed()
    symbol = "BTCUSDT"
    feed._queues[symbol] = asyncio.Queue()
    feed._cliente = object()
    feed._backfill_ventana_enabled = True
    feed._backfill_window_target = 5
    feed._backfill_max = 5

    base_ts = 1_650_000_000_000
    intervalo_ms = feed.intervalo_segundos * 1000

    first = {
        "symbol": symbol,
        "timestamp": base_ts,
        "open": 1.0,
        "high": 1.1,
        "low": 0.9,
        "close": 1.05,
        "volume": 10.0,
        "is_closed": True,
    }

    await feed._handle_candle(symbol, first)

    first_item = feed._queues[symbol].get_nowait()
    assert int(first_item["timestamp"]) == first["timestamp"]
    feed._queues[symbol].task_done()

    extras = [
        {
            "timestamp": base_ts + intervalo_ms,
            "open": 1.1,
            "high": 1.2,
            "low": 1.0,
            "close": 1.15,
            "volume": 12.0,
        },
        {
            "timestamp": base_ts + 2 * intervalo_ms,
            "open": 1.15,
            "high": 1.25,
            "low": 1.1,
            "close": 1.2,
            "volume": 14.0,
        },
    ]

    async def fake_backfill(
        _symbol: str,
        limit: int,
        *,
        intervalo: str,
        cliente: Any | None,
        fetcher: Any | None = None,
    ) -> list[dict[str, Any]]:
        return extras[:limit]

    monkeypatch.setattr("core.adaptador_dinamico.backfill", fake_backfill)

    gap_candle = {
        "symbol": symbol,
        "timestamp": base_ts + 3 * intervalo_ms,
        "open": 1.2,
        "high": 1.3,
        "low": 1.15,
        "close": 1.25,
        "volume": 16.0,
        "is_closed": True,
    }

    await feed._handle_candle(symbol, gap_candle)

    queued_ts: list[int] = []
    while not feed._queues[symbol].empty():
        item = feed._queues[symbol].get_nowait()
        queued_ts.append(int(item["timestamp"]))
        feed._queues[symbol].task_done()

    assert queued_ts == [extras[0]["timestamp"], extras[1]["timestamp"], gap_candle["timestamp"]]


@pytest.mark.asyncio
async def test_consumer_processes_and_updates_stats(capture_events: list[tuple[str, dict[str, Any]]]) -> None:
    processed: list[dict[str, Any]] = []

    async def handler(candle: dict[str, Any]) -> None:
        processed.append(dict(candle))

    feed = make_feed(events=capture_events)
    symbol = "SOLUSDT"
    feed._handler = handler
    feed._running = True
    feed._queues[symbol] = asyncio.Queue()

    task = asyncio.create_task(feed._consumer(symbol))

    candle = {"symbol": symbol, "timestamp": 1_650_000_000_000, "is_closed": True}
    await feed._queues[symbol].put(candle)
    await asyncio.wait_for(feed._queues[symbol].join(), timeout=1)

    feed._running = False
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert processed == [candle]
    assert feed._stats[symbol]["processed"] == 1
    assert feed._stats[symbol]["handler_calls"] == 1
    assert any(evt == "consumer_state" and data["state"] == "healthy" for evt, data in capture_events)


@pytest.mark.asyncio
async def test_consumer_handler_timeout_emits_event(capture_events: list[tuple[str, dict[str, Any]]]) -> None:
    async def handler(_: dict[str, Any]) -> None:
        await asyncio.sleep(0.2)

    feed = make_feed(events=capture_events, handler_timeout=0.1)
    symbol = "DOTUSDT"
    feed._handler = handler
    feed._running = True
    feed._queues[symbol] = asyncio.Queue()

    task = asyncio.create_task(feed._consumer(symbol))

    candle = {"symbol": symbol, "timestamp": 1_650_000_000_000, "is_closed": True}
    await feed._queues[symbol].put(candle)
    await asyncio.wait_for(feed._queues[symbol].join(), timeout=1)

    feed._running = False
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert feed._stats[symbol]["handler_timeouts"] == 1
    assert any(evt == "handler_timeout" for evt, _ in capture_events)


@pytest.mark.asyncio
async def test_consumer_handler_exception_emits_error(capture_events: list[tuple[str, dict[str, Any]]]) -> None:
    async def handler(_: dict[str, Any]) -> None:
        raise RuntimeError("boom")

    feed = make_feed(events=capture_events)
    symbol = "MATICUSDT"
    feed._handler = handler
    feed._running = True
    feed._queues[symbol] = asyncio.Queue()

    task = asyncio.create_task(feed._consumer(symbol))

    candle = {"symbol": symbol, "timestamp": 1_650_000_000_000, "is_closed": True}
    await feed._queues[symbol].put(candle)
    await asyncio.wait_for(feed._queues[symbol].join(), timeout=1)

    feed._running = False
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert feed._stats[symbol]["consumer_errors"] == 1
    assert any(evt == "consumer_error" for evt, _ in capture_events)


@pytest.mark.asyncio
async def test_do_backfill_fetches_and_enqueues_candles(
    monkeypatch: pytest.MonkeyPatch,
    capture_events: list[tuple[str, dict[str, Any]]],
) -> None:
    feed = make_feed(events=capture_events)
    feed._cliente = object()
    symbol = "AVAXUSDT"
    feed._queues[symbol] = asyncio.Queue()
    feed.min_buffer_candles = 2

    base_ts = 1_650_000_000_000
    candles = [
        [base_ts, 1.0, 2.0, 0.5, 1.5, 10.0],
        [base_ts + 60_000, 1.5, 2.5, 1.0, 2.0, 12.0],
    ]

    async def fake_fetch(
        _cliente: Any,
        _symbol: str,
        _intervalo: str,
        *,
        since: int,
        limit: int,
    ) -> list[list[float]]:
        return list(candles)

    monkeypatch.setattr("core.data_feed.fetch_ohlcv_async", fake_fetch)
    monkeypatch.setattr("core.data_feed.validar_integridad_velas", lambda *args, **kwargs: True)

    await feed._do_backfill(symbol)

    assert feed._last_backfill_ts[symbol] == candles[-1][0]
    assert feed._queues[symbol].qsize() == len(candles)
    assert feed._stats[symbol]["received"] == len(candles)
    assert any(evt == "backfill_ok" for evt, _ in capture_events)


@pytest.mark.asyncio
async def test_do_backfill_emits_error_on_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
    capture_events: list[tuple[str, dict[str, Any]]],
) -> None:
    feed = make_feed(events=capture_events)
    feed._cliente = object()
    symbol = "FTMUSDT"
    feed._queues[symbol] = asyncio.Queue()

    async def failing_fetch(*_: Any, **__: Any) -> list[list[float]]:
        raise RuntimeError("network down")

    monkeypatch.setattr("core.data_feed.fetch_ohlcv_async", failing_fetch)
    monkeypatch.setattr("core.data_feed.validar_integridad_velas", lambda *args, **kwargs: True)

    await feed._do_backfill(symbol)

    assert feed._queues[symbol].empty()
    assert symbol not in feed._last_backfill_ts
    assert any(evt == "backfill_error" for evt, _ in capture_events)


@pytest.mark.asyncio
async def test_consumer_does_not_enable_debug_wrapper_without_flag() -> None:
    reset_flag_cache()

    feed = make_feed()
    symbol = "BTCUSDT"
    feed._running = True
    feed._handler_log_events = 0
    feed._queues[symbol] = asyncio.Queue()

    async def handler(_candle: dict) -> None:
        feed._running = False

    feed._handler = handler

    task = asyncio.create_task(feed._consumer(symbol))

    candle = {"symbol": symbol, "timestamp": 1_650_000_000_000, "is_closed": True}
    await feed._queues[symbol].put(candle)
    await asyncio.wait_for(feed._queues[symbol].join(), timeout=1)
    await asyncio.wait_for(task, timeout=1)

    assert not getattr(feed._handler, "__df_debug_wrapper__", False)
    reset_flag_cache()


@pytest.mark.asyncio
async def test_consumer_enables_debug_wrapper_with_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATAFEED_DEBUG_WRAPPER_ENABLED", "true")
    reset_flag_cache()

    feed = make_feed()
    symbol = "ETHUSDT"
    feed._running = True
    feed._handler_log_events = 0
    feed._queues[symbol] = asyncio.Queue()

    async def handler(_candle: dict) -> None:
        feed._running = False

    feed._handler = handler

    task = asyncio.create_task(feed._consumer(symbol))

    candle = {"symbol": symbol, "timestamp": 1_650_100_000_000, "is_closed": True}
    await feed._queues[symbol].put(candle)
    await asyncio.wait_for(feed._queues[symbol].join(), timeout=1)
    await asyncio.wait_for(task, timeout=1)

    assert getattr(feed._handler, "__df_debug_wrapper__", False)
    reset_flag_cache()
