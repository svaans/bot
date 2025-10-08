from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone
from typing import Any, Callable

import pytest

from core.data_feed import DataFeed


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

    assert queued["timestamp"] == base_close
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
    assert feed._stats[symbol]["dropped"] == 1
    assert feed._stats[symbol]["received"] == 2


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
