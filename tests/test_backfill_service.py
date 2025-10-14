from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

import pytest

from core.backfill_service import BackfillService

TF_MS = 5 * 60 * 1000


class DummyCounter:
    def __init__(self) -> None:
        self.values: Dict[Tuple[Tuple[str, str], ...], float] = defaultdict(float)

    def labels(self, **labels: str) -> "DummyCounter":
        key = tuple(sorted(labels.items()))
        self.values.setdefault(key, 0.0)
        return DummyCounterChild(self.values, key)


class DummyCounterChild:
    def __init__(self, store: Dict[Tuple[Tuple[str, str], ...], float], key: Tuple[Tuple[str, str], ...]) -> None:
        self._store = store
        self._key = key

    def inc(self, amount: float = 1.0) -> None:
        self._store[self._key] = self._store.get(self._key, 0.0) + amount


class DummyGauge:
    def __init__(self) -> None:
        self.values: Dict[Tuple[Tuple[str, str], ...], float] = {}
        self.labelnames = ("timeframe",)

    def labels(self, **labels: str) -> "DummyGauge":
        key = tuple(sorted(labels.items()))
        return DummyGaugeChild(self.values, key)


class DummyGaugeChild:
    def __init__(self, store: Dict[Tuple[Tuple[str, str], ...], float], key: Tuple[Tuple[str, str], ...]) -> None:
        self._store = store
        self._key = key

    def set(self, value: float) -> None:
        self._store[self._key] = value

    def inc(self, amount: float = 1.0) -> None:  # pragma: no cover - compat
        self._store[self._key] = self._store.get(self._key, 0.0) + amount


class DummyHistogram:
    def __init__(self) -> None:
        self.values: Dict[Tuple[Tuple[str, str], ...], List[float]] = defaultdict(list)

    def labels(self, **labels: str) -> "DummyHistogram":
        key = tuple(sorted(labels.items()))
        return DummyHistogramChild(self.values, key)


class DummyHistogramChild:
    def __init__(self, store: Dict[Tuple[Tuple[str, str], ...], List[float]], key: Tuple[Tuple[str, str], ...]) -> None:
        self._store = store
        self._key = key

    def observe(self, value: float) -> None:
        self._store[self._key].append(value)


class DummyMetrics:
    def __init__(self) -> None:
        self.backfill_requests_total = DummyCounter()
        self.backfill_klines_fetched_total = DummyCounter()
        self.backfill_duration_seconds = DummyHistogram()
        self.backfill_gaps_found_total = DummyCounter()
        self.buffer_size_v2 = DummyGauge()


class BufferStub:
    def __init__(self) -> None:
        self._store: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)

    def append_many(self, symbol: str, timeframe: str, candles: List[Dict[str, Any]]) -> None:
        self._store[(symbol, timeframe)].extend(candles)

    def get(self, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        return list(self._store[(symbol, timeframe)])


def _make_candle(open_time: int, value: float) -> Dict[str, Any]:
    return {
        "open_time": open_time,
        "open": value,
        "high": value + 1,
        "low": value - 1,
        "close": value,
        "volume": 10.0,
        "close_time": open_time + TF_MS - 1,
    }


@pytest.mark.asyncio
@pytest.mark.flags("backfill.ventana.enabled")
async def test_backfill_run_loads_expected_candles() -> None:
    metrics = DummyMetrics()
    buffer = BufferStub()
    ready: Dict[Tuple[str, str], bool] = {}
    base = (1_700_000_000_000 // TF_MS) * TF_MS
    candles = [_make_candle(base + i * TF_MS, float(i)) for i in range(6)]

    async def fetch(symbol: str, timeframe: str, start: int, end: int, limit: int) -> List[Dict[str, Any]]:
        await asyncio.sleep(0)
        max_open = end - TF_MS
        data = [c for c in candles if start <= c["open_time"] <= max_open]
        return data[:limit]

    service = BackfillService(
        fetch,
        buffer.append_many,
        buffer.get,
        lambda s, t, r: ready.__setitem__((s, t), r),
        None,
        metrics,
        page_limit=2,
    )

    end_dt = datetime.fromtimestamp((base + 6 * TF_MS) / 1000, tz=timezone.utc)
    await service.run(["BTC/EUR"], "5m", min_needed=3, warmup_extra=2, mode="A", end_time=end_dt)

    stored = buffer.get("BTC/EUR", "5m")
    assert len(stored) == 5
    assert [c["open_time"] for c in stored] == [base + TF_MS * i for i in range(1, 6)]
    assert ready[("BTC/EUR", "5m")]

    key = tuple(sorted({"symbol": "BTC/EUR", "timeframe": "5m", "status": "ok"}.items()))
    assert metrics.backfill_requests_total.values[key] == 1
    gauge_key = tuple(sorted({"timeframe": "5m"}.items()))
    assert metrics.buffer_size_v2.values[gauge_key] == len(stored)


@pytest.mark.asyncio
@pytest.mark.flags("backfill.ventana.enabled")
async def test_backfill_deduplicates_and_orders() -> None:
    metrics = DummyMetrics()
    buffer = BufferStub()
    ready: Dict[Tuple[str, str], bool] = {}
    base = (1_700_100_000_000 // TF_MS) * TF_MS
    raw = [
        _make_candle(base + TF_MS, 1.0),
        _make_candle(base, 0.0),
        {**_make_candle(base, 99.0), "volume": 50.0},
        _make_candle(base + 2 * TF_MS, 2.0),
    ]

    async def fetch(symbol: str, timeframe: str, start: int, end: int, limit: int) -> List[Dict[str, Any]]:
        await asyncio.sleep(0)
        data = [c for c in raw if c["open_time"] >= start]
        return data[:limit]

    service = BackfillService(
        fetch,
        buffer.append_many,
        buffer.get,
        lambda s, t, r: ready.__setitem__((s, t), r),
        None,
        metrics,
        page_limit=3,
    )

    end_dt = datetime.fromtimestamp((base + 3 * TF_MS) / 1000, tz=timezone.utc)
    await service.run(["BTC/EUR"], "5m", min_needed=2, warmup_extra=1, mode="A", end_time=end_dt)
    stored = buffer.get("BTC/EUR", "5m")
    assert [c["open_time"] for c in stored] == [base, base + TF_MS, base + 2 * TF_MS]
    volumes = [c["volume"] for c in stored]
    assert volumes[0] == 10.0


@pytest.mark.asyncio
async def test_backfill_refetches_gaps() -> None:
    metrics = DummyMetrics()
    buffer = BufferStub()
    ready: Dict[Tuple[str, str], bool] = {}
    base = (1_700_200_000_000 // TF_MS) * TF_MS
    missing = base + 2 * TF_MS

    async def fetch(symbol: str, timeframe: str, start: int, end: int, limit: int) -> List[Dict[str, Any]]:
        await asyncio.sleep(0)
        if start == missing:
            return [_make_candle(missing, 42.0)]
        data = [
            _make_candle(base, 0.0),
            _make_candle(base + TF_MS, 1.0),
            _make_candle(base + 3 * TF_MS, 3.0),
            _make_candle(base + 4 * TF_MS, 4.0),
        ]
        max_open = end - TF_MS
        return [c for c in data if start <= c["open_time"] <= max_open][:limit]

    service = BackfillService(
        fetch,
        buffer.append_many,
        buffer.get,
        lambda s, t, r: ready.__setitem__((s, t), r),
        None,
        metrics,
        max_refetches=2,
        page_limit=4,
    )

    end_dt = datetime.fromtimestamp((base + 4 * TF_MS) / 1000, tz=timezone.utc)
    await service.run(["BTC/EUR"], "5m", min_needed=3, warmup_extra=1, mode="A", end_time=end_dt)

    stored = buffer.get("BTC/EUR", "5m")
    assert [c["open_time"] for c in stored] == [base, base + TF_MS, missing, base + 3 * TF_MS]
    gap_key = tuple(sorted({"symbol": "BTC/EUR", "timeframe": "5m"}.items()))
    assert metrics.backfill_gaps_found_total.values[gap_key] == 1


@pytest.mark.asyncio
async def test_backfill_merges_live_candles_in_mode_b() -> None:
    metrics = DummyMetrics()
    buffer = BufferStub()
    ready: Dict[Tuple[str, str], bool] = {}
    base = (1_700_300_000_000 // TF_MS) * TF_MS
    start_event = asyncio.Event()
    release_event = asyncio.Event()

    async def fetch(symbol: str, timeframe: str, start: int, end: int, limit: int) -> List[Dict[str, Any]]:
        start_event.set()
        await release_event.wait()
        data = [_make_candle(base + i * TF_MS, float(i)) for i in range(5)]
        max_open = end - TF_MS
        return [c for c in data if start <= c["open_time"] <= max_open][:limit]

    service = BackfillService(
        fetch,
        buffer.append_many,
        buffer.get,
        lambda s, t, r: ready.__setitem__((s, t), r),
        None,
        metrics,
    )

    end_dt = datetime.fromtimestamp((base + 5 * TF_MS) / 1000, tz=timezone.utc)
    task = asyncio.create_task(
        service.run(["BTC/EUR"], "5m", min_needed=3, warmup_extra=1, mode="B", end_time=end_dt)
    )
    await start_event.wait()
    accepted, _ = service.handle_live_candle(
        "BTC/EUR",
        "5m",
        {
            "open_time": base + 5 * TF_MS,
            "open": 5.0,
            "high": 6.0,
            "low": 4.0,
            "close": 5.0,
            "volume": 10.0,
        },
    )
    assert not accepted
    release_event.set()
    await task

    stored = buffer.get("BTC/EUR", "5m")
    assert stored[-1]["open_time"] == base + 5 * TF_MS
    assert ready[("BTC/EUR", "5m")]


@pytest.mark.asyncio
async def test_backfill_records_error_metric_on_failure() -> None:
    metrics = DummyMetrics()
    buffer = BufferStub()
    ready: Dict[Tuple[str, str], bool] = {}

    async def fetch(_symbol: str, _timeframe: str, _start: int, _end: int, _limit: int) -> List[Dict[str, Any]]:
        raise RuntimeError("boom")

    service = BackfillService(
        fetch,
        buffer.append_many,
        buffer.get,
        lambda s, t, r: ready.__setitem__((s, t), r),
        None,
        metrics,
    )

    with pytest.raises(RuntimeError):
        await service.run(["BTC/EUR"], "5m", min_needed=1, warmup_extra=0, mode="A")

    key = tuple(sorted({"symbol": "BTC/EUR", "timeframe": "5m", "status": "error"}.items()))
    assert metrics.backfill_requests_total.values[key] == 1
