from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from core.backfill_service import BackfillService
from core.metrics import (
    BACKFILL_DURATION_SECONDS,
    BACKFILL_GAPS_FOUND_TOTAL,
    BACKFILL_KLINES_FETCHED_TOTAL,
    BACKFILL_REQUESTS_TOTAL,
    BUFFER_SIZE_V2,
)
from core.trader.trader_lite import TraderLite

TF_MS = 5 * 60 * 1000


class DummyConfig(SimpleNamespace):
    symbols: list[str]
    intervalo_velas: str
    modo_real: bool


@pytest.mark.asyncio
async def test_traderlite_backfill_mode_b_blocks_until_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DummyConfig(symbols=["BTC/EUR"], intervalo_velas="5m", modo_real=False)
    processed: list[int] = []

    async def handler(candle: dict) -> None:
        processed.append(int(candle["open_time"]))

    trader = TraderLite(config, candle_handler=handler)
    manager = trader._get_buffer_manager()
    manager._estados.clear()  # type: ignore[attr-defined]
    manager._locks.clear()  # type: ignore[attr-defined]
    trader._backfill_mode = "B"
    trader._backfill_enabled = True

    base = (1_700_400_000_000 // TF_MS) * TF_MS
    start_event = asyncio.Event()
    release_event = asyncio.Event()
    first_start: dict[str, int | None] = {"value": None}

    async def fetch(symbol: str, timeframe: str, start: int, end: int, limit: int) -> list[dict]:
        start_event.set()
        await release_event.wait()
        if first_start["value"] is None:
            first_start["value"] = start
        data = []
        max_open = end - TF_MS
        current = start
        idx = 0
        while len(data) < limit and current <= max_open:
            data.append(
                {
                    "open_time": current,
                    "open": float(idx),
                    "high": float(idx) + 1,
                    "low": float(idx) - 1,
                    "close": float(idx),
                    "volume": 10.0,
                }
            )
            current += TF_MS
            idx += 1
        return data[:limit]

    metrics = SimpleNamespace(
        backfill_requests_total=BACKFILL_REQUESTS_TOTAL,
        backfill_klines_fetched_total=BACKFILL_KLINES_FETCHED_TOTAL,
        backfill_duration_seconds=BACKFILL_DURATION_SECONDS,
        backfill_gaps_found_total=BACKFILL_GAPS_FOUND_TOTAL,
        buffer_size_v2=BUFFER_SIZE_V2,
    )

    trader._backfill_min_needed = 3
    trader._backfill_warmup_extra = 1
    trader._backfill_service = BackfillService(
        fetch,
        trader._buffer_append_many,
        trader._buffer_get,
        trader._backfill_set_ready,
        None,
        metrics,
    )

    await trader.start_backfill()
    assert trader._backfill_task is not None
    await start_event.wait()

    start_value = first_start["value"] or base
    live_candle = {
        "symbol": "BTC/EUR",
        "open_time": start_value + 4 * TF_MS,
        "open": 4.0,
        "high": 5.0,
        "low": 3.0,
        "close": 4.5,
        "volume": 12.0,
        "timeframe": "5m",
    }
    assert trader._update_estado_con_candle(dict(live_candle)) is False
    assert not trader.is_symbol_ready("BTC/EUR", "5m")

    release_event.set()
    await trader._backfill_task
    assert trader.is_symbol_ready("BTC/EUR", "5m")

    assert trader._update_estado_con_candle(dict(live_candle)) is True
    snapshot = trader._buffer_get("BTC/EUR", "5m")
    assert snapshot
    estado_symbol = trader.estado["BTC/EUR"]
    assert estado_symbol.buffer[-1]["timestamp"] == live_candle["open_time"]
    assert processed == []  # handler no llamado aún; pipeline lo invoca aparte