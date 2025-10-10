from __future__ import annotations

"""Rutinas de backfill para el ``DataFeed``."""

import asyncio
import time
from datetime import datetime
from typing import Dict, List

from core.metrics import (
    BACKFILL_DURATION_SECONDS,
    BACKFILL_KLINES_FETCHED_TOTAL,
    BACKFILL_REQUESTS_TOTAL,
)

from ._shared import UTC, _safe_float, log
from . import events


def _observe_backfill_duration(symbol: str, timeframe: str, elapsed: float) -> None:
    try:
        BACKFILL_DURATION_SECONDS.labels(symbol, timeframe).observe(elapsed)
    except Exception:
        pass


def _inc_backfill_counter(symbol: str, timeframe: str, candles: int) -> None:
    try:
        BACKFILL_KLINES_FETCHED_TOTAL.labels(symbol, timeframe).inc(candles)
    except Exception:
        pass


def _note_backfill_status(symbol: str, timeframe: str, status: str) -> None:
    try:
        BACKFILL_REQUESTS_TOTAL.labels(symbol, timeframe, status).inc()
    except Exception:
        pass


def _emit(feed: "DataFeed", event: str, payload: Dict[str, object]) -> None:
    events.emit_event(feed, event, payload)
    events.emit_bus_signal(feed, event, payload)


async def do_backfill(feed: "DataFeed", symbol: str) -> None:
    """Obtiene velas históricas faltantes antes de iniciar el stream."""

    timeframe = feed.intervalo
    start = time.perf_counter()
    base_payload: Dict[str, object] = {"symbol": symbol, "timeframe": timeframe}
    
    if not feed._cliente:
        _note_backfill_status(symbol, timeframe, "no_client")
        return

    from . import fetch_ohlcv_async as _fetch_ohlcv_async, validar_integridad_velas as _validar_integridad_velas

    intervalo_ms = feed.intervalo_segundos * 1000
    ahora = int(datetime.now(UTC).timestamp() * 1000)
    last_ts = feed._last_close_ts.get(symbol)
    cached = feed._last_backfill_ts.get(symbol)

    if last_ts is not None and cached is not None and last_ts <= cached:
        _note_backfill_status(symbol, timeframe, "cached")
        return

    if last_ts is None:
        faltan = feed.min_buffer_candles
        since = ahora - intervalo_ms * faltan
    else:
        faltan = max(0, (ahora - last_ts) // intervalo_ms)
        if faltan <= 0:
            feed._last_backfill_ts[symbol] = last_ts
            _note_backfill_status(symbol, timeframe, "up_to_date")
            return
        since = last_ts + 1

    restante_total = min(max(faltan, feed.min_buffer_candles), feed._backfill_max)
    ohlcv: List[List[float]] = []

    _note_backfill_status(symbol, timeframe, "started")
    _emit(feed, "backfill_started", {**base_payload, "remaining": restante_total})
    

    while restante_total > 0:
        limit = min(100, restante_total)
        try:
            chunk = await _fetch_ohlcv_async(feed._cliente, symbol, feed.intervalo, since=since, limit=limit)
        except Exception as exc:
            log.exception("%s: error backfill (%s)", symbol, exc)
            _note_backfill_status(symbol, timeframe, "error")
            _emit(feed, "backfill_error", {**base_payload, "error": str(exc)})
            return

        if not chunk:
            break

        ohlcv.extend(chunk)
        restante_total -= len(chunk)
        since = int(chunk[-1][0]) + 1
        _inc_backfill_counter(symbol, timeframe, len(chunk))
        if len(chunk) < limit:
            break

        await asyncio.sleep(0)

    if not ohlcv:
        _note_backfill_status(symbol, timeframe, "empty")
        _emit(feed, "backfill_empty", base_payload)
        return

    feed._last_backfill_ts[symbol] = int(ohlcv[-1][0])

    ok = _validar_integridad_velas(
        symbol,
        feed.intervalo,
        (
            {
                "timestamp": int(o[0]),
                "open": o[1],
                "high": o[2],
                "low": o[3],
                "close": o[4],
                "volume": o[5],
            }
            for o in ohlcv
        ),
        log,
    )
    if not ok:
        _emit(feed, "backfill_invalid", base_payload))

    for o in ohlcv:
        candle = {
            "symbol": symbol,
            "timestamp": int(o[0]),
            "open": _safe_float(o[1], 0.0),
            "high": _safe_float(o[2], 0.0),
            "low": _safe_float(o[3], 0.0),
            "close": _safe_float(o[4], 0.0),
            "volume": _safe_float(o[5], 0.0),
            "is_closed": True,
        }
        await feed._handle_candle(symbol, candle)

    elapsed = max(0.0, time.perf_counter() - start)
    _observe_backfill_duration(symbol, timeframe, elapsed)
    _note_backfill_status(symbol, timeframe, "success")
    _emit(
        feed,
        "backfill_ok",
        {
            **base_payload,
            "candles": len(ohlcv),
            "elapsed": elapsed,
        },
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .datafeed import DataFeed
