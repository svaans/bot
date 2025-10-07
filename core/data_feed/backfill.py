from __future__ import annotations

"""Rutinas de backfill para el ``DataFeed``."""

import asyncio
from datetime import datetime
from typing import List

from ._shared import UTC, _safe_float, log
from . import events


async def do_backfill(feed: "DataFeed", symbol: str) -> None:
    """Obtiene velas históricas faltantes antes de iniciar el stream."""

    if not feed._cliente:
        return

    from . import fetch_ohlcv_async as _fetch_ohlcv_async, validar_integridad_velas as _validar_integridad_velas

    intervalo_ms = feed.intervalo_segundos * 1000
    ahora = int(datetime.now(UTC).timestamp() * 1000)
    last_ts = feed._last_close_ts.get(symbol)
    cached = feed._last_backfill_ts.get(symbol)

    if last_ts is not None and cached is not None and last_ts <= cached:
        return

    if last_ts is None:
        faltan = feed.min_buffer_candles
        since = ahora - intervalo_ms * faltan
    else:
        faltan = max(0, (ahora - last_ts) // intervalo_ms)
        if faltan <= 0:
            feed._last_backfill_ts[symbol] = last_ts
            return
        since = last_ts + 1

    restante_total = min(max(faltan, feed.min_buffer_candles), feed._backfill_max)
    ohlcv: List[List[float]] = []

    while restante_total > 0:
        limit = min(100, restante_total)
        try:
            chunk = await _fetch_ohlcv_async(feed._cliente, symbol, feed.intervalo, since=since, limit=limit)
        except Exception as exc:
            log.exception("%s: error backfill (%s)", symbol, exc)
            events.emit_event(feed, "backfill_error", {"symbol": symbol, "error": str(exc)})
            return

        if not chunk:
            break

        ohlcv.extend(chunk)
        restante_total -= len(chunk)
        since = int(chunk[-1][0]) + 1
        if len(chunk) < limit:
            break

        await asyncio.sleep(0)

    if not ohlcv:
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
        events.emit_event(feed, "backfill_invalid", {"symbol": symbol})

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

    events.emit_event(feed, "backfill_ok", {"symbol": symbol, "candles": len(ohlcv)})


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .datafeed import DataFeed