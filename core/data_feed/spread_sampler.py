"""Herramientas para estimar spreads en ``DataFeed``."""

from __future__ import annotations

import asyncio
import math
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Mapping

from core.utils.log_utils import safe_extra

from ._shared import _safe_float, log


@dataclass(slots=True)
class SpreadSample:
    """Representa una observación de spread bid/ask."""

    bid: float
    ask: float
    ratio: float
    timestamp_ms: int | None
    source: str | None = None


FetchCallable = Callable[[Any | None, str], Awaitable[Mapping[str, Any] | None]]


class SpreadSampler:
    """Gestiona el muestreo y cacheo de spreads por símbolo."""

    def __init__(self, ttl: float = 2.5) -> None:
        self._ttl = max(0.25, float(ttl))
        self._cache: Dict[str, tuple[float, SpreadSample]] = {}
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def sample(
        self,
        symbol: str,
        fetcher: FetchCallable,
        *,
        cliente: Any | None,
    ) -> SpreadSample | None:
        """Obtiene un ``SpreadSample`` cacheado o consultando al ``fetcher``."""

        sym = str(symbol).upper()
        now = time.monotonic()
        cached = self._cache.get(sym)
        if cached is not None and now - cached[0] < self._ttl:
            return cached[1]

        lock = self._locks[sym]
        async with lock:
            cached = self._cache.get(sym)
            if cached is not None and time.monotonic() - cached[0] < self._ttl:
                return cached[1]

            try:
                raw = await fetcher(cliente, sym)
            except Exception:
                log.exception("spread.fetch_failed", extra=safe_extra({"symbol": sym}))
                return None

            sample = self._parse_raw(sym, raw)
            if sample is None:
                return None

            self._cache[sym] = (time.monotonic(), sample)
            return sample

    @staticmethod
    def _parse_raw(symbol: str, raw: Mapping[str, Any] | None) -> SpreadSample | None:
        if not isinstance(raw, Mapping):
            return None

        bid = _safe_float(raw.get("bid") or raw.get("bidPrice"), math.nan)
        ask = _safe_float(raw.get("ask") or raw.get("askPrice"), math.nan)

        if not math.isfinite(bid) or not math.isfinite(ask):
            return None

        bid = float(bid)
        ask = float(ask)
        if bid < 0 or ask < 0:
            return None
        if ask < bid:
            ask = bid

        mid = (ask + bid) / 2 if ask + bid != 0 else max(ask, bid, 1e-9)
        spread = max(ask - bid, 0.0)
        ratio = 0.0 if mid <= 0 else spread / max(mid, 1e-12)

        timestamp = raw.get("timestamp") or raw.get("time") or raw.get("updateTime")
        try:
            ts_int = int(float(timestamp)) if timestamp is not None else None
        except (TypeError, ValueError):
            ts_int = None

        source = raw.get("source")
        if source is not None:
            source = str(source)

        log.debug(
            "spread.sample", extra=safe_extra({"symbol": symbol, "ratio": ratio, "spread": spread})
        )

        return SpreadSample(bid=bid, ask=ask, ratio=ratio, timestamp_ms=ts_int, source=source)


__all__ = ["SpreadSample", "SpreadSampler"]
