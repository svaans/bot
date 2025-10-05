"""Servicio de backfill asíncrono para precargar velas históricas."""
from __future__ import annotations

import asyncio
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

from core.utils.logger import configurar_logger
from core.utils.timeframes import TF_MINUTES, ensure_utc, floor_to_tf, tf_to_ms

FetchKlinesCallable = Callable[[str, str, int, int, int], Awaitable[List[Dict[str, Any]]]]
BufferAppendManyCallable = Callable[[str, str, List[Dict[str, Any]]], None]
BufferGetCallable = Callable[[str, str], List[Dict[str, Any]]]
SetReadyCallable = Callable[[str, str, bool], None]


class BackfillError(RuntimeError):
    """Error base del servicio de backfill."""


class _NullMetric:
    def labels(self, **_kwargs: Any) -> "_NullMetric":
        return self

    def inc(self, *_args: Any, **_kwargs: Any) -> None:  # pragma: no cover - trivial
        return None

    def set(self, *_args: Any, **_kwargs: Any) -> None:  # pragma: no cover - trivial
        return None

    def observe(self, *_args: Any, **_kwargs: Any) -> None:  # pragma: no cover - trivial
        return None


class BackfillService:
    """Gestiona la precarga de velas históricas antes del trading en vivo."""

    def __init__(
        self,
        fetch_klines: FetchKlinesCallable,
        buffer_append_many: BufferAppendManyCallable,
        buffer_get: BufferGetCallable,
        set_ready_flag: SetReadyCallable,
        logger: Any | None,
        metrics: Any | None,
        *,
        max_refetches: Optional[int] = None,
        page_limit: Optional[int] = None,
        concurrency: Optional[int] = None,
        headroom: Optional[int] = None,
    ) -> None:
        self.fetch_klines = fetch_klines
        self.buffer_append_many = buffer_append_many
        self.buffer_get = buffer_get
        self.set_ready_flag = set_ready_flag
        self.logger = logger or configurar_logger("backfill_service")

        self.max_refetches = int(
            max(0, max_refetches if max_refetches is not None else int(os.getenv("BACKFILL_MAX_REFETCHES", "3")))
        )
        self.page_limit = int(
            max(1, page_limit if page_limit is not None else int(os.getenv("BACKFILL_PAGE_LIMIT", "1000")))
        )
        self.concurrency = int(
            max(1, concurrency if concurrency is not None else int(os.getenv("BACKFILL_CONCURRENCY", "3")))
        )
        self.headroom = int(
            max(0, headroom if headroom is not None else int(os.getenv("BACKFILL_HEADROOM", "200")))
        )

        self._mode: str = "A"
        self._ready: Dict[Tuple[str, str], bool] = defaultdict(lambda: False)
        self._live_buffers: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        self._tf_cache: Dict[str, int] = {}

        self._metrics = {
            "requests": getattr(metrics, "backfill_requests_total", _NullMetric()),
            "fetched": getattr(metrics, "backfill_klines_fetched_total", _NullMetric()),
            "duration": getattr(metrics, "backfill_duration_seconds", _NullMetric()),
            "gaps": getattr(metrics, "backfill_gaps_found_total", _NullMetric()),
            "buffer": getattr(metrics, "buffer_size", _NullMetric()),
        }

    # ------------------------------------------------------------------ API pública
    async def run(
        self,
        symbols: Iterable[str],
        timeframe: str,
        min_needed: int,
        warmup_extra: int = 0,
        mode: str = "A",
        end_time: Optional[datetime] = None,
    ) -> None:
        """Ejecuta el backfill para los símbolos indicados."""

        symbols_list = [s.upper() for s in symbols if s]
        if not symbols_list:
            return
        if min_needed <= 0:
            raise ValueError("min_needed debe ser mayor a cero")

        need = int(min_needed + max(0, warmup_extra))
        if need <= 0:
            raise ValueError("need debe ser positivo")

        self._mode = mode.upper() if mode else "A"
        if self._mode not in {"A", "B"}:
            raise ValueError("mode debe ser 'A' o 'B'")

        tf_ms = self._get_tf_ms(timeframe)
        tf_minutes = TF_MINUTES[timeframe.lower()]

        now_dt = ensure_utc(end_time or datetime.now(timezone.utc))
        end_dt = floor_to_tf(now_dt, tf_minutes)
        end_ms = int(end_dt.timestamp() * 1000)
        start_ms = max(0, end_ms - need * tf_ms)

        await self._prepare_flags(symbols_list, timeframe)

        sem = asyncio.Semaphore(self.concurrency)
        tasks = [
            asyncio.create_task(
                self._run_symbol_with_lock(sem, symbol, timeframe, min_needed, need, start_ms, end_ms, tf_ms)
            )
            for symbol in symbols_list
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            raise errors[0]

    def handle_live_candle(self, symbol: str, timeframe: str, candle: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Gestiona una vela en vivo durante el modo B.

        Devuelve ``(True, vela_normalizada)`` cuando debe procesarse, ``(False, None)``
        si debe retenerse hasta que el histórico esté listo.
        """

        sym = symbol.upper()
        tf = timeframe.lower()
        key = self._key(sym, tf)
        if self._mode != "B":
            return True, self._normalize_live(sym, tf, candle)

        if self._ready.get(key, False):
            return True, self._normalize_live(sym, tf, candle)

        normalized = self._normalize_live(sym, tf, candle)
        if normalized is not None:
            self._live_buffers[key].append(normalized)
        return False, None

    def is_ready(self, symbol: str, timeframe: str) -> bool:
        key = self._key(symbol, timeframe)
        return bool(self._ready.get(key, False))

    # ----------------------------------------------------------------- Internos
    async def _run_symbol_with_lock(
        self,
        semaphore: asyncio.Semaphore,
        symbol: str,
        timeframe: str,
        min_needed: int,
        need: int,
        start_ms: int,
        end_ms: int,
        tf_ms: int,
    ) -> None:
        async with semaphore:
            await self._run_symbol(symbol, timeframe, min_needed, need, start_ms, end_ms, tf_ms)

    async def _run_symbol(
        self,
        symbol: str,
        timeframe: str,
        min_needed: int,
        need: int,
        start_ms: int,
        end_ms: int,
        tf_ms: int,
    ) -> None:
        key = self._key(symbol, timeframe)
        status = "ok"
        start_time = time.perf_counter()
        self._log_info("start", symbol, timeframe, need=need, start_ms=start_ms, end_ms=end_ms)
        try:
            klines = await self._collect_history(symbol, timeframe, need, start_ms, end_ms, tf_ms)
            if not klines:
                raise BackfillError(f"No se pudieron recuperar velas para {symbol}")

            if self._mode == "B":
                live = self._drain_live_queue(key)
                if live:
                    klines = self._merge_klines(klines, live)
                    self._log_info("merge_done", symbol, timeframe, buffer_len=len(klines))

            limit = need + self.headroom
            trimmed = klines[-limit:]

            existing = self.buffer_get(symbol, timeframe) or []
            existing_keys = {
                ts for ts in (self._extract_open_time(c) for c in existing) if ts is not None
            }
            to_append = [c for c in trimmed if c["open_time"] not in existing_keys]
            if to_append:
                self.buffer_append_many(symbol, timeframe, to_append)

            buffer_len = len(self.buffer_get(symbol, timeframe) or [])
            self._update_buffer_metric(symbol, timeframe, buffer_len)

            ready = buffer_len >= min_needed
            self.set_ready_flag(symbol, timeframe, ready)
            self._ready[key] = ready
            if ready:
                self._log_info("ready", symbol, timeframe, buffer_len=buffer_len, min_needed=min_needed)
        except Exception as exc:
            status = "error"
            self._log_error("error", symbol, timeframe, need=need, details={"error": str(exc)})
            raise
        finally:
            elapsed = time.perf_counter() - start_time
            self._metric("duration", symbol, timeframe).observe(elapsed)
            self._metric("requests", symbol, timeframe, status=status).inc()

    async def _collect_history(
        self,
        symbol: str,
        timeframe: str,
        need: int,
        start_ms: int,
        end_ms: int,
        tf_ms: int,
    ) -> List[Dict[str, Any]]:
        cursor = start_ms
        collected: List[Dict[str, Any]] = []
        seen: set[int] = set()
        max_open = end_ms - tf_ms
        attempts_without_progress = 0

        while len(collected) < need and cursor <= max_open:
            limit = min(self.page_limit, need - len(collected))
            batch = await self.fetch_klines(symbol, timeframe, cursor, end_ms, limit)
            if not isinstance(batch, list):
                batch = []
            self._metric("fetched", symbol, timeframe).inc(len(batch))

            normalized = []
            for raw in batch:
                try:
                    candle = self._normalize(symbol, timeframe, raw)
                except BackfillError:
                    continue
                if candle["open_time"] < cursor:
                    continue
                if candle["open_time"] > max_open:
                    continue
                normalized.append(candle)

            if not normalized:
                attempts_without_progress += 1
                if attempts_without_progress >= 2:
                    cursor += tf_ms
                else:
                    await asyncio.sleep(0)
                continue

            attempts_without_progress = 0
            normalized = self._sort_and_dedupe(normalized)
            for candle in normalized:
                if candle["open_time"] in seen:
                    continue
                collected.append(candle)
                seen.add(candle["open_time"])
            cursor = normalized[-1]["open_time"] + tf_ms
            self._log_debug(
                "page_fetched",
                symbol,
                timeframe,
                fetched=len(normalized),
                cursor_start=cursor - tf_ms,
                cursor_next=cursor,
            )

        collected = self._sort_and_dedupe(collected)
        collected = await self._ensure_continuity(symbol, timeframe, collected, tf_ms, end_ms)
        return collected

    async def _ensure_continuity(
        self,
        symbol: str,
        timeframe: str,
        klines: List[Dict[str, Any]],
        tf_ms: int,
        end_ms: int,
    ) -> List[Dict[str, Any]]:
        attempts = 0
        result = list(klines)
        while attempts <= self.max_refetches:
            gaps = self._detect_gaps(result, tf_ms)
            if not gaps:
                break
            attempts += 1
            self._metric("gaps", symbol, timeframe).inc()
            self._log_warning("gap_detected", symbol, timeframe, gap_count=len(gaps))
            start = gaps[0]
            end = gaps[-1] + tf_ms
            fetched = await self.fetch_klines(symbol, timeframe, start, end, min(self.page_limit, len(gaps) + 1))
            normalized = [self._normalize(symbol, timeframe, raw) for raw in fetched]
            filtered = [c for c in normalized if start <= c["open_time"] <= end - tf_ms]
            result = self._merge_klines(result, filtered)
        else:
            raise BackfillError(f"Persisten gaps en histórico de {symbol}")

        for candle in result:
            if candle["open_time"] % tf_ms != 0:
                raise BackfillError(f"Vela desalineada en {symbol}: {candle['open_time']}")
            if candle["open_time"] > end_ms:
                raise BackfillError("Backfill retornó velas fuera del rango solicitado")
        return result

    def _detect_gaps(self, klines: List[Dict[str, Any]], tf_ms: int) -> List[int]:
        gaps: List[int] = []
        if len(klines) < 2:
            return gaps
        previous = klines[0]["open_time"]
        for candle in klines[1:]:
            current = candle["open_time"]
            delta = current - previous
            if delta == tf_ms:
                previous = current
                continue
            if delta < tf_ms:
                raise BackfillError("Velas fuera de orden cronológico")
            pointer = previous + tf_ms
            while pointer < current:
                gaps.append(pointer)
                pointer += tf_ms
            previous = current
        return gaps

    def _merge_klines(self, base: Iterable[Dict[str, Any]], extra: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: Dict[int, Dict[str, Any]] = {}
        for candle in base:
            merged[candle["open_time"]] = candle
        for candle in extra:
            current = merged.get(candle["open_time"])
            if current is None or self._prefer_new(candle, current):
                merged[candle["open_time"]] = candle
        return sorted(merged.values(), key=lambda c: c["open_time"])

    def _prefer_new(self, candidate: Dict[str, Any], current: Dict[str, Any]) -> bool:
        return self._is_complete(candidate) and not self._is_complete(current)

    def _is_complete(self, candle: Dict[str, Any]) -> bool:
        for field in ("open", "high", "low", "close", "volume", "close_time"):
            value = candle.get(field)
            if value is None:
                return False
        return True

    def _normalize(self, symbol: str, timeframe: str, raw: Dict[str, Any]) -> Dict[str, Any]:
        tf_ms = self._get_tf_ms(timeframe)
        data = dict(raw)
        open_time = self._extract_open_time(data)
        if open_time is None:
            raise BackfillError("open_time faltante")
        open_time = int(open_time)
        candle = {
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": open_time,
            "timestamp": open_time,
            "open": float(data.get("open", data.get("o", 0.0))),
            "high": float(data.get("high", data.get("h", 0.0))),
            "low": float(data.get("low", data.get("l", 0.0))),
            "close": float(data.get("close", data.get("c", 0.0))),
            "volume": float(data.get("volume", data.get("v", 0.0))),
            "close_time": int(data.get("close_time", data.get("closeTime", open_time + tf_ms - 1))),
        }
        return candle

    def _normalize_live(
        self,
        symbol: str,
        timeframe: str,
        candle: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        try:
            return self._normalize(symbol, timeframe, candle)
        except BackfillError:
            return None

    def _extract_open_time(self, candle: Dict[str, Any]) -> Optional[int]:
        for key in ("open_time", "openTime", "timestamp", "time"):
            value = candle.get(key)
            if value is None:
                continue
            try:
                return int(float(value))
            except (TypeError, ValueError):
                continue
        return None

    def _sort_and_dedupe(self, candles: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: Dict[int, Dict[str, Any]] = {}
        for candle in candles:
            existing = merged.get(candle["open_time"])
            if existing is None or self._prefer_new(candle, existing):
                merged[candle["open_time"]] = candle
        return sorted(merged.values(), key=lambda c: c["open_time"])

    def _drain_live_queue(self, key: Tuple[str, str]) -> List[Dict[str, Any]]:
        if key not in self._live_buffers:
            return []
        items = self._live_buffers.pop(key)
        return items

    async def _prepare_flags(self, symbols: List[str], timeframe: str) -> None:
        for symbol in symbols:
            key = self._key(symbol, timeframe)
            self._ready[key] = False
            try:
                self.set_ready_flag(symbol, timeframe, False)
            except Exception:
                pass

    def _metric(self, name: str, symbol: str, timeframe: str, **labels: Any) -> Any:
        metric = self._metrics.get(name)
        if metric is None:
            return _NullMetric()
        payload = {"symbol": symbol, "timeframe": timeframe}
        payload.update(labels)
        try:
            return metric.labels(**payload)
        except TypeError:
            label_names = getattr(metric, "labelnames", ())
            if not label_names:
                return metric
            values = [payload.get(label, "") for label in label_names]
            try:
                return metric.labels(*values)
            except Exception:
                return _NullMetric()

    def _update_buffer_metric(self, symbol: str, timeframe: str, size: int) -> None:
        try:
            self._metric("buffer", symbol, timeframe).set(float(size))
        except Exception:
            pass

    def _get_tf_ms(self, timeframe: str) -> int:
        tf = timeframe.lower()
        if tf in self._tf_cache:
            return self._tf_cache[tf]
        value = tf_to_ms(tf)
        self._tf_cache[tf] = value
        return value

    def _key(self, symbol: str, timeframe: str) -> Tuple[str, str]:
        return symbol.upper(), timeframe.lower()

    def _log_info(self, event: str, symbol: str, timeframe: str, **extra: Any) -> None:
        self.logger.info(event, extra=self._log_payload(event, symbol, timeframe, extra))

    def _log_warning(self, event: str, symbol: str, timeframe: str, **extra: Any) -> None:
        self.logger.warning(event, extra=self._log_payload(event, symbol, timeframe, extra))

    def _log_error(self, event: str, symbol: str, timeframe: str, **extra: Any) -> None:
        self.logger.error(event, extra=self._log_payload(event, symbol, timeframe, extra))

    def _log_debug(self, event: str, symbol: str, timeframe: str, **extra: Any) -> None:
        self.logger.debug(event, extra=self._log_payload(event, symbol, timeframe, extra))

    def _log_payload(self, event: str, symbol: str, timeframe: str, extra: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "event": event,
            "symbol": symbol,
            "timeframe": timeframe,
        }
        payload.update(extra)
        return payload