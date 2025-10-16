"""Funciones relacionadas con el procesamiento de velas."""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any, Awaitable, Callable, Mapping

from core.utils.feature_flags import is_flag_enabled
from core.utils.utils import timestamp_alineado, validar_integridad_velas
from core.utils.log_utils import safe_extra
from core.adaptador_dinamico import backfill_ventana
from ._shared import COMBINED_STREAM_KEY, ConsumerState, log
from . import events

try:  # pragma: no cover - métricas opcionales
    from core.metrics import (
        CONSUMER_SKIPPED_EXPECTED_TOTAL,
        DATAFEED_CANDLES_ENQUEUED_TOTAL,
        DATAFEED_WS_MESSAGES_TOTAL,
        registrar_vela_recibida,
        registrar_vela_rechazada,
    )
except Exception:  # pragma: no cover - fallback cuando Prometheus no está disponible
    from core.utils.metrics_compat import Counter

    CONSUMER_SKIPPED_EXPECTED_TOTAL = Counter(
        "consumer_skipped_expected_total",
        "Skips esperados del consumer de DataFeed",
        ["symbol", "timeframe", "reason"],
    )
    DATAFEED_WS_MESSAGES_TOTAL = Counter(
        "datafeed_ws_messages_total",
        "Mensajes recibidos por el DataFeed desde WS",
        ["type"],
    )
    DATAFEED_CANDLES_ENQUEUED_TOTAL = Counter(
        "datafeed_candles_enqueued_total",
        "Velas encoladas por símbolo y timeframe",
        ["symbol", "tf"],
    )

    def registrar_vela_recibida(*_args: Any, **_kwargs: Any) -> None:
        return None

    def registrar_vela_rechazada(*_args: Any, **_kwargs: Any) -> None:
        return None


_PATCH_LOGGED = False


def _metrics_extended_enabled() -> bool:
    return is_flag_enabled("metrics.extended.enabled")


def _debug_wrapper_enabled() -> bool:
    return is_flag_enabled("datafeed.debug_wrapper.enabled")


def _ensure_patch_logged() -> None:
    """Emite un log informativo único para verificar el parche en ejecución."""

    global _PATCH_LOGGED
    if not _PATCH_LOGGED:
        log.info(
            "consumer.patch.active",
            extra=safe_extra({"stage": "DataFeed._consumer"}),
        )
        _PATCH_LOGGED = True


def _to_int(value: Any) -> int | None:
    """Convierte ``value`` a ``int`` si es posible."""

    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _backfill_window_enabled(feed: "DataFeed") -> bool:
    if getattr(feed, "_backfill_ventana_enabled", False):
        return True
    return is_flag_enabled("backfill.ventana.enabled")


async def _maybe_run_backfill_window(
    feed: "DataFeed",
    symbol: str,
    *,
    last_ts: int | None,
    new_ts: int | None,
    intervalo_ms: int,
) -> None:
    if last_ts is None or new_ts is None or intervalo_ms <= 0:
        return

    gap_ms = new_ts - last_ts
    if gap_ms <= intervalo_ms:
        return

    if not _backfill_window_enabled(feed):
        return

    cliente = getattr(feed, "_cliente", None)
    if cliente is None:
        return

    max_total = getattr(feed, "_backfill_window_target", None)
    try:
        max_total_int = int(max_total) if max_total is not None else feed.min_buffer_candles
    except (TypeError, ValueError):
        max_total_int = feed.min_buffer_candles
    max_total_int = max(0, max_total_int)
    max_total_int = min(max_total_int, getattr(feed, "_backfill_max", max_total_int))
    if max_total_int <= 0:
        return

    missing_intervals = int(gap_ms // intervalo_ms) - 1
    if missing_intervals <= 0:
        return

    allowed = min(missing_intervals, max_total_int)
    total_fetched = 0
    start_ts = last_ts + intervalo_ms

    while total_fetched < allowed and missing_intervals > 0:
        remaining = min(allowed - total_fetched, missing_intervals)
        extras = await backfill_ventana(
            symbol,
            intervalo=feed.intervalo,
            cliente=cliente,
            start_ts=start_ts,
            candles=remaining,
            current_ts=new_ts,
            max_candles=remaining,
        )
        if not extras:
            break

        extras_sorted = sorted(extras, key=lambda c: c.get("timestamp", 0))
        for extra in extras_sorted:
            extra.setdefault("symbol", symbol)
            extra.setdefault("timeframe", feed.intervalo)
            extra.setdefault("interval", feed.intervalo)
            extra.setdefault("tf", feed.intervalo)
            extra["_df_backfill_window"] = True
            await handle_candle(feed, symbol, extra, _from_backfill=True)
            total_fetched += 1
            ts_extra = extra.get("timestamp")
            if isinstance(ts_extra, int):
                start_ts = ts_extra + intervalo_ms

        last_processed = feed._last_close_ts.get(symbol, last_ts)
        missing_intervals = int((new_ts - last_processed) // intervalo_ms) - 1
        if len(extras_sorted) < remaining:
            break


def _to_float(value: Any) -> float | None:
    """Convierte ``value`` a ``float`` si es posible."""

    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_candle_payload(candle: Mapping[str, Any]) -> dict[str, Any]:
    """Normaliza cargas provenientes de Binance antes de procesarlas."""

    normalized: dict[str, Any] = dict(candle)

    # Aceptamos alias comunes para ``is_closed`` y ``symbol``.
    if "is_closed" not in normalized:
        if "isClosed" in normalized:
            normalized["is_closed"] = bool(normalized["isClosed"])
        elif "closed" in normalized and isinstance(normalized["closed"], bool):
            normalized["is_closed"] = bool(normalized["closed"])
        elif "x" in normalized:
            normalized["is_closed"] = bool(normalized["x"])

    symbol = normalized.get("symbol") or normalized.get("s") or normalized.get("S")
    if symbol is not None:
        normalized["symbol"] = str(symbol).upper()

    # Desempaquetamos mensajes ``kline`` típicos de Binance.
    kline: Mapping[str, Any] | None = None
    for key in ("k", "kline"):
        raw = normalized.get(key)
        if isinstance(raw, Mapping):
            kline = raw
            break

    if kline:
        normalized.setdefault("symbol", str(kline.get("s") or symbol or "").upper())
        intervalo = kline.get("i")
        if intervalo:
            normalized.setdefault("intervalo", intervalo)
            normalized.setdefault("interval", intervalo)
            normalized.setdefault("tf", intervalo)

        open_time = _to_int(kline.get("t") or kline.get("open_time"))
        close_time = _to_int(kline.get("T") or kline.get("close_time"))
        if open_time is not None and "open_time" not in normalized:
            normalized["open_time"] = open_time
        if close_time is not None and "close_time" not in normalized:
            normalized["close_time"] = close_time

        event_time_candidates = (
            normalized.get("event_time"),
            normalized.get("eventTime"),
            normalized.get("E"),
            kline.get("E") if kline else None,
            kline.get("event_time") if kline else None,
            close_time,
        )
        for candidate in event_time_candidates:
            parsed = _to_int(candidate)
            if parsed is not None:
                normalized["event_time"] = parsed
                break

        for target, source in (
            ("open", "o"),
            ("high", "h"),
            ("low", "l"),
            ("close", "c"),
            ("volume", "v"),
        ):
            if target not in normalized:
                value = _to_float(kline.get(source))
                if value is not None:
                    normalized[target] = value

        if "is_closed" not in normalized:
            normalized["is_closed"] = bool(kline.get("x"))

        if open_time is not None:
            normalized["timestamp"] = open_time
        elif "timestamp" not in normalized and close_time is not None:
            normalized["timestamp"] = close_time

    else:
        if "timestamp" not in normalized:
            candidate_ts = (
                normalized.get("close_time")
                or normalized.get("open_time")
                or normalized.get("event_time")
            )
            parsed = _to_int(candidate_ts)
            if parsed is not None:
                normalized["timestamp"] = parsed

    return normalized



async def handle_candle(
    feed: "DataFeed", symbol: str, candle: dict, *, _from_backfill: bool = False
) -> None:
    """Normaliza y encola una vela recibida desde el stream."""

    candle = _normalize_candle_payload(candle)

    metrics_enabled = _metrics_extended_enabled()
    timeframe_label = str(feed.intervalo or "unknown")
    symbol_label = str(symbol).upper()

    if not candle.get("is_closed", False):
        if metrics_enabled:
            registrar_vela_rechazada(symbol_label, "not_closed", timeframe_label)
        return
    ts = candle.get("timestamp")
    if ts is None:
        for key in (
            "close_time",
            "closeTime",
            "open_time",
            "openTime",
            "event_time",
            "eventTime",
        ):
            candidato = candle.get(key)
            if candidato is not None:
                ts = candidato
                break

    try:
        ts = int(float(ts)) if ts is not None else None
    except (TypeError, ValueError):
        ts = None
    if ts is None:
        if metrics_enabled:
            registrar_vela_rechazada(symbol_label, "timestamp_invalid", timeframe_label)
        return
    if not timestamp_alineado(ts, feed.intervalo):
        if metrics_enabled:
            registrar_vela_rechazada(symbol_label, "timestamp_unaligned", timeframe_label)
        return

    candle["timestamp"] = ts
    candle.setdefault("timeframe", feed.intervalo)
    candle.setdefault("interval", feed.intervalo)
    candle.setdefault("tf", feed.intervalo)
    last = feed._last_close_ts.get(symbol)

    if not _from_backfill:
        intervalo_ms = int(getattr(feed, "intervalo_segundos", 0) * 1000)
        await _maybe_run_backfill_window(
            feed,
            symbol,
            last_ts=last,
            new_ts=ts,
            intervalo_ms=intervalo_ms,
        )
        last = feed._last_close_ts.get(symbol)
    if last is not None and ts <= last:
        if metrics_enabled:
            registrar_vela_rechazada(symbol_label, "stale", timeframe_label)
        return

    events.reset_reconnect_tracking(feed, symbol)
    if feed._combined:
        events.reset_reconnect_tracking(feed, COMBINED_STREAM_KEY)

    if not validar_integridad_velas(symbol, feed.intervalo, [candle], log):
        events.emit_event(feed, "candle_invalid", {"symbol": symbol, "ts": ts})
        if metrics_enabled:
            registrar_vela_rechazada(symbol_label, "integrity_failed", timeframe_label)
        return

    if not feed.ws_connected_event.is_set():
        events.signal_ws_connected(feed, symbol)

    queue = feed._queues.get(symbol)
    if queue is None:
        if metrics_enabled:
            registrar_vela_rechazada(symbol_label, "queue_missing", timeframe_label)
        return

    candle.setdefault("_df_enqueue_time", time.monotonic())

    tf_label = str(feed.intervalo).lower()
    DATAFEED_WS_MESSAGES_TOTAL.labels(type=f"kline_{tf_label}").inc()

    log.debug(
        "recv candle",
        extra=safe_extra({"symbol": symbol, "timestamp": ts, "queue_size": queue.qsize()}),
    )

    bar_open_ts = _to_int(candle.get("open_time")) or _to_int(candle.get("openTime"))
    bar_close_ts = _to_int(candle.get("close_time")) or _to_int(candle.get("closeTime"))

    queue_size_after: int

    if feed.queue_policy == "block" or not queue.maxsize:
        await queue.put(candle)
        queue_size_after = queue.qsize()
    elif feed.queue_policy == "drop_newest":
        if queue.full():
            feed._stats[symbol]["dropped"] += 1
            events.emit_event(feed, "queue_drop", {"symbol": symbol, "policy": "drop_newest"})
            queue_size_after = queue.qsize()
            log.debug(
                "queue.drop_newest",
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "tf": feed.intervalo,
                        "queue_size": queue_size_after,
                        "maxsize": queue.maxsize,
                    }
                ),
            )
            return
        queue.put_nowait(candle)
        queue_size_after = queue.qsize()
    else:
        while True:
            try:
                queue.put_nowait(candle)
                queue_size_after = queue.qsize()
                break
            except asyncio.QueueFull:
                try:
                    _ = queue.get_nowait()
                    queue.task_done()
                    feed._stats[symbol]["dropped"] += 1
                    events.emit_event(feed, "queue_drop", {"symbol": symbol})
                    current_size = queue.qsize()
                    if (
                        not getattr(feed, "_queue_capacity_breach_logged", False)
                        and feed.queue_max
                        and getattr(feed, "queue_min_recommended", 0) > 0
                        and feed.queue_max < feed.queue_min_recommended
                    ):
                        feed._queue_capacity_breach_logged = True
                        payload = {
                            "symbol": symbol,
                            "tf": feed.intervalo,
                            "queue_max": feed.queue_max,
                            "queue_min_recommended": feed.queue_min_recommended,
                            "queue_size": current_size,
                            "drops": feed._stats[symbol]["dropped"],
                        }
                        log.warning("queue.capacity.breach", extra=safe_extra(payload))
                        events.emit_event(feed, "queue_capacity_breach", payload)
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0)
                    continue


    log.info(
        "queue.enqueue",
        extra=safe_extra(
            {
                "symbol": symbol,
                "tf": feed.intervalo,
                "bar_open_ts": bar_open_ts,
                "bar_close_ts": bar_close_ts,
                "queue_size": queue_size_after,
            }
        ),
    )

    DATAFEED_CANDLES_ENQUEUED_TOTAL.labels(
        symbol=str(symbol).upper(),
        tf=tf_label,
    ).inc()

    if metrics_enabled:
        registrar_vela_recibida(symbol_label, timeframe_label)

    feed._stats[symbol]["received"] += 1
    feed._last_close_ts[symbol] = ts
    feed._ultimo_candle[symbol] = {
        k: candle.get(k) for k in ("timestamp", "open", "high", "low", "close", "volume")
    }
    feed._last_monotonic[symbol] = time.monotonic()
    events.emit_event(feed, "tick", {"symbol": symbol, "ts": ts})


async def consumer_loop(feed: "DataFeed", symbol: str) -> None:
    """Consume velas de la cola y ejecuta el handler del usuario."""

    queue = feed._queues[symbol]
    _ensure_patch_logged()
    log.debug(
        "consumer.signature",
        extra=safe_extra(
            {"id": id(feed), "file": __file__, "stage": "DataFeed._consumer"}
        ),
    )
    feed._consumer_last[symbol] = time.monotonic()
    events.set_consumer_state(feed, symbol, ConsumerState.STARTING)
    log.info(
        "consumer.loop.start",
        extra=safe_extra({"symbol": symbol, "tf": feed.intervalo, "stage": "DataFeed._consumer"}),
    )

    while feed._running:
        candle = await queue.get()
        sym = str(candle.get("symbol") or symbol).upper()
        ts = candle.get("timestamp") or candle.get("close_time") or candle.get("open_time")
        timeframe = candle.get("timeframe") or candle.get("interval") or candle.get("tf")
        outcome = "ok"
        handler = feed._handler
        if handler is None:
            log.error(
                "handler.missing",
                extra=safe_extra(
                    {"symbol": sym, "timestamp": ts, "stage": "DataFeed._consumer"}
                ),
            )
            queue.task_done()
            continue
        handler_info = describe_handler(handler)
        if feed._handler_expected_info and handler_info["id"] != feed._handler_expected_info["id"]:
            log.warning(
                "handler.mismatch",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "stage": "DataFeed._consumer",
                        "expected_id": feed._handler_expected_info["id"],
                        "expected_line": feed._handler_expected_info.get("line"),
                        "actual_id": handler_info["id"],
                        "actual_line": handler_info.get("line"),
                    }
                ),
            )
        log.debug(
            "consumer.enter",
            extra=safe_extra(
                {
                    "symbol": sym,
                    "timestamp": ts,
                    "tf": timeframe,
                    "handler_id": handler_info["id"],
                    "stage": "DataFeed._consumer",
                }
            ),
        )
        feed._handler_expected_info = handler_info
        
        handler_completed = False
        try:
            if candle.get("_df_enqueue_time") is not None:
                edad = time.monotonic() - candle["_df_enqueue_time"]
                feed._stats[symbol]["latency_ms"] = int(edad * 1000)

            events.set_consumer_state(feed, symbol, ConsumerState.HEALTHY)
            feed._stats[symbol]["handler_calls"] += 1
            log.debug(
                "handler.invoke",
                extra=safe_extra({"symbol": sym, "stage": "DataFeed._consumer"}),
            )
            await asyncio.wait_for(handler(candle), timeout=feed.handler_timeout)
            handler_completed = True

        except asyncio.TimeoutError:
            outcome = "timeout"
            feed._stats[symbol]["handler_timeouts"] += 1
            log.error(
                "handler.timeout",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeout": feed.handler_timeout,
                        "stage": "DataFeed._consumer",
                    }
                ),
            )
            events.emit_event(feed, "handler_timeout", {"symbol": sym, "ts": ts})

        except asyncio.CancelledError:
            queue.task_done()
            raise

        except Exception as exc:
            outcome = "error"
            feed._stats[symbol]["consumer_errors"] += 1
            log.exception(
                "Error en handler",
                extra=safe_extra({"symbol": sym, "timestamp": ts}),
            )
            events.emit_event(feed, "consumer_error", {"symbol": sym, "ts": ts, "error": str(exc)})

        finally:
            feed._consumer_last[symbol] = time.monotonic()
            queue.task_done()
            skip_reason = candle.pop("_df_skip_reason", None)
            skip_details = candle.pop("_df_skip_details", None)
            if outcome == "ok" and skip_reason:
                outcome = "skipped"
                feed._stats[symbol]["skipped"] += 1
                payload = {
                    "symbol": sym,
                    "timestamp": ts,
                    "stage": "DataFeed._consumer",
                    "reason": skip_reason,
                    "event": "consumer.skip",
                }
                timeframe = candle.get("timeframe") or candle.get("interval") or candle.get("tf")
                if timeframe:
                    payload["timeframe"] = timeframe
                if skip_details:
                    payload["details"] = skip_details
                eval_ctx = candle.get("_df_eval_context") if isinstance(candle, dict) else None
                if isinstance(eval_ctx, dict):
                    for key in (
                        "buffer_len",
                        "min_needed",
                        "bar_open_ts",
                        "event_ts",
                        "elapsed_secs",
                        "interval_secs",
                        "bar_close_ts",
                    ):
                        if key in eval_ctx and key not in payload:
                            payload[key] = eval_ctx[key]
                details_map = skip_details if isinstance(skip_details, dict) else {}
                timing_map = (
                    details_map.get("timing")
                    if isinstance(details_map.get("timing"), dict)
                    else {}
                )
                if isinstance(details_map, dict):
                    for key in ("buffer_len", "min_needed"):
                        if key not in payload and key in details_map:
                            payload[key] = details_map[key]
                if isinstance(timing_map, dict):
                    for key, alias in (
                        ("bar_open_ts", "bar_open_ts"),
                        ("event_ts", "bar_event_ts"),
                        ("elapsed_secs", "elapsed_secs"),
                        ("interval_secs", "interval_secs"),
                        ("bar_close_ts", "bar_close_ts"),
                    ):
                        if key not in payload and alias in timing_map:
                            payload[key] = timing_map[alias]
                trace_id = candle.get("_df_trace_id") if isinstance(candle, dict) else None
                if trace_id:
                    payload["trace_id"] = trace_id

                log_method = log.info
                if skip_reason == "no_signal":
                    feed._stats[symbol]["skipped_expected"] += 1
                    timeframe_label = payload.get("timeframe") or feed.intervalo
                    try:
                        CONSUMER_SKIPPED_EXPECTED_TOTAL.labels(sym, timeframe_label, skip_reason).inc()
                    except Exception:
                        pass
                    log_method = log.debug
                log_method("consumer.skip", extra=safe_extra(payload))
            elif outcome == "ok":
                feed._stats[symbol]["processed"] += 1
            should_activate_debug_wrapper = (
                outcome == "ok"
                and feed._handler_log_events == 0
                and not getattr(handler, "__df_debug_wrapper__", False)
                and _debug_wrapper_enabled()
            )
            if handler_completed:
                note_handler_log(feed)
            log.debug(
                "consumer.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "tf": timeframe,
                        "outcome": outcome,
                        "stage": "DataFeed._consumer",
                        "queue_size": queue.qsize(),
                        "handler_wrapper_calls": feed._handler_wrapper_calls,
                    }
                ),
            )
            if should_activate_debug_wrapper:
                activate_handler_debug_wrapper(feed)
            ts_processed = candle.get("timestamp")
            prev = getattr(feed, f"_last_processed_{symbol}", None)
            if ts_processed is not None:
                if prev is not None and ts_processed <= prev:
                    events.set_consumer_state(feed, symbol, ConsumerState.LOOP)
                    log.error(
                        "%s: consumer sin avanzar timestamp (prev=%s actual=%s)",
                        symbol,
                        prev,
                        ts_processed,
                    )
                    await reiniciar_consumer(feed, symbol)
                    continue
                setattr(feed, f"_last_processed_{symbol}", ts_processed)


async def reiniciar_consumer(feed: "DataFeed", symbol: str) -> None:
    """Reinicia el consumidor asociado al símbolo."""

    task = feed._consumer_tasks.get(symbol)
    if task and not task.done():
        task.cancel()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, timeout=feed.cancel_timeout)
    feed._consumer_tasks[symbol] = asyncio.create_task(
        consumer_loop(feed, symbol), name=f"consumer_{symbol}"
    )


def note_handler_log(feed: "DataFeed") -> None:
    """Marca que el handler del usuario generó logs."""

    feed._handler_log_events += 1


def describe_handler(handler: Callable[[dict], Awaitable[None]]) -> dict[str, int | str | None]:
    """Obtiene metadatos relevantes del handler para trazabilidad."""

    qualname = getattr(handler, "__qualname__", None)
    code = getattr(handler, "__code__", None)
    line = code.co_firstlineno if code else None
    module = getattr(handler, "__module__", None)
    return {"id": id(handler), "qualname": qualname, "line": line, "module": module}


def activate_handler_debug_wrapper(feed: "DataFeed") -> None:
    """Envuelve el handler para instrumentar llamadas si no hay evidencias de logs."""

    if feed._handler is None:
        return
    if not _debug_wrapper_enabled():
        return
    if getattr(feed._handler, "__df_debug_wrapper__", False):
        return

    original = feed._handler
    handler_info = describe_handler(original)

    async def _wrapped(candle: dict) -> None:
        feed._handler_wrapper_calls += 1
        call_idx = feed._handler_wrapper_calls
        log.debug(
            "handler.wrapper.enter",
            extra=safe_extra(
                {
                    "stage": "DataFeed._consumer",
                    "handler_id": handler_info["id"],
                    "handler_line": handler_info.get("line"),
                    "call": call_idx,
                }
            ),
        )
        try:
            await original(candle)
        finally:
            log.debug(
                "handler.wrapper.exit",
                extra=safe_extra(
                    {
                        "stage": "DataFeed._consumer",
                        "handler_id": handler_info["id"],
                        "handler_line": handler_info.get("line"),
                        "call": call_idx,
                    }
                )
            )

    setattr(_wrapped, "__df_debug_wrapper__", True)
    setattr(_wrapped, "__df_original__", original)
    setattr(_wrapped, "__qualname__", getattr(original, "__qualname__", _wrapped.__qualname__))
    feed._handler = _wrapped
    feed._handler_expected_info = describe_handler(_wrapped)
    log.warning(
        "handler.wrapper.enabled",
        extra=safe_extra(
            {
                "stage": "DataFeed._consumer",
                "handler_id": handler_info["id"],
                "handler_line": handler_info.get("line"),
                "reason": "missing_trader_logs",
            }
        ),
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .datafeed import DataFeed
