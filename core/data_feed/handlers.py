"""Funciones relacionadas con el procesamiento de velas."""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any, Awaitable, Callable, Mapping

from core.utils.utils import timestamp_alineado, validar_integridad_velas
from core.utils.log_utils import safe_extra
from ._shared import COMBINED_STREAM_KEY, ConsumerState, log
from . import events


_PATCH_LOGGED = False


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



async def handle_candle(feed: "DataFeed", symbol: str, candle: dict) -> None:
    """Normaliza y encola una vela recibida desde el stream."""

    candle = _normalize_candle_payload(candle)

    if not candle.get("is_closed", False):
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
    if ts is None or not timestamp_alineado(ts, feed.intervalo):
        return

    candle["timestamp"] = ts
    candle.setdefault("timeframe", feed.intervalo)
    candle.setdefault("interval", feed.intervalo)
    candle.setdefault("tf", feed.intervalo)
    last = feed._last_close_ts.get(symbol)
    if last is not None and ts <= last:
        return

    events.reset_reconnect_tracking(feed, symbol)
    if feed._combined:
        events.reset_reconnect_tracking(feed, COMBINED_STREAM_KEY)

    if not validar_integridad_velas(symbol, feed.intervalo, [candle], log):
        events.emit_event(feed, "candle_invalid", {"symbol": symbol, "ts": ts})
        return

    if not feed.ws_connected_event.is_set():
        events.signal_ws_connected(feed, symbol)

    queue = feed._queues.get(symbol)
    if queue is None:
        return

    candle.setdefault("_df_enqueue_time", time.monotonic())

    log.debug(
        "recv candle",
        extra=safe_extra({"symbol": symbol, "timestamp": ts, "queue_size": queue.qsize()}),
    )

    if feed.queue_policy == "block" or not queue.maxsize:
        await queue.put(candle)
    else:
        while True:
            try:
                queue.put_nowait(candle)
                break
            except asyncio.QueueFull:
                try:
                    _ = queue.get_nowait()
                    queue.task_done()
                    feed._stats[symbol]["dropped"] += 1
                    events.emit_event(feed, "queue_drop", {"symbol": symbol})
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0)
                    continue

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

    while feed._running:
        candle = await queue.get()
        sym = str(candle.get("symbol") or symbol).upper()
        ts = candle.get("timestamp") or candle.get("close_time") or candle.get("open_time")
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
            if skip_reason == "pipeline_missing":
                log.debug(
                    "consumer.pipeline_missing_ignored",
                    extra=safe_extra(
                        {
                            "symbol": sym,
                            "timestamp": ts,
                            "stage": "DataFeed._consumer",
                        }
                    ),
                )
                skip_reason = None
                skip_details = None
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
                log.debug("consumer.skip", extra=safe_extra(payload))
            elif outcome == "ok":
                feed._stats[symbol]["processed"] += 1
            if handler_completed:
                note_handler_log(feed)
            log.debug(
                "consumer.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "outcome": outcome,
                        "stage": "DataFeed._consumer",
                        "queue_size": queue.qsize(),
                        "handler_wrapper_calls": feed._handler_wrapper_calls,
                    }
                ),
            )
            if (
                outcome == "ok"
                and feed._handler_log_events == 0
                and not getattr(handler, "__df_debug_wrapper__", False)
            ):
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
