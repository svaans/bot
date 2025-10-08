"""Funciones relacionadas con el procesamiento de velas."""

import asyncio
import contextlib
import time
from typing import Awaitable, Callable

from core.utils.utils import timestamp_alineado, validar_integridad_velas
from core.utils.log_utils import safe_extra
from ._shared import COMBINED_STREAM_KEY, ConsumerState, log
from . import events


async def handle_candle(feed: "DataFeed", symbol: str, candle: dict) -> None:
    """Normaliza y encola una vela recibida desde el stream."""

    if not candle.get("is_closed", True):
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
            if outcome == "ok" and skip_reason:
                outcome = "skipped"
                feed._stats[symbol]["skipped"] += 1
                payload = {
                    "symbol": sym,
                    "timestamp": ts,
                    "stage": "DataFeed._consumer",
                    "reason": skip_reason,
                }
                if skip_details:
                    payload["details"] = skip_details
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
