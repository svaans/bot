import asyncio
import random
from typing import List

from binance_api.websocket import InactividadTimeoutError

from . import escuchar_velas, escuchar_velas_combinado

from ._shared import COMBINED_STREAM_KEY, log
from . import events


def _emit_ws_drop_signal(feed: "DataFeed", payload: dict[str, object]) -> None:
    """Emite una señal diagnóstica cuando se detecta la caída de un stream WS."""

    if not events.ws_signals_enabled():
        return
    enriched = {"intervalo": feed.intervalo, **payload}
    feed._emit_bus_signal("datafeed.ws.drop", enriched)


def _compute_retry_delay(feed: "DataFeed", key: str, *, reset_backoff: bool = False) -> float:
    """Calcula el tiempo de espera aplicando backoff exponencial con jitter."""

    attempts = 1
    telemetry = getattr(feed, "_ws_retry_telemetry", {})
    if isinstance(telemetry, dict):
        data = telemetry.get(key)
        if isinstance(data, dict):
            raw_attempts = data.get("attempts")
            try:
                attempts = max(1, int(raw_attempts))
            except (TypeError, ValueError):
                attempts = 1
    if reset_backoff:
        attempts = 1

    base = getattr(feed, "ws_backoff_base", 0.5) or 0.5
    max_delay = getattr(feed, "ws_backoff_max", 60.0) or 60.0
    jitter_factor = getattr(feed, "ws_backoff_jitter", 0.25) or 0.0

    delay = base * (2 ** max(0, attempts - 1))
    delay = min(delay, max_delay)

    if jitter_factor > 0:
        low = max(0.0, 1.0 - float(jitter_factor))
        high = 1.0 + float(jitter_factor)
        if high <= low:
            high = low + 0.01
        delay *= random.uniform(low, high)

    return max(0.0, round(delay, 3))


async def stream_simple(feed: "DataFeed", symbol: str) -> None:
    """Loop principal de escucha de velas por símbolo."""

    primera_vez = True
    while feed._running and symbol in feed._queues:
        if not events.verify_reconnect_limits(feed, symbol, "loop_guard"):
            return
        try:
            if primera_vez:
                await asyncio.sleep(random.random())  # desincroniza arranques
                primera_vez = False

            await feed._do_backfill(symbol)
            log.info(
                "ws_connect:start",
                extra={
                    "symbol": symbol,
                    "intervalo": feed.intervalo,
                    "stage": "DataFeed",
                },
            )
            if not feed.ws_connected_event.is_set():
                events.signal_ws_connected(feed, symbol)

            await escuchar_velas(
                symbol,
                feed.intervalo,
                feed._handle_candle,
                feed.tiempo_inactividad,
                feed._cliente,
                mensaje_timeout=feed.tiempo_inactividad,
                backpressure=feed.backpressure,
                ultimos={
                    "ultimo_timestamp": feed._last_close_ts.get(symbol),
                    "ultimo_cierre": (
                        feed._ultimo_candle.get(symbol, {}).get("close")
                        if feed._ultimo_candle.get(symbol)
                        else None
                    ),
                },
            )
            drop_payload = {"symbol": symbol, "reason": "stream_end"}
            events.mark_ws_state(feed, False, drop_payload)
            _emit_ws_drop_signal(feed, drop_payload)
            log.info("stream simple finalizado; reintentando…", extra={"symbol": symbol})
            events.emit_event(feed, "ws_end", {"symbol": symbol})
            if not events.register_reconnect_attempt(feed, symbol, "stream_end"):
                return
            delay = _compute_retry_delay(feed, symbol)
            await asyncio.sleep(delay)

        except InactividadTimeoutError:
            log.warning(
                "ws_connect:retry",
                extra={
                    "symbol": symbol,
                    "intervalo": feed.intervalo,
                    "stage": "DataFeed",
                    "reason": "inactividad",
                },
                exc_info=True,
            )
            drop_payload = {"symbol": symbol, "reason": "inactividad"}
            events.mark_ws_state(feed, False, drop_payload)
            _emit_ws_drop_signal(feed, drop_payload)
            log.warning("stream simple: reinicio por inactividad", extra={"symbol": symbol})
            events.emit_event(feed, "ws_inactividad", {"symbol": symbol})
            if not feed.ws_connected_event.is_set():
                events.signal_ws_failure(feed, "Timeout de inactividad durante el arranque del WS")
                return
            if not events.register_reconnect_attempt(feed, symbol, "inactividad"):
                return
            delay = _compute_retry_delay(feed, symbol, reset_backoff=True)
            await asyncio.sleep(delay)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            log.warning(
                "ws_connect:retry",
                extra={
                    "symbol": symbol,
                    "intervalo": feed.intervalo,
                    "stage": "DataFeed",
                    "reason": type(exc).__name__,
                },
                exc_info=True,
            )
            drop_payload = {"symbol": symbol, "reason": type(exc).__name__}
            events.mark_ws_state(feed, False, drop_payload)
            _emit_ws_drop_signal(feed, drop_payload)
            log.exception("stream simple: error; reintentando", extra={"symbol": symbol})
            events.emit_event(feed, "ws_error", {"symbol": symbol, "error": str(exc)})
            if not feed.ws_connected_event.is_set():
                events.signal_ws_failure(feed, exc)
                return
            if not events.register_reconnect_attempt(feed, symbol, "error"):
                return
            delay = _compute_retry_delay(feed, symbol)
            await asyncio.sleep(delay)


async def stream_combinado(feed: "DataFeed", symbols: List[str]) -> None:
    """Loop principal para streams combinados de múltiples símbolos."""

    primera_vez = True
    while feed._running and all(s in feed._queues for s in symbols):
        if not events.verify_reconnect_limits(feed, COMBINED_STREAM_KEY, "loop_guard"):
            return
        try:
            if primera_vez:
                await asyncio.sleep(random.random())
                primera_vez = False

            await asyncio.gather(*(feed._do_backfill(s) for s in symbols))
            log.info(
                "ws_connect:start",
                extra={
                    "symbols": symbols,
                    "intervalo": feed.intervalo,
                    "stage": "DataFeed",
                    "mode": "combined",
                },
            )
            if not feed.ws_connected_event.is_set():
                objetivo = symbols[0] if symbols else None
                events.signal_ws_connected(feed, objetivo)

            async def wrap(sym: str):
                async def _handler(candle: dict) -> None:
                    await feed._handle_candle(sym, candle)

                return _handler

            handlers = {s: await wrap(s) for s in symbols}

            await escuchar_velas_combinado(
                symbols,
                feed.intervalo,
                handlers,
                {},
                feed.tiempo_inactividad,
                60,
                cliente=feed._cliente,
                mensaje_timeout=feed.tiempo_inactividad,
                backpressure=feed.backpressure,
                ultimos={
                    s: {
                        "ultimo_timestamp": feed._last_close_ts.get(s),
                        "ultimo_cierre": (
                            feed._ultimo_candle.get(s, {}).get("close")
                            if feed._ultimo_candle.get(s)
                            else None
                        ),
                    }
                    for s in symbols
                },
            )
            drop_payload = {"symbols": symbols, "reason": "stream_end"}
            events.mark_ws_state(feed, False, drop_payload)
            _emit_ws_drop_signal(feed, drop_payload)
            log.info("stream combinado finalizado; reintentando…")
            events.emit_event(feed, "ws_end", {"symbols": symbols})
            if not events.register_reconnect_attempt(feed, COMBINED_STREAM_KEY, "stream_end"):
                return
            delay = _compute_retry_delay(feed, COMBINED_STREAM_KEY)
            await asyncio.sleep(delay)

        except InactividadTimeoutError as exc:
            log.warning(
                "ws_connect:retry",
                extra={
                    "symbols": symbols,
                    "intervalo": feed.intervalo,
                    "stage": "DataFeed",
                    "reason": "inactividad",
                },
                exc_info=True,
            )
            drop_payload = {"symbols": symbols, "reason": "inactividad"}
            events.mark_ws_state(feed, False, drop_payload)
            _emit_ws_drop_signal(feed, drop_payload)
            log.warning("stream combinado: reinicio por inactividad")
            events.emit_event(feed, "ws_inactividad", {"symbols": symbols})
            if not feed.ws_connected_event.is_set():
                events.signal_ws_failure(feed, "Timeout de inactividad durante el arranque del WS")
                return
            if not events.register_reconnect_attempt(feed, COMBINED_STREAM_KEY, "inactividad"):
                return
            delay = _compute_retry_delay(feed, COMBINED_STREAM_KEY, reset_backoff=True)
            await asyncio.sleep(delay)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            log.warning(
                "ws_connect:retry",
                extra={
                    "symbols": symbols,
                    "intervalo": feed.intervalo,
                    "stage": "DataFeed",
                    "reason": type(exc).__name__,
                },
                exc_info=True,
            )
            drop_payload = {"symbols": symbols, "reason": type(exc).__name__}
            events.mark_ws_state(feed, False, drop_payload)
            _emit_ws_drop_signal(feed, drop_payload)
            log.exception("stream combinado: error; reintentando")
            events.emit_event(feed, "ws_error", {"symbols": symbols, "error": str(exc)})
            if not feed.ws_connected_event.is_set():
                events.signal_ws_failure(feed, exc)
                return
            if not events.register_reconnect_attempt(feed, COMBINED_STREAM_KEY, "error"):
                return
            delay = _compute_retry_delay(feed, COMBINED_STREAM_KEY)
            await asyncio.sleep(delay)


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .datafeed import DataFeed  # Ensure DataFeed is imported from the correct module
