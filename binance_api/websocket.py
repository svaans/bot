"""Implementación de streams de velas para Binance (simulado y real)."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, Optional, Sequence, TYPE_CHECKING

import websockets

if TYPE_CHECKING:  # pragma: no cover - hints opcionales
    from .cliente import BinanceClient

from core.utils.log_utils import truncate_for_log

from .utils import normalize_symbol_for_rest, normalize_symbol_for_ws


def _record_ws_dispatch_queue_metrics(depth: int, maxsize: int) -> None:
    """Actualiza gauge/contador de la cola Fase 2 sin alterar el comportamiento del worker."""
    try:
        from core.metrics import WS_DISPATCH_BACKLOG_EVENTS_TOTAL, WS_DISPATCH_QUEUE_DEPTH
    except Exception:
        return
    warn_floor = max(16, (maxsize * 3) // 4)
    try:
        WS_DISPATCH_QUEUE_DEPTH.set(float(depth))
    except Exception:
        pass
    if depth >= warn_floor:
        try:
            WS_DISPATCH_BACKLOG_EVENTS_TOTAL.inc()
        except Exception:
            pass

__all__ = [
    "InactividadTimeoutError",
    "escuchar_velas",
    "escuchar_velas_combinado",
]

logger = logging.getLogger(__name__)
UTC = timezone.utc
_WS_BASE_URL = "wss://stream.binance.com:9443"
_WS_TESTNET_BASE_URL = "wss://testnet.binance.vision"
_DEFAULT_STREAM_PATTERN = "^[a-z0-9]+@kline_{interval}$"


class InactividadTimeoutError(RuntimeError):
    """Señala que no llegaron velas dentro del timeout establecido."""


@dataclass(slots=True)
class _StreamState:
    symbol: str
    intervalo: str
    ultimo_ts: int
    ultimo_close: float


def _intervalo_segundos(intervalo: str) -> float:
    mapping = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "8h": 28800,
        "12h": 43200,
        "1d": 86400,
    }
    try:
        return float(mapping[intervalo])
    except KeyError as exc:  # pragma: no cover
        raise ValueError(f"Intervalo no soportado: {intervalo}") from exc


async def _call_handler(
    handler: Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None],
    candle: Dict[str, Any],
) -> None:
    result = handler(candle)
    if asyncio.iscoroutine(result):
        await result


def _init_state(
    symbol: str,
    intervalo: str,
    ultimo_timestamp: Optional[int],
    ultimo_cierre: Optional[float],
) -> _StreamState:
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    step_ms = int(_intervalo_segundos(intervalo) * 1000)
    if ultimo_timestamp:
        base_ts = int(ultimo_timestamp)
    else:
        base_ts = max(0, ((now_ms // step_ms) - 1) * step_ms)
    close = ultimo_cierre if ultimo_cierre is not None else 100.0
    return _StreamState(symbol, intervalo, base_ts, float(close))


def _should_simulate(cliente: Any | None) -> bool:
    return cliente is None or getattr(cliente, "simulated", True)


def _ws_base_url(cliente: Any | None) -> str:
    testnet = bool(getattr(cliente, "testnet", False))
    return _WS_TESTNET_BASE_URL if testnet else _WS_BASE_URL


async def escuchar_velas(
    symbol: str,
    intervalo: str,
    handler: Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None],
    _unused: Dict[str, Any],
    timeout_inactividad: float,
    _heartbeat: float,
    *,
    cliente: Any | None = None,
    mensaje_timeout: float | None = None,
    backpressure: bool = True,
    ultimo_timestamp: Optional[int] = None,
    ultimo_cierre: Optional[float] = None,
) -> None:
    """Escucha velas para ``symbol`` desde Binance o modo simulado.

    En modo real, ``backpressure`` se conserva en la firma por compatibilidad con
    :class:`core.data_feed.DataFeed` y el wrapper en ``core.data_feed``; el
    cliente WebSocket no aplica colas propias: el backpressure real es la
    política de cola del DataFeed (``DF_QUEUE_*``, ``DF_BACKPRESSURE``).
    """

    if _should_simulate(cliente):
        await _escuchar_velas_simulado(
            symbol,
            intervalo,
            handler,
            timeout_inactividad=timeout_inactividad,
            ultimo_timestamp=ultimo_timestamp,
            ultimo_cierre=ultimo_cierre,
        )
        return

    await _escuchar_velas_real(
        symbol,
        intervalo,
        handler,
        timeout_inactividad=timeout_inactividad,
        mensaje_timeout=mensaje_timeout,
        cliente=cliente,
    )


async def escuchar_velas_combinado(
    symbols: Iterable[str],
    intervalo: str,
    handler: Callable[[str, Dict[str, Any]], Awaitable[None]]
    | Callable[[str, Dict[str, Any]], None]
    | Mapping[str, Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None]],
    _unused: Dict[str, Any],
    timeout_inactividad: float,
    _heartbeat: float,
    *,
    cliente: Any | None = None,
    mensaje_timeout: float | None = None,
    backpressure: bool = True,
    ultimo_timestamp: Optional[int] = None,
    ultimo_cierre: Optional[float] = None,
    ultimos: Mapping[str, Mapping[str, Any]] | None = None,
) -> None:
    """Escucha velas para múltiples símbolos.

    Con cliente real, ``backpressure`` no altera el socket; el DataFeed regula
    la carga vía colas. En simulación combinada se reenvía a cada tarea simulada.
    """

    if _should_simulate(cliente):
        await _escuchar_velas_combinado_simulado(
            symbols,
            intervalo,
            handler,
            timeout_inactividad=timeout_inactividad,
            cliente=cliente,
            mensaje_timeout=mensaje_timeout,
            backpressure=backpressure,
            ultimo_timestamp=ultimo_timestamp,
            ultimo_cierre=ultimo_cierre,
            ultimos=ultimos,
        )
        return

    await _escuchar_velas_combinado_real(
        list(symbols),
        intervalo,
        handler,
        timeout_inactividad=timeout_inactividad,
        mensaje_timeout=mensaje_timeout,
        cliente=cliente,
    )


# ──────────────────────────────── SIMULACIÓN ───────────────────────────────
async def _escuchar_velas_simulado(
    symbol: str,
    intervalo: str,
    handler: Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None],
    *,
    timeout_inactividad: float,
    ultimo_timestamp: Optional[int],
    ultimo_cierre: Optional[float],
) -> None:
    del timeout_inactividad  # El modo simulado no expira por inactividad

    state = _init_state(symbol, intervalo, ultimo_timestamp, ultimo_cierre)
    real_step = _intervalo_segundos(intervalo)
    intervalo_segundos = max(0.5, real_step / 60)
    paso_ms = int(real_step * 1000)
    last_emitted_close_time: Optional[int] = None
    current_close_time = state.ultimo_ts

    async def emitir_candle(candle: Dict[str, Any]) -> None:
        nonlocal last_emitted_close_time
        close_time = int(candle.get("close_time", 0))
        if last_emitted_close_time is not None and close_time <= last_emitted_close_time:
            return
        last_emitted_close_time = close_time
        await _call_handler(handler, candle)

    if ultimo_timestamp is not None:
        current_close_time = _calc_next_close_time(ultimo_timestamp, intervalo)
        candle = _build_backfill_candle(
            symbol,
            intervalo,
            current_close_time,
            state.ultimo_close if ultimo_cierre is None else float(ultimo_cierre),
        )
        state.ultimo_ts = current_close_time
        state.ultimo_close = float(candle["close"])
        await emitir_candle(candle)
        await asyncio.sleep(0)
        try:
            await asyncio.sleep(intervalo_segundos)
        except asyncio.CancelledError:
            raise

    while True:
        try:
            await asyncio.sleep(intervalo_segundos)
        except asyncio.CancelledError:
            raise
            
        if _is_being_cancelled():
            raise asyncio.CancelledError

        current_close_time += paso_ms
        state.ultimo_ts = current_close_time
        ruido = random.uniform(-0.3, 0.3)
        open_price = max(1.0, state.ultimo_close + ruido)
        close_price = max(1.0, open_price + random.uniform(-0.2, 0.2))
        high_price = max(open_price, close_price) + random.uniform(0.0, 0.1)
        low_price = min(open_price, close_price) - random.uniform(0.0, 0.1)
        volume = max(10.0, random.uniform(25.0, 35.0))

        open_time = max(0, current_close_time - paso_ms)
        event_time = current_close_time

        candle = {
            "event_time": event_time,
            "symbol": symbol,
            "intervalo": intervalo,
            "open_time": open_time,
            "close_time": current_close_time,
            "open": round(open_price, 6),
            "high": round(high_price, 6),
            "low": round(low_price, 6),
            "close": round(close_price, 6),
            "volume": round(volume, 6),
            "is_closed": True,
        }
        state.ultimo_close = close_price
        await emitir_candle(candle)


async def _escuchar_velas_combinado_simulado(
    symbols: Iterable[str],
    intervalo: str,
    handler: Callable[[str, Dict[str, Any]], Awaitable[None]]
    | Callable[[str, Dict[str, Any]], None]
    | Mapping[str, Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None]],
    *,
    timeout_inactividad: float,
    cliente: Any | None,
    mensaje_timeout: float | None,
    backpressure: bool,
    ultimo_timestamp: Optional[int],
    ultimo_cierre: Optional[float],
    ultimos: Mapping[str, Mapping[str, Any]] | None,
) -> None:

    def _resolver_handler(symbol: str):
        if isinstance(handler, Mapping):
            fn = handler.get(symbol)
            if fn is None:
                async def _dummy(_: Dict[str, Any]) -> None:
                    return None
                return _dummy
            return fn
        return lambda candle: handler(symbol, candle)  # type: ignore[return-value]

    tasks = []
    for symbol in symbols:
        info = (ultimos or {}).get(symbol, {})
        tasks.append(
            asyncio.create_task(
                escuchar_velas(
                    symbol,
                    intervalo,
                    _resolver_handler(symbol),
                    {},
                    timeout_inactividad=1.0,
                    _heartbeat=1.0,
                    cliente=None,
                    mensaje_timeout=mensaje_timeout,
                    backpressure=backpressure,
                    ultimo_timestamp=info.get("ultimo_timestamp", ultimo_timestamp),
                    ultimo_cierre=info.get("ultimo_cierre", ultimo_cierre),
                )
            )
        )
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()

# ───────────────────────────────── MODO REAL ───────────────────────────────
def _build_stream_name(symbol: str, intervalo: str) -> str:
    symbol_ws = normalize_symbol_for_ws(symbol)
    return f"{symbol_ws}@kline_{intervalo.lower()}"


def _stream_pattern(intervalo: str) -> re.Pattern[str]:
    pattern = _DEFAULT_STREAM_PATTERN.format(interval=re.escape(intervalo.lower()))
    return re.compile(pattern)


def _ensure_valid_streams(streams: Sequence[str], intervalo: str) -> None:
    pattern = _stream_pattern(intervalo)
    for stream in streams:
        if not pattern.fullmatch(stream):
            logger.error(
                "ws.subscribe.invalid_stream",
                extra={
                    "event": "ws.subscribe.invalid_stream",
                    "stream": stream,
                    "reason": "format",
                },
            )
            raise ValueError(f"Formato de stream inválido: {stream}")


def _log_subscribe_streams(streams: Sequence[str]) -> None:
    logger.info(
        "ws.subscribe.streams",
        extra={"event": "ws.subscribe.streams", "streams": list(streams)},
    )


def _log_connect_url(url: str) -> None:
    logger.info("ws.connect.url", extra={"event": "ws.connect.url", "url": url})


def _log_ignored(stream: str, reason: str) -> None:
    logger.debug(
        "ws.msg.ignored",
        extra={"event": "ws.msg.ignored", "stream": stream, "reason": reason},
    )


def _log_parsed(stream: str, event_type: str | None, candle: Dict[str, Any]) -> None:
    logger.debug(
        "ws.msg.parsed",
        extra={
            "event": "ws.msg.parsed",
            "stream": stream,
            "event_type": event_type,
            "kline_tf": candle.get("intervalo"),
            "symbol": candle.get("symbol"),
            "closed": bool(candle.get("is_closed")),
        },
    )


async def _escuchar_velas_real(
    symbol: str,
    intervalo: str,
    handler: Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None],
    *,
    timeout_inactividad: float,
    mensaje_timeout: float | None,
    cliente: Any | None,
) -> None:
    stream = _build_stream_name(symbol, intervalo)
    _ensure_valid_streams([stream], intervalo)
    _log_subscribe_streams([stream])
    url = f"{_ws_base_url(cliente)}/ws/{stream}"
    _log_connect_url(url)
    await _consume_ws_stream(
        url,
        lambda payload: _emit_if_closed(handler, payload, stream, intervalo),
        timeout_inactividad=timeout_inactividad,
        mensaje_timeout=mensaje_timeout,
    )


async def _escuchar_velas_combinado_real(
    symbols: Iterable[str],
    intervalo: str,
    handler: Callable[[str, Dict[str, Any]], Awaitable[None]]
    | Callable[[str, Dict[str, Any]], None]
    | Mapping[str, Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None]],
    *,
    timeout_inactividad: float,
    mensaje_timeout: float | None,
    cliente: Any | None,
) -> None:
    streams = [_build_stream_name(symbol, intervalo) for symbol in symbols]
    _ensure_valid_streams(streams, intervalo)
    _log_subscribe_streams(streams)
    url = f"{_ws_base_url(cliente)}/stream?streams={'/'.join(streams)}"
    _log_connect_url(url)

    if isinstance(handler, Mapping):
        async def dispatch(symbol: str, candle: Dict[str, Any]) -> None:
            symbol_upper = symbol.upper()
            fn = handler.get(symbol_upper) or handler.get(symbol)
            if fn is None:
                for key, candidate in handler.items():
                    try:
                        if normalize_symbol_for_rest(str(key)) == symbol_upper:
                            fn = candidate
                            break
                    except Exception:
                        continue
            if fn is None:
                return
            result = fn(candle)
            if asyncio.iscoroutine(result):
                await result
    else:
        async def dispatch(symbol: str, candle: Dict[str, Any]) -> None:
            result = handler(symbol, candle)  # type: ignore[misc]
            if asyncio.iscoroutine(result):
                await result

    # Diagnóstico: deja traza INFO de la PRIMERA vela cerrada dispatchada por
    # símbolo. Permite verificar en producción que el pipeline WS -> trader se
    # activa realmente tras ``binance_ws_connected`` sin tener que subir todo a
    # DEBUG. El set vive en el closure del consumidor combinado.
    first_closed_seen: set[str] = set()

    async def process_combined(payload: Dict[str, Any]) -> None:
        stream_name = str(payload.get("stream") or "")
        data = payload.get("data", {})
        if not stream_name or not isinstance(data, Mapping):
            _log_ignored(stream_name or "unknown", "malformed_payload")
            return

        kline = data.get("k")
        if not isinstance(kline, Mapping):
            _log_ignored(stream_name, "missing_kline")
            return

        interval = str(kline.get("i", "")).lower()
        if interval != intervalo.lower():
            _log_ignored(stream_name, "interval_mismatch")
            return

        if not bool(kline.get("x")):
            _log_ignored(stream_name, "not_closed")
            return
        candle = _convert_kline(dict(kline))
        if candle is None:
            _log_ignored(stream_name, "convert_failed")
            return
        symbol = str(kline.get("s") or stream_name.split("@", 1)[0]).upper()
        candle.setdefault("stream", stream_name)
        _log_parsed(stream_name, data.get("e"), candle)
        if symbol not in first_closed_seen:
            first_closed_seen.add(symbol)
            logger.info(
                "ws.first_closed_candle",
                extra={
                    "event": "ws.first_closed_candle",
                    "symbol": symbol,
                    "stream": stream_name,
                    "interval": interval,
                    "close_time": candle.get("close_time"),
                },
            )
        # --- DIAGNÓSTICO TEMPORAL — REVERTIR ------------------------------
        # Log INFO de TODA vela cerrada dispatchada (no solo la primera).
        # Volumen bajo: ~1/min para 5 símbolos en 5m. Permite comparar con
        # ``df.recv.closed`` en el DataFeed: si la vela se loguea aquí pero
        # no llega a ``df.recv.closed`` es un problema de propagación del
        # handler; si llega a ``df.recv.closed`` pero no pasa de ``stale``
        # es un bug de ``_last_close_ts``/backfill_window.
        logger.info(
            "ws.closed_candle",
            extra={
                "event": "ws.closed_candle",
                "symbol": symbol,
                "stream": stream_name,
                "interval": interval,
                "open_time": candle.get("open_time"),
                "close_time": candle.get("close_time"),
            },
        )
        # ------------------------------------------------------------------
        await dispatch(symbol, candle)

    await _consume_ws_stream(
        url,
        process_combined,
        timeout_inactividad=timeout_inactividad,
        mensaje_timeout=mensaje_timeout,
    )


_WS_DISPATCH_QUEUE_DEFAULT = 64


def _ws_dispatch_queue_maxsize() -> int:
    """Tamaño máximo de la cola entre ``recv`` y el worker que ejecuta ``handler``.

    Fase 2: acota memoria y aplica backpressure (``put`` espera si el worker va
    atrasado). Rango razonable 1..4096; variable de entorno ``WS_DISPATCH_QUEUE_MAX``.
    """

    raw = os.getenv("WS_DISPATCH_QUEUE_MAX", str(_WS_DISPATCH_QUEUE_DEFAULT))
    try:
        n = int(raw)
    except (TypeError, ValueError):
        n = _WS_DISPATCH_QUEUE_DEFAULT
    return max(1, min(n, 4096))


async def _ws_dispatch_worker_loop(
    dispatch_queue: "asyncio.Queue[tuple[float, dict[str, Any]]]",
    handler: Callable[[Dict[str, Any]], Awaitable[None] | None],
    *,
    url: str,
) -> None:
    """Un único consumidor: preserva el orden de mensajes y serializa ``handler``."""

    while True:
        _recv_parse_t0, data = await dispatch_queue.get()
        try:
            result = handler(data)
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.exception(
                "ws.dispatch.handler_failed",
                extra={"event": "ws.dispatch.handler_failed", "url": url},
            )
            raise
        finally:
            dispatch_queue.task_done()


def _worker_failed_exc(worker_task: asyncio.Task) -> BaseException | None:
    if not worker_task.done():
        return None
    if worker_task.cancelled():
        return None
    exc = worker_task.exception()
    return exc if exc else None


async def _consume_ws_stream(
    url: str,
    handler: Callable[[Dict[str, Any]], Awaitable[None] | None],
    *,
    timeout_inactividad: float,
    mensaje_timeout: float | None,
) -> None:
    inactivity = mensaje_timeout or timeout_inactividad or 30.0
    inactivity = max(1.0, float(inactivity))
    backoff = 1.0
    while True:
        if _is_being_cancelled():
            raise asyncio.CancelledError
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                logger.info(
                    "binance_ws_connected",
                    extra={"event": "binance_ws_connected", "url": url},
                )
                backoff = 1.0
                # Diagnóstico: contador de frames recibidos en esta sesión WS.
                # Permite confirmar tráfico en vivo con una traza INFA cada
                # ``frames_log_every`` mensajes, sin bajar el resto a DEBUG.
                frames_received = 0
                frames_log_every = 100
                maxsize = _ws_dispatch_queue_maxsize()
                dispatch_queue: asyncio.Queue[tuple[float, dict[str, Any]]] = asyncio.Queue(
                    maxsize=maxsize
                )
                worker_task = asyncio.create_task(
                    _ws_dispatch_worker_loop(dispatch_queue, handler, url=url),
                    name="binance_ws_dispatch",
                )
                try:
                    while True:
                        failed = _worker_failed_exc(worker_task)
                        if failed is not None:
                            raise failed
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=inactivity)
                        except asyncio.TimeoutError as exc:
                            raise InactividadTimeoutError(
                                f"Sin mensajes de Binance en {inactivity}s"
                            ) from exc
                        recv_t0 = time.perf_counter()
                        frames_received += 1
                        if frames_received == 1 or frames_received % frames_log_every == 0:
                            logger.info(
                                "ws.frames.progress",
                                extra={
                                    "event": "ws.frames.progress",
                                    "url": url,
                                    "frames": frames_received,
                                },
                            )
                        preview: str
                        if isinstance(raw, bytes):
                            preview = raw[:64].decode("utf-8", "ignore")
                            raw_text = raw.decode("utf-8", "ignore")
                            raw_len = len(raw)
                        else:
                            preview = str(raw)[:64]
                            raw_text = str(raw)
                            raw_len = len(raw_text)
                        logger.debug(
                            "ws.recv.raw",
                            extra={
                                "event": "ws.recv.raw",
                                "url": url,
                                "len": raw_len,
                                "first_bytes": preview,
                            },
                        )
                        try:
                            data = json.loads(raw_text)
                        except json.JSONDecodeError as exc:
                            logger.warning(
                                "ws.recv.json_error",
                                extra={
                                    "event": "ws.recv.json_error",
                                    "url": url,
                                    "error": truncate_for_log(repr(exc), 400),
                                    "first_bytes": preview,
                                },
                            )
                            continue
                        if not isinstance(data, dict):
                            logger.debug(
                                "ws.recv.non_object_json",
                                extra={
                                    "event": "ws.recv.non_object_json",
                                    "url": url,
                                    "type": type(data).__name__,
                                },
                            )
                            continue
                        failed = _worker_failed_exc(worker_task)
                        if failed is not None:
                            raise failed
                        await dispatch_queue.put((recv_t0, data))
                        recv_to_enqueue_ms = (time.perf_counter() - recv_t0) * 1000.0
                        depth = dispatch_queue.qsize()
                        warn_floor = max(16, (maxsize * 3) // 4)
                        _record_ws_dispatch_queue_metrics(depth, maxsize)
                        if depth >= warn_floor:
                            logger.warning(
                                "ws.dispatch.backlog",
                                extra={
                                    "event": "ws.dispatch.backlog",
                                    "url": url,
                                    "depth": depth,
                                    "maxsize": maxsize,
                                },
                            )
                        logger.debug(
                            "ws.dispatch.enqueued",
                            extra={
                                "event": "ws.dispatch.enqueued",
                                "url": url,
                                "recv_to_enqueue_ms": round(recv_to_enqueue_ms, 4),
                                "queue_depth": depth,
                            },
                        )
                finally:
                    worker_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await worker_task
        except InactividadTimeoutError:
            raise
        except asyncio.CancelledError:
            raise
        except websockets.ConnectionClosedOK:  # pragma: no cover - desconexión limpia
            await asyncio.sleep(0)
        except (websockets.WebSocketException, OSError) as exc:
            logger.warning(
                "binance_ws_retry",
                extra={
                    "event": "binance_ws_retry",
                    "url": url,
                    "error": truncate_for_log(repr(exc), 400),
                    "backoff": round(backoff, 2),
                },
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
        else:
            # try completó sin excepción (p. ej. salida limpia del ``async with``
            # tras ``ConnectionClosedOK``): terminar el bucle externo de reconexión.
            break


def _emit_if_closed(
    handler: Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None],
    payload: Dict[str, Any],
    stream_name: str,
    intervalo: str,
) -> Awaitable[None] | None:
    event_type = payload.get("e")
    kline = payload.get("k")
    if not isinstance(kline, Mapping):
        _log_ignored(stream_name, "missing_kline")
        return None
    interval = str(kline.get("i", "")).lower()
    if interval != intervalo.lower():
        _log_ignored(stream_name, "interval_mismatch")
        return None
    if not bool(kline.get("x")):
        _log_ignored(stream_name, "not_closed")
        return None
    candle = _convert_kline(dict(kline))
    if candle is None:
        _log_ignored(stream_name, "convert_failed")
        return None
    candle.setdefault("stream", stream_name)
    _log_parsed(stream_name, event_type, candle)
    return _call_handler(handler, candle)


def _convert_kline(kline: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not kline or not kline.get("x"):
        return None
    raw_v = kline.get("v")
    vol = float(raw_v) if raw_v is not None else 0.0
    if vol == 0.0:
        logger.warning(
            "ws.kline.zero_volume",
            extra={"symbol": kline.get("s"), "t": kline.get("t"), "raw_v": raw_v},
        )
    return {
        "event_time": int(kline.get("T", kline.get("t", 0))),
        "symbol": kline.get("s", ""),
        "intervalo": kline.get("i", ""),
        "open_time": int(kline.get("t", 0)),
        "close_time": int(kline.get("T", 0)),
        "open": float(kline.get("o", 0.0)),
        "high": float(kline.get("h", 0.0)),
        "low": float(kline.get("l", 0.0)),
        "close": float(kline.get("c", 0.0)),
        "volume": vol,
        "is_closed": bool(kline.get("x", False)),
    }


def _calc_next_close_time(ultimo_ts: int, intervalo: str) -> int:
    """Devuelve el ``close_time`` inmediatamente posterior a ``ultimo_ts``."""

    step_ms = int(_intervalo_segundos(intervalo) * 1000)
    return ((int(ultimo_ts) // step_ms) + 1) * step_ms


def _build_backfill_candle(
    symbol: str,
    intervalo: str,
    close_time: int,
    last_close: float,
) -> Dict[str, Any]:
    """Crea una vela determinista para el arranque del stream."""

    base = max(1.0, float(last_close))
    high = round(base + 0.05, 6)
    low = round(max(0.1, base - 0.05), 6)
    paso_ms = int(_intervalo_segundos(intervalo) * 1000)
    open_time = max(0, close_time - paso_ms)
    return {
        "event_time": close_time,
        "symbol": symbol,
        "intervalo": intervalo,
        "open_time": open_time,
        "close_time": close_time,
        "open": round(base, 6),
        "high": high,
        "low": low,
        "close": round(base, 6),
        "volume": 30.0,
        "is_closed": True,
    }

def _is_being_cancelled() -> bool:
    """Indica si la tarea actual está en proceso de cancelación."""

    task = asyncio.current_task()
    if task is None:
        return False

    cancelling = getattr(task, "cancelling", None)
    if callable(cancelling):  # Python 3.11+
        try:
            return bool(cancelling())
        except Exception:  # pragma: no cover - defensivo
            return False

    return task.cancelled()
