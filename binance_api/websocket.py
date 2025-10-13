"""Implementación de streams de velas para Binance (simulado y real)."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, Optional, Sequence, TYPE_CHECKING

import websockets

if TYPE_CHECKING:  # pragma: no cover - hints opcionales
    from .cliente import BinanceClient

from .utils import normalize_symbol_for_ws

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
    """Escucha velas para ``symbol`` desde Binance o modo simulado."""

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
    """Escucha velas para múltiples símbolos."""

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
                return
            result = fn(candle)
            if asyncio.iscoroutine(result):
                await result
    else:
        async def dispatch(symbol: str, candle: Dict[str, Any]) -> None:
            result = handler(symbol, candle)  # type: ignore[misc]
            if asyncio.iscoroutine(result):
                await result

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
        await dispatch(symbol, candle)

    await _consume_ws_stream(
        url,
        process_combined,
        timeout_inactividad=timeout_inactividad,
        mensaje_timeout=mensaje_timeout,
    )


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
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=inactivity)
                    except asyncio.TimeoutError as exc:
                        raise InactividadTimeoutError(
                            f"Sin mensajes de Binance en {inactivity}s"
                        ) from exc
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
                    data = json.loads(raw_text)
                    result = handler(data)
                    if asyncio.iscoroutine(result):
                        await result
        except InactividadTimeoutError:
            raise
        except asyncio.CancelledError:
            raise
        except websockets.ConnectionClosedOK:  # pragma: no cover - desconexión limpia
            await asyncio.sleep(0)
        except (websockets.WebSocketException, OSError) as exc:
            logger.warning(
                "binance_ws_retry",
                extra={"event": "binance_ws_retry", "url": url, "error": repr(exc), "backoff": round(backoff, 2)},
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
        else:
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
        "volume": float(kline.get("v", 0.0)),
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
