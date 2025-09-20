"""
Binance WebSocket Lite — mínimamente suficiente para DataFeedLite

Proporciona las 3 exportaciones que espera el feed:
- InactividadTimeoutError
- escuchar_velas(symbol, intervalo, handler, ...)
- escuchar_velas_combinado(symbols, intervalo, handlers, ...)

Filosofía
- Sin métricas ni dependencias al resto del bot.
- Sin suscripciones dinámicas (usamos endpoints con stream directo).
- Manejo de inactividad con timeout -> se levanta InactividadTimeoutError para que el caller relance.
- Backoff exponencial simple entre reconexiones.
- Solo emite velas **cerradas** (kline.x == True).

Notas
- DataFeedLite ya implementa backfill; aquí no lo duplicamos.
- `handlers` en combinado es un dict {symbol: async fn(candle_dict)} (igual que en tu código).
"""
from __future__ import annotations
import asyncio
import json
import random
import ssl
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List, Optional

import certifi
import websockets
from websockets.exceptions import ConnectionClosed

try:
    from core.utils.utils import configurar_logger, intervalo_a_segundos
except Exception:  # pragma: no cover
    import logging
    def configurar_logger(name: str):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)
    def intervalo_a_segundos(x: str) -> int:
        m = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400}
        return m.get(x, 60)


log = configurar_logger("binance_ws_lite")
UTC = timezone.utc


class InactividadTimeoutError(Exception):
    """Señala que el WebSocket se cerró por falta de datos."""
    pass


_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
INTERVALOS_VALIDOS = {"1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"}

# Ajustables por env si quieres (sin import de os para mantenerlo liviano)
PING_INTERVAL = 30
PING_TIMEOUT = 10
OPEN_TIMEOUT = 10
CLOSE_TIMEOUT = 5
MAX_BACKOFF = 60.0


def _norm(symbol: str) -> str:
    return symbol.replace("/", "").lower()


def _parse_kline(payload: dict, symbol_hint: Optional[str] = None) -> Optional[dict]:
    # payload puede ser {e: 'kline', k: {...}} o data anidada en combinado
    try:
        if payload.get("e") != "kline":
            return None
        k = payload["k"]
        if not k.get("x"):  # solo vela cerrada
            return None
        return {
            "symbol": symbol_hint or payload.get("s"),
            "timestamp": int(k["t"]),
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "is_closed": True,
        }
    except Exception:
        return None


async def _run_ws(url: str, on_message: Callable[[str], Awaitable[None]], *, mensaje_timeout: int) -> None:
    backoff = 1.0
    first = True
    while True:
        try:
            if first:
                await asyncio.sleep(random.random())  # jitter inicial
                first = False
            async with websockets.connect(
                url,
                open_timeout=OPEN_TIMEOUT,
                close_timeout=CLOSE_TIMEOUT,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                max_size=2 ** 20,
                max_queue=0,
                ssl=_SSL_CONTEXT,
            ) as ws:
                log.info(f"🔌 Conectado {url} @ {datetime.now(UTC).isoformat()}")
                backoff = 1.0
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=mensaje_timeout)
                    except asyncio.TimeoutError:
                        raise InactividadTimeoutError(f"Sin datos en {mensaje_timeout}s")
                    await on_message(msg)
        except asyncio.CancelledError:
            raise
        except InactividadTimeoutError as e:
            log.warning(f"⏰ {e}. Reintentando…")
        except ConnectionClosed as e:
            log.warning(f"🚪 WS cerrado — code={e.code} reason={e.reason}")
        except Exception as e:
            log.warning(f"❌ Error WS: {e}")
        # backoff exponencial simple
        await asyncio.sleep(backoff)
        backoff = min(MAX_BACKOFF, backoff * 2)


async def escuchar_velas(
    symbol: str,
    intervalo: str,
    callback: Callable[[dict], Awaitable[None]],
    last_message: Optional[Dict[str, float]] = None,
    tiempo_maximo: Optional[int] = None,
    ping_interval: Optional[int] = None,
    cliente=None,
    mensaje_timeout: Optional[int] = None,
    backpressure: bool = False,
    ultimo_timestamp: Optional[int] = None,
    ultimo_cierre: Optional[float] = None,
) -> None:
    """Escucha velas cerradas de `symbol` y llama `callback(candle)` por cada una."""
    if "/" not in symbol:
        raise ValueError("Símbolo inválido (falta '/'): " + symbol)
    if intervalo not in INTERVALOS_VALIDOS:
        raise ValueError("Intervalo inválido: " + intervalo)

    url = f"wss://stream.binance.com:9443/ws/{_norm(symbol)}@kline_{intervalo}"
    intervalo_ms = intervalo_a_segundos(intervalo) * 1000
    mensaje_timeout = mensaje_timeout or max(2 * intervalo_a_segundos(intervalo), 90)

    async def on_message(msg: str) -> None:
        try:
            data = json.loads(msg)
        except Exception:
            return
        vela = _parse_kline(data, symbol_hint=symbol)
        if not vela:
            return
        # Filtro de progreso temporal y alineación básica
        ts = vela["timestamp"]
        if ultimo_timestamp is not None and ts <= ultimo_timestamp:
            return
        if ts % intervalo_ms != 0:
            return
        await callback(vela)

    await _run_ws(url, on_message, mensaje_timeout=mensaje_timeout)


async def escuchar_velas_combinado(
    symbols: List[str],
    intervalo: str,
    handlers: Dict[str, Callable[[dict], Awaitable[None]]],
    last_message: Optional[Dict[str, float]] = None,
    tiempo_maximo: Optional[int] = None,
    ping_interval: Optional[int] = None,
    cliente=None,
    mensaje_timeout: Optional[int] = None,
    backpressure: bool = False,
    ultimos: Optional[Dict[str, Dict[str, float]]] = None,
) -> None:
    """Escucha velas cerradas de múltiples símbolos (stream combinado)."""
    if not symbols:
        raise ValueError("Debe proporcionarse al menos un símbolo")
    if intervalo not in INTERVALOS_VALIDOS:
        raise ValueError("Intervalo inválido: " + intervalo)
    norm_map = {_norm(s): s for s in symbols}
    for s in symbols:
        if "/" not in s:
            raise ValueError("Símbolo inválido: " + s)
        if s not in handlers:
            raise ValueError("Falta handler para " + s)

    streams = "/".join(f"{_norm(s)}@kline_{intervalo}" for s in symbols)
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    intervalo_ms = intervalo_a_segundos(intervalo) * 1000
    mensaje_timeout = mensaje_timeout or max(5 * intervalo_a_segundos(intervalo), 300)
    ult_ts = {s: (ultimos.get(s, {}).get("ultimo_timestamp") if ultimos else None) for s in symbols}

    async def on_message(msg: str) -> None:
        try:
            data = json.loads(msg)
        except Exception:
            return
        # Formato combinado: {stream: "btcusdt@kline_1m", data: {...}}
        payload = data.get("data", {})
        stream = data.get("stream", "")
        norm = stream.split("@")[0]
        sym = norm_map.get(norm)
        if not sym:
            return
        vela = _parse_kline(payload, symbol_hint=sym)
        if not vela:
            return
        ts = vela["timestamp"]
        if ult_ts.get(sym) is not None and ts <= ult_ts[sym]:
            return
        if ts % intervalo_ms != 0:
            return
        ult_ts[sym] = ts
        await handlers[sym](vela)

    await _run_ws(url, on_message, mensaje_timeout=mensaje_timeout)

