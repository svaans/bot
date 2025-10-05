"""Streams simulados de velas para entornos offline."""
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional

__all__ = [
    "InactividadTimeoutError",
    "escuchar_velas",
    "escuchar_velas_combinado",
]

UTC = timezone.utc


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


async def _call_handler(handler: Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None], candle: Dict[str, Any]) -> None:
    result = handler(candle)
    if asyncio.iscoroutine(result):
        await result


def _init_state(symbol: str, intervalo: str, ultimo_timestamp: Optional[int], ultimo_cierre: Optional[float]) -> _StreamState:
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    step_ms = int(_intervalo_segundos(intervalo) * 1000)
    if ultimo_timestamp:
        base_ts = int(ultimo_timestamp)
    else:
        # Alinea el timestamp al cierre más reciente completado para evitar
        # discrepancias con consumidores que validan la integridad del stream.
        base_ts = max(0, ((now_ms // step_ms) - 1) * step_ms)
    close = ultimo_cierre if ultimo_cierre is not None else 100.0
    return _StreamState(symbol, intervalo, base_ts, float(close))


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
    """Genera velas artificiales y las envía al ``handler``.

    Reglas clave para la simulación:

    * Si ``ultimo_timestamp`` está presente, se emite **solo** la vela
      inmediatamente posterior a ese cierre para evitar backfills agresivos.
    * Dos velas con el mismo ``close_time`` no se entregan más de una vez.
    * El loop principal mantiene un ritmo constante controlado por
      ``intervalo``.
    """

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
        # Cede el control al event loop para permitir cancelaciones inmediatas
        # antes de iniciar el bucle productor recurrente. Esto evita que, en
        # entornos de test donde ``asyncio.sleep`` está parcheado a dormir 0s,
        # se emitan velas adicionales antes de que la cancelación se procese.
        await asyncio.sleep(0)
        # Además, esperamos un intervalo completo cancelable. Así damos margen
        # para que un consumidor que cancele tras recibir el backfill detenga
        # la tarea antes de que se genere una nueva vela.
        try:
            await asyncio.sleep(intervalo_segundos)
        except asyncio.CancelledError:
            raise

    while True:
        try:
            await asyncio.sleep(intervalo_segundos)
        except asyncio.CancelledError:
            raise

        current_task = asyncio.current_task()
        if current_task is not None and current_task.cancelling():
            raise asyncio.CancelledError

        current_close_time += paso_ms
        state.ultimo_ts = current_close_time
        ruido = random.uniform(-0.3, 0.3)
        open_price = max(1.0, state.ultimo_close + ruido)
        close_price = max(1.0, open_price + random.uniform(-0.2, 0.2))
        high_price = max(open_price, close_price) + random.uniform(0.0, 0.1)
        low_price = min(open_price, close_price) - random.uniform(0.0, 0.1)
        volume = max(10.0, random.uniform(25.0, 35.0))

        candle = {
            "event_time": int(time.time() * 1000),
            "symbol": symbol,
            "intervalo": intervalo,
            "open_time": current_close_time,
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


async def escuchar_velas_combinado(
    symbols: Iterable[str],
    intervalo: str,
    handler: Callable[[str, Dict[str, Any]], Awaitable[None]] | Callable[[str, Dict[str, Any]], None] | Mapping[str, Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None]],
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
    """Genera streams simultáneos para múltiples símbolos."""

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
                    _unused,
                    timeout_inactividad,
                    _heartbeat,
                    cliente=cliente,
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
    return {
        "event_time": int(time.time() * 1000),
        "symbol": symbol,
        "intervalo": intervalo,
        "open_time": close_time,
        "close_time": close_time,
        "open": round(base, 6),
        "high": high,
        "low": low,
        "close": round(base, 6),
        "volume": 30.0,
        "is_closed": True,
    }
