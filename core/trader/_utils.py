"""Utilidades compartidas para TraderLite y Trader."""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import math
import os
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional

from core.streams.candle_filter import CandleFilter
from core.utils import configurar_logger, _should_log
from core.utils.log_utils import safe_extra

__all__ = [
    "_is_awaitable",
    "_maybe_await",
    "_silence_task_result",
    "_max_buffer_velas",
    "_max_estrategias_buffer",
    "tf_seconds",
    "_normalize_timestamp",
    "_reason_none",
    "EstadoSimbolo",
]


log = configurar_logger("trader_modular", modo_silencioso=True)


def _is_awaitable(x: Any) -> bool:
    """Devuelve ``True`` si el objeto puede esperarse con ``await``."""

    return inspect.isawaitable(x) or asyncio.isfuture(x)


async def _maybe_await(x: Any):
    """Espera el resultado si es awaitable; de lo contrario, lo devuelve directo."""

    if _is_awaitable(x):
        return await x
    return x


def _silence_task_result(task: asyncio.Task) -> None:
    """Consume resultados/errores de tareas lanzadas en segundo plano."""

    with contextlib.suppress(Exception):
        task.result()


def _max_buffer_velas() -> int:
    """Obtiene el tamaño máximo del buffer de velas desde variables de entorno."""

    return int(os.getenv("MAX_BUFFER_VELAS", "300"))


def _max_estrategias_buffer() -> int:
    """Determina el tamaño máximo del buffer para resultados de estrategias."""

    return int(os.getenv("MAX_ESTRATEGIAS_BUFFER", str(_max_buffer_velas())))


_TF_UNIT_SECONDS: Dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
    "w": 7 * 24 * 60 * 60,
}


def tf_seconds(timeframe: Optional[str]) -> int:
    """Convierte un timeframe textual (``1m``, ``5m``, ``1h``) a segundos."""

    if not timeframe:
        return 0

    tf = str(timeframe).strip().lower()
    if not tf:
        return 0

    if tf.endswith("ms"):
        try:
            return int(float(tf[:-2]) / 1000.0)
        except ValueError:
            return 0

    unit = tf[-1]
    factor = _TF_UNIT_SECONDS.get(unit)
    if factor is None:
        return 0

    value_part = tf[:-1] or "0"
    try:
        value = float(value_part)
    except ValueError:
        return 0

    return int(value * factor)


def _normalize_timestamp(value: Any) -> Optional[float]:
    """Normaliza un timestamp que puede venir en milisegundos o segundos."""

    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    if numeric >= 1e11:
        return numeric / 1000.0
    return numeric


def _reason_none(
    symbol: str,
    timeframe: Optional[str],
    buf_len: int,
    min_bars: int,
    bar_open_ts: Optional[float],
    bar_event_ts: Optional[float],
    *,
    interval_secs: Optional[int] = None,
    skew_allow: float = 1.5,
    bar_close_ts: Optional[float] = None,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Determina el motivo por el cual no se debe evaluar la estrategia aún.

    Parameters
    ----------
    bar_open_ts:
        Marca de tiempo (en segundos) del inicio de la vela, normalizada al
        intervalo del timeframe.
    bar_event_ts:
        Marca de tiempo (en segundos) reportada por el exchange para el cierre
        o evento de la vela. Se utiliza como referencia temporal principal para
        evitar depender del reloj local.
    interval_secs:
        Duración del timeframe en segundos. Si no se especifica, se intentará
        inferir a partir del ``timeframe`` textual.
    skew_allow:
        Tolerancia (en segundos) para compensar jitter en la recepción de
        mensajes.
    bar_close_ts:
        Marca de tiempo de cierre reportada (si difiere del evento).
    context:
        Diccionario opcional que será rellenado con los valores normalizados
        utilizados en la toma de decisión (open, close, event, elapsed, etc.).
    """

    if timeframe is None:
        return "timeframe_none"

    if min_bars > 0 and buf_len < min_bars:
        return "warmup"

    tf_secs = int(interval_secs or tf_seconds(timeframe))
    skew = max(0.0, float(skew_allow))

    ctx: Dict[str, Any] = {
        "interval_secs": tf_secs,
        "skew_allow_secs": skew,
        "bar_open_ts": None,
        "bar_event_ts": None,
        "bar_close_ts": None,
        "elapsed_secs": None,
    }

    def _safe_float(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    open_ts = _safe_float(bar_open_ts)
    event_ts = _safe_float(bar_event_ts)
    close_ts = _safe_float(bar_close_ts)
    if close_ts is None:
        close_ts = event_ts

    if open_ts is not None and tf_secs > 0:
        open_ts = (math.floor(open_ts / tf_secs)) * tf_secs

    ctx["bar_open_ts"] = open_ts
    ctx["bar_event_ts"] = event_ts
    ctx["bar_close_ts"] = close_ts

    if open_ts is None or event_ts is None or tf_secs <= 0:
        if context is not None:
            context.update(ctx)
        return "ready"

    elapsed_secs = event_ts - open_ts
    ctx["elapsed_secs"] = elapsed_secs

    if elapsed_secs < -skew:
        reason = "bar_in_future"
        if abs(elapsed_secs) >= tf_secs:
            reason = "bar_ts_out_of_range"
        if _should_log(f"{reason}:{symbol}:{timeframe}", every=10.0):
            log.warning(
                "[%s] Timestamp de vela fuera de rango (%s)",
                symbol,
                reason,
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "reason": reason,
                        "buffer_len": buf_len,
                        "min_needed": min_bars,
                        "bar_open_ts": open_ts,
                        "bar_close_ts": close_ts,
                        "event_ts": event_ts,
                        "elapsed_secs": elapsed_secs,
                        "elapsed_ms": int(elapsed_secs * 1000),
                        "interval_secs": tf_secs,
                        "interval_ms": tf_secs * 1000,
                        "skew_allow_secs": skew,
                        "skew_allow_ms": int(skew * 1000),
                    }
                ),
            )
        if context is not None:
            context.update(ctx)
        return reason

    if (elapsed_secs + skew) < tf_secs:
        if _should_log(f"waiting_close:{symbol}:{timeframe}", every=5.0):
            remaining = tf_secs - elapsed_secs
            log.debug(
                "[%s] Esperando cierre de vela (elapsed < intervalo)",
                symbol,
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "reason": "waiting_close",
                        "buffer_len": buf_len,
                        "min_needed": min_bars,
                        "bar_open_ts": open_ts,
                        "bar_close_ts": close_ts,
                        "event_ts": event_ts,
                        "elapsed_secs": elapsed_secs,
                        "elapsed_ms": int(elapsed_secs * 1000),
                        "remaining_secs": remaining,
                        "remaining_ms": int(remaining * 1000),
                        "interval_secs": tf_secs,
                        "interval_ms": tf_secs * 1000,
                        "skew_allow_secs": skew,
                        "skew_allow_ms": int(skew * 1000),
                    }
                ),
            )
        if context is not None:
            context.update(ctx)
        return "waiting_close"

    if context is not None:
        context.update(ctx)

    return "ready"


@dataclass
class EstadoSimbolo:
    """Estado mínimo por símbolo."""

    buffer: Deque[dict] = field(default_factory=lambda: deque(maxlen=_max_buffer_velas()))
    estrategias_buffer: Deque[dict] = field(
        default_factory=lambda: deque(maxlen=_max_estrategias_buffer())
    )
    ultimo_timestamp: Optional[int] = None
    candle_filter: CandleFilter = field(default_factory=CandleFilter)
    indicadores_cache: Dict[str, dict[str, float | None]] = field(default_factory=dict)
    fastpath_mode: str = "normal"
