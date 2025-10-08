"""Utilidades compartidas para TraderLite y Trader."""
from __future__ import annotations

import asyncio
import contextlib
import inspect
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
    last_bar_ts: Optional[float],
    now_ts: Optional[float],
) -> str:
    """Determina el motivo por el cual no se debe evaluar la estrategia aún."""

    if timeframe is None:
        return "timeframe_none"

    if min_bars > 0 and buf_len < min_bars:
        return "warmup"

    tf_secs = tf_seconds(timeframe)
    if (
        tf_secs > 0
        and last_bar_ts is not None
        and now_ts is not None
        and last_bar_ts > 0
    ):
        elapsed_secs = now_ts - last_bar_ts
        if elapsed_secs < 0:
            reason = "bar_in_future"
            if abs(elapsed_secs) > (tf_secs * 3):
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
                            "now_ts": now_ts,
                            "last_bar_ts": last_bar_ts,
                            "elapsed_secs": elapsed_secs,
                            "elapsed_ms": int(elapsed_secs * 1000),
                            "interval_secs": tf_secs,
                            "interval_ms": tf_secs * 1000,
                        }
                    ),
                )
            return reason

        if elapsed_secs < tf_secs:
            if _should_log(f"waiting_close:{symbol}:{timeframe}", every=5.0):
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
                            "now_ts": now_ts,
                            "last_bar_ts": last_bar_ts,
                            "elapsed_secs": elapsed_secs,
                            "elapsed_ms": int(elapsed_secs * 1000),
                            "interval_secs": tf_secs,
                            "interval_ms": tf_secs * 1000,
                        }
                    ),
                )
            return "waiting_close"

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
