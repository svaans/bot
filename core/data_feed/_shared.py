from __future__ import annotations

"""Componentes compartidos para el paquete ``core.data_feed``."""

from datetime import timezone
from enum import Enum
from typing import Any

from core.utils.logger import configurar_logger

COMBINED_STREAM_KEY = "__combined__"
UTC = timezone.utc


class ConsumerState(Enum):
    """Estados posibles del consumidor de velas."""

    STARTING = 0
    HEALTHY = 1
    STALLED = 2
    LOOP = 3


def _safe_float(value: Any, default: float) -> float:
    """Devuelve ``value`` como ``float`` o ``default`` si no es válido."""

    try:
        v = float(value)
        if v != v:  # NaN
            return default
        return v
    except Exception:
        return default


def _safe_int(value: Any, default: int) -> int:
    """Devuelve ``value`` como ``int`` o ``default`` si falla la conversión."""

    try:
        return int(value)
    except Exception:
        return default


log = configurar_logger("datafeed", modo_silencioso=False)

__all__ = [
    "COMBINED_STREAM_KEY",
    "ConsumerState",
    "UTC",
    "_safe_float",
    "_safe_int",
    "log",
]