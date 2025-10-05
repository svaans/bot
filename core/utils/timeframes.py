"""Utilidades para manejar timeframes y alineaciones temporales."""
from __future__ import annotations

from datetime import datetime, timezone
__all__ = ["TF_MINUTES", "floor_to_tf", "tf_to_ms", "ensure_utc"]

TF_MINUTES: dict[str, int] = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "45m": 45,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
    "3d": 4320,
    "1w": 10080,
}

UTC = timezone.utc


def ensure_utc(dt: datetime) -> datetime:
    """Devuelve ``dt`` con zona horaria UTC."""

    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def tf_to_ms(timeframe: str) -> int:
    """Convierte ``timeframe`` (por ejemplo ``"5m"``) a milisegundos."""

    tf = timeframe.lower().strip()
    minutes = TF_MINUTES.get(tf)
    if minutes is None:
        raise ValueError(f"Timeframe no soportado: {timeframe}")
    return minutes * 60_000


def floor_to_tf(dt_utc: datetime, timeframe_minutes: int | None) -> datetime:
    """Ajusta ``dt_utc`` hacia abajo al múltiplo más cercano de ``timeframe_minutes``."""

    if timeframe_minutes is None or timeframe_minutes <= 0:
        raise ValueError("timeframe_minutes debe ser positivo")

    dt = ensure_utc(dt_utc)
    total_seconds = int(dt.timestamp())
    step_seconds = timeframe_minutes * 60
    floored = (total_seconds // step_seconds) * step_seconds
    return datetime.fromtimestamp(floored, tz=UTC)