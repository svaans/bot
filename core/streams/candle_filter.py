"""Filtros para velas provenientes de streams en tiempo real."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, MutableMapping, Sequence

__all__ = ["CandleFilter"]


@dataclass(slots=True)
class CandleFilter:
    """Filtra velas duplicadas o inconsistentes."""

    ultimo_timestamp: int | None = None
    ultimo_close: float | None = None
    estadisticas: MutableMapping[str, int] = field(default_factory=lambda: {"aceptadas": 0, "rechazadas": 0})

    def _extract_timestamp(self, candle: Mapping[str, Any] | Sequence[Any]) -> int | None:
        if isinstance(candle, Mapping):
            for key in ("close_time", "open_time", "timestamp", "event_time"):
                value = candle.get(key)
                if value is not None:
                    try:
                        return int(float(value))
                    except (TypeError, ValueError):
                        return None
            return None
        if not candle:
            return None
        try:
            return int(float(candle[0]))
        except (TypeError, ValueError):
            return None

    def accept(self, candle: Mapping[str, Any] | Sequence[Any]) -> bool:
        """Evalúa si la vela debe procesarse."""

        ts = self._extract_timestamp(candle)
        if ts is None:
            self.estadisticas["rechazadas"] += 1
            return False
        if self.ultimo_timestamp is not None and ts <= self.ultimo_timestamp:
            self.estadisticas["rechazadas"] += 1
            return False
        self.ultimo_timestamp = ts
        if isinstance(candle, Mapping):
            close = candle.get("close")
            if isinstance(close, (int, float)):
                self.ultimo_close = float(close)
        elif len(candle) > 4:
            try:
                self.ultimo_close = float(candle[4])
            except (TypeError, ValueError):
                pass
        self.estadisticas["aceptadas"] += 1
        return True

    def reset(self) -> None:
        """Reinicia el estado interno."""

        self.ultimo_timestamp = None
        self.ultimo_close = None
        self.estadisticas.update({"aceptadas": 0, "rechazadas": 0})