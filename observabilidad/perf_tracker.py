"""Utilidades para agregar métricas de rendimiento en ventanas deslizantes."""
from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional


@dataclass
class _WindowSnapshot:
    """Resumen estadístico de una ventana de observaciones."""

    window: int
    mean: float
    p95: float
    p99: float
    max: float
    total_count: int
    total_mean: float
    total_max: float


class RollingStats:
    """Mantiene estadísticas acumuladas y de ventana fija."""

    def __init__(self, window: int = 512) -> None:
        self._window: Deque[float] = deque(maxlen=window)
        self._total_count: int = 0
        self._total_sum: float = 0.0
        self._total_max: float = 0.0

    def observe(self, value: float) -> None:
        """Registra ``value`` en la ventana y en los acumulados."""

        self._window.append(value)
        self._total_count += 1
        self._total_sum += value
        if value > self._total_max:
            self._total_max = value

    def snapshot(self) -> Optional[_WindowSnapshot]:
        """Devuelve estadísticas de la ventana actual si hay datos."""

        if not self._window:
            return None
        data = sorted(self._window)
        window = len(data)
        mean = sum(data) / window
        p95 = data[min(window - 1, max(0, int(0.95 * (window - 1))))]
        p99 = data[min(window - 1, max(0, int(0.99 * (window - 1))))]
        max_val = data[-1]
        total_mean = self._total_sum / self._total_count if self._total_count else 0.0
        return _WindowSnapshot(
            window=window,
            mean=mean,
            p95=p95,
            p99=p99,
            max=max_val,
            total_count=self._total_count,
            total_mean=total_mean,
            total_max=self._total_max,
        )

    def reset(self) -> None:
        """Limpia la ventana (no los acumulados)."""

        self._window.clear()


class StageMetrics:
    """Agrega métricas de latencia, tiempo en cola y tamaño de backlog."""

    def __init__(
        self,
        stage: str,
        *,
        log_interval: float = 5.0,
        window: int = 512,
    ) -> None:
        self.stage = stage
        self.latency = RollingStats(window)
        self.queue_delay = RollingStats(window)
        self.queue_size = RollingStats(window)
        self._log_interval = max(0.5, log_interval)
        ahora = time.monotonic()
        self._since_start = ahora
        self._since_count = 0
        self._last_extra: Dict[str, Any] = {}

    def record(
        self,
        *,
        latency: Optional[float] = None,
        queue_delay: Optional[float] = None,
        queue_size: Optional[float] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Registra una nueva observación en la etapa."""

        if latency is not None:
            self.latency.observe(latency)
        if queue_delay is not None:
            self.queue_delay.observe(queue_delay)
        if queue_size is not None:
            self.queue_size.observe(queue_size)
            self._last_extra["queue_size_current"] = queue_size
        if extra:
            self._last_extra.update(extra)
        self._since_count += 1

    def maybe_flush(
        self,
        logger,
        *,
        extra: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ) -> None:
        """Emite un log estructurado si el intervalo se cumplió o ``force`` es ``True``."""

        if self._since_count == 0 and not force:
            return
        ahora = time.monotonic()
        if not force and (ahora - self._since_start) < self._log_interval:
            return
        payload: Dict[str, Any] = {
            "evento": "stage_metrics",
            "stage": self.stage,
            "timestamp": time.time(),
            "throughput_per_s": 0.0,
        }
        elapsed = max(ahora - self._since_start, 1e-6)
        payload["throughput_per_s"] = round(self._since_count / elapsed, 6)
        lat_snapshot = self.latency.snapshot()
        if lat_snapshot:
            payload["latency_ms"] = _serialize_snapshot(lat_snapshot)
        delay_snapshot = self.queue_delay.snapshot()
        if delay_snapshot:
            payload["queue_delay_ms"] = _serialize_snapshot(delay_snapshot)
        size_snapshot = self.queue_size.snapshot()
        if size_snapshot:
            payload["queue_size"] = _serialize_snapshot(size_snapshot)
        merged_extra: Dict[str, Any] = {}
        if self._last_extra:
            merged_extra.update(self._last_extra)
        if extra:
            merged_extra.update(extra)
        if merged_extra:
            payload.update(merged_extra)
        logger.info(json.dumps(payload, ensure_ascii=False))
        self._since_start = ahora
        self._since_count = 0
        self._last_extra.clear()

    def flush(self, logger, *, extra: Optional[Dict[str, Any]] = None) -> None:
        """Forza un volcado del estado acumulado."""

        self.maybe_flush(logger, extra=extra, force=True)


def _serialize_snapshot(snapshot: _WindowSnapshot) -> Dict[str, Any]:
    """Normaliza el ``dataclass`` en un ``dict`` listo para JSON."""

    return {
        "window": snapshot.window,
        "mean": round(snapshot.mean, 6),
        "p95": round(snapshot.p95, 6),
        "p99": round(snapshot.p99, 6),
        "max": round(snapshot.max, 6),
        "total_count": snapshot.total_count,
        "total_mean": round(snapshot.total_mean, 6),
        "total_max": round(snapshot.total_max, 6),
    }
