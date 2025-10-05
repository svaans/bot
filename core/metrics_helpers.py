"""Helpers seguros para interactuar con métricas Prometheus.

Estos wrappers evitan que un desajuste de etiquetas tumbe el flujo
operativo. Si las etiquetas no coinciden con la definición del métrico,
se hace fallback a la variante sin labels y se registra una advertencia.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("metrics")


def safe_inc(metric: Any, **labels: Any) -> None:
    """Incrementa un contador tolerando desajustes de labels."""
    try:
        if labels:
            metric.labels(**labels).inc()
        else:
            metric.inc()
    except TypeError as exc:
        logger.warning(
            "metrics_label_mismatch; fallback no-label",
            extra={
                "metric": getattr(metric, "_name", getattr(metric, "name", str(metric))),
                "labels": labels,
                "error": str(exc),
            },
        )
        try:
            metric.inc()
        except Exception:
            pass


def safe_set(gauge: Any, value: float, **labels: Any) -> None:
    """Actualiza un gauge tolerando desajustes de labels."""
    try:
        if labels:
            gauge.labels(**labels).set(value)
        else:
            gauge.set(value)
    except TypeError as exc:
        logger.warning(
            "metrics_label_mismatch; fallback no-label",
            extra={
                "metric": getattr(gauge, "_name", getattr(gauge, "name", str(gauge))),
                "labels": labels,
                "error": str(exc),
            },
        )
        try:
            gauge.set(value)
        except Exception:
            pass