"""Métricas de observabilidad expuestas para las estrategias."""
from __future__ import annotations

from core.utils.metrics_compat import Counter

__all__ = ["SIGNALS_CONFLICT"]

SIGNALS_CONFLICT = Counter(
    "strategy_signals_conflict_total",
    "Conflictos detectados entre señales alcistas y bajistas",
    ["symbol"],
)
