"""Métricas de observabilidad expuestas para las estrategias."""
from __future__ import annotations

from core.utils.metrics_compat import Counter

__all__ = ["SIGNALS_CONFLICT", "SIGNALS_CONFLICT_RESOLVED"]

SIGNALS_CONFLICT = Counter(
    "strategy_signals_conflict_total",
    "Conflictos detectados entre señales alcistas y bajistas",
    ["symbol"],
)

SIGNALS_CONFLICT_RESOLVED = Counter(
    "strategy_signals_conflict_resolved_total",
    "Conflictos resueltos tras aplicar convicción de estrategias",
    ["symbol", "resolution"],
)
