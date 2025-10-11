"""Métricas de observabilidad expuestas para las estrategias y servicios."""
from __future__ import annotations

from core.utils.metrics_compat import Counter, Gauge

__all__ = [
    "SIGNALS_CONFLICT",
    "SIGNALS_CONFLICT_RESOLVED",
    "NOTIFICATIONS_TOTAL",
    "NOTIFICATIONS_RETRY",
    "EMOTIONAL_STATE_SCORE",
    "EMOTIONAL_STATE_TRANSITIONS",
    "EMOTIONAL_RISK_GAUGE",
    "EMOTIONAL_STREAK_GAUGE",
]

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


NOTIFICATIONS_TOTAL = Counter(
    "bot_notifications_total",
    "Notificaciones emitidas hacia canales externos por resultado",
    ["channel", "result"],
)

NOTIFICATIONS_RETRY = Counter(
    "bot_notifications_retry_total",
    "Reintentos realizados al enviar notificaciones externas",
    ["channel"],
)

EMOTIONAL_STATE_SCORE = Gauge(
    "bot_emotional_state_score",
    "Puntuación numérica asociada al estado emocional calculado",
)

EMOTIONAL_STATE_TRANSITIONS = Counter(
    "bot_emotional_state_transitions_total",
    "Transiciones entre estados emocionales detectados",
    ["state"],
)

EMOTIONAL_RISK_GAUGE = Gauge(
    "bot_emotional_risk",
    "Medición del riesgo acumulado utilizado para el estado emocional",
)

EMOTIONAL_STREAK_GAUGE = Gauge(
    "bot_emotional_streak",
    "Rachas consecutivas de ganancias y pérdidas registradas",
    ["type"],
)
