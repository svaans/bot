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
    "BOT_ORDERS_RETRY_SCHEDULED_TOTAL",
    "BOT_TRADER_PURGE_RUNS_TOTAL",
    "BOT_DATAFEED_WS_FAILURES_TOTAL",
    "BOT_LIMIT_ORDERS_SUBMITTED_TOTAL",
    "BOT_BACKFILL_WINDOW_RUNS_TOTAL",
    "CAPITAL_REGISTERED_GAUGE",
    "CAPITAL_CONFIGURED_GAUGE",
    "CAPITAL_REGISTERED_TOTAL",
    "CAPITAL_CONFIGURED_TOTAL",
    "CAPITAL_DIVERGENCE_ABSOLUTE",
    "CAPITAL_DIVERGENCE_RATIO",
    "CAPITAL_DIVERGENCE_THRESHOLD",
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

BOT_ORDERS_RETRY_SCHEDULED_TOTAL = Counter(
    "bot_orders_retry_scheduled_total",
    "Reintentos de persistencia de órdenes programados por símbolo",
    ["symbol"],
)

BOT_TRADER_PURGE_RUNS_TOTAL = Counter(
    "bot_trader_purge_runs_total",
    "Ejecuciones del purgado de historial del trader por símbolo",
    ["symbol"],
)

BOT_DATAFEED_WS_FAILURES_TOTAL = Counter(
    "bot_datafeed_ws_failures_total",
    "Reinicios del DataFeed por fallos en el WebSocket",
    ["reason"],
)

BOT_LIMIT_ORDERS_SUBMITTED_TOTAL = Counter(
    "bot_limit_orders_submitted_total",
    "Órdenes limit registradas por símbolo y lado",
    ["symbol", "side"],
)

BOT_BACKFILL_WINDOW_RUNS_TOTAL = Counter(
    "bot_backfill_window_runs_total",
    "Backfills ejecutados para recalentar ventanas de historial",
    ["symbol", "timeframe"],
)


CAPITAL_REGISTERED_GAUGE = Gauge(
    "bot_capital_registered",
    "Capital disponible registrado por símbolo tras sincronización",
    ["symbol"],
)

CAPITAL_CONFIGURED_GAUGE = Gauge(
    "bot_capital_configured",
    "Capital teórico/configurado por símbolo",
    ["symbol"],
)

CAPITAL_REGISTERED_TOTAL = Gauge(
    "bot_capital_registered_total",
    "Capital registrado total sumado entre símbolos",
)

CAPITAL_CONFIGURED_TOTAL = Gauge(
    "bot_capital_configured_total",
    "Capital teórico total configurado",
)

CAPITAL_DIVERGENCE_ABSOLUTE = Gauge(
    "bot_capital_divergence_absolute",
    "Diferencia absoluta entre capital registrado y teórico",
)

CAPITAL_DIVERGENCE_RATIO = Gauge(
    "bot_capital_divergence_ratio",
    "Relación absoluta (porcentaje) de divergencia total de capital",
)

CAPITAL_DIVERGENCE_THRESHOLD = Gauge(
    "bot_capital_divergence_threshold",
    "Umbral configurado para divergencia relativa de capital",
)
