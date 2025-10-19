"""Métricas de observabilidad expuestas para las estrategias y servicios."""
from __future__ import annotations

from core.utils.metrics_compat import Counter, Gauge, Histogram

__all__ = [
    "SIGNALS_CONFLICT",
    "SIGNALS_CONFLICT_RESOLVED",
    "NOTIFICATIONS_TOTAL",
    "NOTIFICATIONS_RETRY",
    "BOT_OPERATIONAL_MODE",
    "BOT_OPERATIONAL_MODE_TRANSITIONS_TOTAL",
    "BOT_MODO_REAL",
    "EMOTIONAL_STATE_SCORE",
    "EMOTIONAL_STATE_TRANSITIONS",
    "EMOTIONAL_RISK_GAUGE",
    "EMOTIONAL_STREAK_GAUGE",
    "BOT_ORDERS_RETRY_SCHEDULED_TOTAL",
    "BOT_TRADER_PURGE_RUNS_TOTAL",
    "BOT_DATAFEED_WS_FAILURES_TOTAL",
    "BOT_LIMIT_ORDERS_SUBMITTED_TOTAL",
    "BOT_BACKFILL_WINDOW_RUNS_TOTAL",
    "METRIC_EXPORT_FAILURES_TOTAL",
    "REPORT_IO_ERRORS_TOTAL",
    "CAPITAL_REGISTERED_GAUGE",
    "CAPITAL_CONFIGURED_GAUGE",
    "CAPITAL_REGISTERED_TOTAL",
    "CAPITAL_CONFIGURED_TOTAL",
    "CAPITAL_DIVERGENCE_ABSOLUTE",
    "CAPITAL_DIVERGENCE_RATIO",
    "CAPITAL_DIVERGENCE_THRESHOLD",
    "METRIC_WRITE_LATENCY_SECONDS",
    "REPORT_GENERATION_DURATION_SECONDS",
    "AUDITORIA_WRITES_TOTAL",
    "AUDITORIA_WRITE_LATENCY_SECONDS",
    "AUDITORIA_ERRORS_TOTAL",
    "AUDITORIA_LOCK_WAIT_SECONDS",
    "AUDITORIA_LOCK_CONTENTION_TOTAL",
    "SYNERGY_CAP_SATURATION",
    "SYNERGY_CAP_DISPERSION",
    "SYNERGY_CAP_P90",
    "HOT_RELOAD_RESTARTS_TOTAL",
    "HOT_RELOAD_DEBOUNCE_SECONDS",
    "HOT_RELOAD_BACKEND",
    "HOT_RELOAD_SCAN_DURATION_SECONDS",
    "HOT_RELOAD_ERRORS_TOTAL",
    "CONTEXT_LAST_UPDATE_SECONDS",
    "CONTEXT_SCORE_DISTRIBUTION",
    "CONTEXT_UPDATE_LATENCY_SECONDS",
    "CONTEXT_PARSING_ERRORS_TOTAL",
    "CONTEXT_VOLUME_EXTREME_TOTAL",
]

BOT_OPERATIONAL_MODE = Gauge(
    "bot_operational_mode",
    "Estado actual del modo operativo (1=activo)",
    ["mode"],
)

BOT_OPERATIONAL_MODE_TRANSITIONS_TOTAL = Counter(
    "bot_operational_mode_transitions_total",
    "Total de transiciones entre modos operativos",
    ["from_mode", "to_mode", "trigger"],
)

BOT_MODO_REAL = Gauge(
    "bot_modo_real",
    "Indicador binario del modo real activo",
)

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


METRIC_EXPORT_FAILURES_TOTAL = Counter(
    "metric_export_failures_total",
    "Errores al exportar registros de métricas al almacenamiento persistente",
    ["operation"],
)


REPORT_IO_ERRORS_TOTAL = Counter(
    "report_io_errors_total",
    "Errores de entrada/salida al generar o persistir reportes",
    ["operation"],
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


AUDITORIA_WRITES_TOTAL = Counter(
    "auditoria_writes_total",
    "Total de escrituras de auditoría persistidas por formato",
    ["formato"],
)

AUDITORIA_WRITE_LATENCY_SECONDS = Histogram(
    "auditoria_write_latency_seconds",
    "Latencia de persistencia de registros de auditoría por formato",
    ["formato"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

AUDITORIA_ERRORS_TOTAL = Counter(
    "auditoria_errors_total",
    "Errores registrados al intentar persistir auditorías por formato",
    ["formato"],
)

AUDITORIA_LOCK_WAIT_SECONDS = Histogram(
    "auditoria_lock_wait_seconds",
    "Tiempo de espera para adquirir el candado de persistencia de auditoría",
    ["formato"],
    buckets=(0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

AUDITORIA_LOCK_CONTENTION_TOTAL = Counter(
    "auditoria_lock_contention_total",
    "Incidencias donde la adquisición del candado de auditoría excedió el umbral permitido",
    ["formato"],
)


SYNERGY_CAP_SATURATION = Gauge(
    "strategy_synergy_cap_saturation",
    "Fracción de evaluaciones de sinergia que alcanzan el cap configurado",
)

SYNERGY_CAP_DISPERSION = Gauge(
    "strategy_synergy_cap_dispersion",
    "Dispersión (desviación estándar poblacional) de sinergias observadas",
)

SYNERGY_CAP_P90 = Gauge(
    "strategy_synergy_cap_p90",
    "Percentil 90 de sinergia sin limitar observado en la ventana de seguimiento",
)


HOT_RELOAD_RESTARTS_TOTAL = Counter(
    "hot_reload_restarts_total",
    "Reinicios solicitados por el mecanismo de hot reload por motivo",
    ["reason"],
)

HOT_RELOAD_DEBOUNCE_SECONDS = Counter(
    "hot_reload_debounce_seconds",
    "Segundos acumulados en ventanas de espera (debounce) del hot reload",
    ["kind"],
)

HOT_RELOAD_BACKEND = Counter(
    "hot_reload_backend",
    "Veces que se inició el observador de hot reload por tipo de backend",
    ["mode"],
)

HOT_RELOAD_SCAN_DURATION_SECONDS = Histogram(
    "hot_reload_scan_duration_seconds",
    "Duración observada al programar escaneos iniciales por backend",
    ["mode"],
)

HOT_RELOAD_ERRORS_TOTAL = Counter(
    "hot_reload_errors_total",
    "Errores detectados en el flujo del mecanismo de hot reload",
    ["stage"],
)


CONTEXT_LAST_UPDATE_SECONDS = Gauge(
    "context_last_update_seconds",
    "Timestamp del último evento de contexto recibido por símbolo",
    ["symbol"],
)

_CONTEXT_SCORE_BUCKETS = (
    -10.0,
    -5.0,
    -2.5,
    -1.0,
    -0.5,
    -0.25,
    -0.1,
    -0.05,
    0.0,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)

CONTEXT_SCORE_DISTRIBUTION = Histogram(
    "context_score_distribution",
    "Distribución de puntajes de contexto observados por símbolo",
    ["symbol"],
    buckets=_CONTEXT_SCORE_BUCKETS,
)

CONTEXT_UPDATE_LATENCY_SECONDS = Histogram(
    "context_update_latency_seconds",
    "Latencia entre el timestamp del evento y su procesamiento por fuente",
    ["symbol", "source"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

CONTEXT_PARSING_ERRORS_TOTAL = Counter(
    "context_parsing_errors_total",
    "Errores detectados durante el parseo de mensajes de contexto",
    ["symbol", "stage"],
)

CONTEXT_VOLUME_EXTREME_TOTAL = Counter(
    "context_volume_extreme_total",
    "Eventos donde el volumen se consideró extremo según la mediana reciente",
    ["symbol", "source"],
)


METRIC_WRITE_LATENCY_SECONDS = Histogram(
    "metric_write_latency_seconds",
    "Latencia en segundos de operaciones de escritura a disco",
    ["operation", "extension"],
)

REPORT_GENERATION_DURATION_SECONDS = Histogram(
    "report_generation_duration_seconds",
    "Duración en segundos de la generación de reportes diarios",
    ["status"],
)