"""Contadores básicos de métricas para el bot."""

from __future__ import annotations

import os
import types
import sys
from collections import defaultdict
from typing import Any, Dict, Optional
from wsgiref.simple_server import WSGIServer

from core.utils.metrics_compat import (
    Counter,
    Gauge,
    Histogram,
    HAVE_PROM,
    start_wsgi_server,
)

# Fallback para registro_metrico.registrar(...)
try:
    import core.registro_metrico as _registro_metrico_module  # type: ignore
except ImportError:
    _registro_metrico_module = types.SimpleNamespace(registrar=lambda *args, **kwargs: None)  # type: ignore
    sys.modules.setdefault("core.registro_metrico", _registro_metrico_module)  # type: ignore

# Soporta tanto core.registro_metrico.registro_metrico.registrar
# como core.registro_metrico.registrar
registro_metrico = getattr(
    _registro_metrico_module,
    "registro_metrico",
    _registro_metrico_module,
)

# Fallback para alert_manager (evita romper si no existe el módulo)
try:
    from core.alertas import alert_manager  # type: ignore
except Exception:
    class _DummyAlerts:
        def record(self, *_args, **_kwargs) -> float:
            return 0.0
        def should_alert(self, *_args, **_kwargs) -> bool:
            return False
    alert_manager = _DummyAlerts()  # type: ignore

try:
    from observability.metrics import (
        BOT_ORDERS_RETRY_SCHEDULED_TOTAL as _GUARDRAIL_ORDERS_RETRY_TOTAL,
    )
except Exception:  # pragma: no cover - módulo opcional
    _GUARDRAIL_ORDERS_RETRY_TOTAL = None

from core.utils.logger import configurar_logger

log = configurar_logger("metrics")


class _NullMetric:
    """Implementación nula para objetos de métricas de Prometheus."""

    def labels(self, **_kwargs: Any) -> "_NullMetric":
        return self
    def inc(self, *args: Any, **kwargs: Any) -> None: ...
    def set(self, *args: Any, **kwargs: Any) -> None: ...
    def observe(self, *args: Any, **kwargs: Any) -> None: ...
    def time(self, *args: Any, **kwargs: Any) -> "_NullMetric":
        return self


def _metric_or_null(metric: Any, name: str) -> Any:
    """Devuelve ``metric`` o un objeto nulo si está deshabilitada."""
    if metric is None:
        debug = getattr(log, "debug", None)
        if callable(debug):
            debug("Métrica %s deshabilitada; usando NullMetric", name)
        return _NullMetric()
    return metric


# Acumuladores en memoria
_decisions: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
_orders: Dict[str, int] = defaultdict(int)
_buy_rejected_insufficient_funds: int = 0
_correlacion_btc: Dict[str, float] = {}

_velas_total: Dict[tuple[str, str], int] = defaultdict(int)
_velas_rechazadas: Dict[tuple[str, str], Dict[str, int]] = defaultdict(lambda: defaultdict(int))
_ordenes_registro_pendiente_activas: set[str] = set()

# ---- Definición de métricas ----
VELAS_DUPLICADAS = Counter(
    "candles_duplicates_total",
    "Velas duplicadas detectadas por símbolo",
    ["symbol", "timeframe"],
)
CANDLES_DUPLICADAS_RATE = Gauge(
    "candles_duplicates_rate",
    "Tasa de velas duplicadas por minuto",
    ["symbol", "timeframe"],
)

VELAS_TOTAL = Counter(
    "velas_total",
    "Velas recibidas por símbolo",
    ["symbol", "timeframe"],
)
VELAS_RECHAZADAS = Counter(
    "velas_rechazadas_total",
    "Velas rechazadas por símbolo y razón",
    ["symbol", "timeframe", "reason"],
)
ENTRADAS_RECHAZADAS_V2 = Counter(
    "procesar_vela_entradas_rechazadas_total_v2",
    "Entradas rechazadas tras validaciones finales",
    ["symbol", "timeframe", "reason"],
)
VELAS_RECHAZADAS_PCT = Gauge(
    "velas_rechazadas_pct",
    "Porcentaje de velas rechazadas por símbolo",
    ["symbol", "timeframe"],
)

FUNDING_SIGNAL_DECORATIONS_TOTAL = Counter(
    "funding_signal_decorations_total",
    "Resultados del decorador de funding aplicado a señales",
    ["symbol", "side", "outcome"],
)

WARMUP_PROGRESS = Gauge(
    "context_warmup_progress",
    "Progreso de warmup de datos",
    ["symbol"],
)

WARMUP_RESTANTE = Gauge(
    "warmup_restante",
    "Velas que faltan para evaluar",
    ["symbol", "timeframe"],
)

LAST_BAR_AGE = Gauge(
    "last_bar_age_seconds",
    "Segundos desde el cierre de la última vela",
    ["symbol", "timeframe"],
)

BACKFILL_REQUESTS_TOTAL = Counter(
    "backfill_requests_total",
    "Solicitudes de backfill por símbolo y timeframe",
    ["symbol", "timeframe", "status"],
)

BACKFILL_KLINES_FETCHED_TOTAL = Counter(
    "backfill_klines_fetched_total",
    "Número de velas obtenidas durante el backfill",
    ["symbol", "timeframe"],
)

BACKFILL_DURATION_SECONDS = Histogram(
    "backfill_duration_seconds",
    "Duración del backfill por símbolo",
    ["symbol", "timeframe"],
)

BACKFILL_GAPS_FOUND_TOTAL = Counter(
    "backfill_gaps_found_total",
    "Huecos detectados durante el backfill",
    ["symbol", "timeframe"],
)

BUFFER_SIZE_V2 = Gauge(
    "buffer_size_v2",
    "Tamaño del buffer de velas precargadas",
    ["timeframe"],
)

FEEDS_FUNDING_MISSING = Counter(
    "feeds_funding_missing_total",
    "Consultas de funding rate ausentes por símbolo y razón",
    ["symbol", "reason"],
)

FEEDS_OPEN_INTEREST_MISSING = Counter(
    "feeds_open_interest_missing_total",
    "Consultas de open interest ausentes por símbolo y razón",
    ["symbol", "reason"],
)

FEEDS_MISSING_RATE = Gauge(
    "feeds_missing_rate",
    "Tasa de feeds ausentes por minuto",
    ["symbol"],
)

# Métricas de DataFeed
WS_CONNECTED_GAUGE = Gauge(
    "datafeed_ws_connected",
    "Estado binario de la conexión WS del DataFeed (1=conectado, 0=desconectado)",
)

DATAFEED_WS_MESSAGES_TOTAL = Counter(
    "datafeed_ws_messages_total",
    "Mensajes recibidos desde Binance WebSocket por tipo",
    ["type"],
)

DATAFEED_CANDLES_ENQUEUED_TOTAL = Counter(
    "datafeed_candles_enqueued_total",
    "Velas encoladas por símbolo y timeframe",
    ["symbol", "tf"],
)

QUEUE_SIZE = Gauge(
    "datafeed_queue_size",
    "Tamaño de cola de DataFeed",
    ["symbol"],
)
CONSUMER_SKIPPED_EXPECTED_TOTAL = Counter(
    "consumer_skipped_expected_total",
    "Skips esperados del consumer del DataFeed por símbolo/timeframe",
    ["symbol", "timeframe", "reason"],
)

PRODUCER_LAG_SECONDS = Gauge(
    "datafeed_producer_lag_seconds",
    "Segundos desde el último mensaje recibido del stream para el símbolo",
    ["symbol"],
)

CONSUMER_LAG_SECONDS = Gauge(
    "datafeed_consumer_lag_seconds",
    "Segundos desde el último consumo exitoso del símbolo",
    ["symbol"],
)

INGEST_LATENCY = Histogram(
    "datafeed_ingest_latency_seconds",
    "Latencia desde recepción hasta procesamiento",
    ["symbol"],
)

DATAFEED_HANDLER_LATENCY = Histogram(
    "datafeed_handler_latency_seconds",
    "Latencia del handler de DataFeed desde el encolado hasta la ejecución",
    ["symbol", "timeframe"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

# Métricas de Trader
BARS_IN_FUTURE_TOTAL = Counter(
    "bars_in_future_total",
    "Velas descartadas por llegar con timestamps adelantados",
    ["symbol", "timeframe"],
)
BARS_OUT_OF_RANGE_TOTAL = Counter(
    "bars_out_of_range_total",
    "Velas descartadas por timestamps fuera de rango",
    ["symbol", "timeframe"],
)
CANDLES_PROCESSED_TOTAL = Counter(
    "candles_processed_total",
    "Velas cerradas procesadas por símbolo y timeframe",
    ["symbol", "timeframe"],
)
EVAL_INTENTOS_TOTAL = Counter(
    "eval_intentos_total",
    "Intentos de evaluación clasificados por etapa",
    ["symbol", "timeframe", "etapa"],
)
WAITING_CLOSE_STREAK = Gauge(
    "waiting_close_streak",
    "Racha actual de velas en espera de cierre",
    ["symbol", "timeframe"],
)
EVAL_ATTEMPTS_TOTAL = Counter(
    "eval_attempts_total",
    "Intentos de evaluación de estrategia",
    ["symbol", "timeframe"],
)
TRADER_QUEUE_SIZE = Gauge(
    "trader_queue_size",
    "Tamaño de la cola interna del Trader",
    ["symbol", "timeframe"],
)
TRADER_PIPELINE_LATENCY = Histogram(
    "trader_pipeline_latency_seconds",
    "Latencia del procesamiento de velas en el trader modular",
    ["symbol", "timeframe"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)
TRADER_PIPELINE_QUEUE_WAIT = Histogram(
    "trader_pipeline_queue_wait_seconds",
    "Tiempo que una vela espera en la cola interna del trader",
    ["symbol", "timeframe"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

TRADER_PROCESAR_VELA_CALLS_TOTAL = Counter(
    "trader_procesar_vela_calls_total",
    "Invocaciones al pipeline procesar_vela por símbolo y timeframe",
    ["symbol", "tf"],
)

TRADER_SKIPS_TOTAL = Counter(
    "trader_skips_total",
    "Eventos en los que se omite la evaluación de una vela",
    ["reason"],
)


WATCHDOG_RESTARTS = Counter(
    "watchdog_restarts_total",
    "Reinicios de tareas provocados por el watchdog",
    ["task"],
)
WATCHDOG_RESTART_RATE = Gauge(
    "watchdog_restart_rate",
    "Tasa de reinicios del watchdog por minuto",
    ["task"],
)

BINANCE_WEIGHT_USED_1M = Gauge(
    "binance_weight_used_1m",
    "Peso utilizado en el último minuto según Binance",
)
PARTIAL_CLOSE_COLLISION = Counter(
    "partial_close_collision_total",
    "Intentos concurrentes de cierre parcial por símbolo",
    ["symbol"],
)
CONTADOR_REGISTRO_ERRORES = Counter(
    "order_register_errors_total",
    "Errores al registrar órdenes",
)

ORDERS_SYNC_SUCCESS_TOTAL = Counter(
    "orders_sync_success_total",
    "Reconciliaciones de órdenes exitosas",
)

ORDERS_SYNC_FAILURE_TOTAL = Counter(
    "orders_sync_failure_total",
    "Reconciliaciones de órdenes fallidas",
    ["reason"],
)

ORDERS_MARKET_RETRY_EXHAUSTED_TOTAL = Counter(
    "orders_market_retry_exhausted_total",
    "Órdenes de mercado que agotaron los reintentos",
    ["side", "symbol"],
)

ORDERS_CREATE_SKIP_QUANTITY_TOTAL = Counter(
    "orders_create_skip_quantity_total",
    "Solicitudes de creación de orden omitidas por cantidad inválida",
    ["symbol", "side", "source"],
)

ORDERS_REGISTRO_PENDIENTE = Gauge(
    "orders_registro_pendiente_active",
    "Indicador binario de órdenes con registro pendiente por símbolo",
    ["symbol"],
)

ORDERS_REGISTRO_PENDIENTE_TOTAL = Counter(
    "orders_registro_pendiente_total",
    "Veces que una orden quedó pendiente de registro por símbolo",
    ["symbol"],
)

ORDERS_RETRY_SCHEDULED_BY_REASON_TOTAL = Counter(
    "bot_orders_retry_scheduled_reason_total",
    "Reintentos de registro programados tras fallas de persistencia",
    ["symbol", "reason"],
)

_METRICS_WITH_FALLBACK = [
    "VELAS_DUPLICADAS",
    "CANDLES_DUPLICADAS_RATE",
    "VELAS_TOTAL",
    "VELAS_RECHAZADAS",
    "VELAS_RECHAZADAS_PCT",
    "ENTRADAS_RECHAZADAS_V2",
    "WARMUP_PROGRESS",
    "WARMUP_RESTANTE",
    "LAST_BAR_AGE",
    "BACKFILL_REQUESTS_TOTAL",
    "BACKFILL_KLINES_FETCHED_TOTAL",
    "BACKFILL_DURATION_SECONDS",
    "BACKFILL_GAPS_FOUND_TOTAL",
    "BUFFER_SIZE_V2",
    "FEEDS_FUNDING_MISSING",
    "FEEDS_OPEN_INTEREST_MISSING",
    "FEEDS_MISSING_RATE",
    "DATAFEED_WS_MESSAGES_TOTAL",
    "DATAFEED_CANDLES_ENQUEUED_TOTAL",
    "WS_CONNECTED_GAUGE",
    "QUEUE_SIZE",
    "CONSUMER_SKIPPED_EXPECTED_TOTAL",
    "INGEST_LATENCY",
    "DATAFEED_HANDLER_LATENCY",
    "TRADER_QUEUE_SIZE",
    "TRADER_PIPELINE_LATENCY",
    "TRADER_PROCESAR_VELA_CALLS_TOTAL",
    "TRADER_SKIPS_TOTAL",
    "TRADER_PIPELINE_QUEUE_WAIT",
    "CANDLES_PROCESSED_TOTAL",
    "EVAL_INTENTOS_TOTAL",
    "WATCHDOG_RESTARTS",
    "WATCHDOG_RESTART_RATE",
    "BINANCE_WEIGHT_USED_1M",
    "PARTIAL_CLOSE_COLLISION",
    "CONTADOR_REGISTRO_ERRORES",
    "ORDERS_SYNC_SUCCESS_TOTAL",
    "ORDERS_SYNC_FAILURE_TOTAL",
    "ORDERS_MARKET_RETRY_EXHAUSTED_TOTAL",
    "ORDERS_CREATE_SKIP_QUANTITY_TOTAL",
    "ORDERS_REGISTRO_PENDIENTE",
    "ORDERS_REGISTRO_PENDIENTE_TOTAL",
    "ORDERS_RETRY_SCHEDULED_BY_REASON_TOTAL",
]

for _metric_name in _METRICS_WITH_FALLBACK:
    globals()[_metric_name] = _metric_or_null(globals().get(_metric_name), _metric_name)

UMBRAL_VELAS_RECHAZADAS = float(os.getenv("UMBRAL_VELAS_RECHAZADAS", "5"))


# ---- API de registro simple ---------------------------------------------

def registrar_decision(symbol: str, action: str) -> None:
    _decisions[symbol][action] += 1
    registro_metrico.registrar("decision", {"symbol": symbol, "action": action})


def registrar_orden(status: str) -> None:
    _orders[status] += 1
    registro_metrico.registrar("orden", {"status": status})


def registrar_binance_weight(weight: int) -> None:
    BINANCE_WEIGHT_USED_1M.set(weight)


def registrar_buy_rejected_insufficient_funds() -> None:
    global _buy_rejected_insufficient_funds
    _buy_rejected_insufficient_funds += 1
    registro_metrico.registrar("buy_rejected", {"reason": "insufficient_funds"})


def registrar_partial_close_collision(symbol: str) -> None:
    PARTIAL_CLOSE_COLLISION.labels(symbol=symbol).inc()
    registro_metrico.registrar("partial_close_collision", {"symbol": symbol})


def registrar_registro_error() -> None:
    CONTADOR_REGISTRO_ERRORES.inc()
    registro_metrico.registrar("order_register_error", {})


def registrar_crear_skip_quantity(
    symbol: str,
    side: str,
    quantity_source: str | None,
) -> None:
    """Cuenta descartes de órdenes por cantidades inválidas."""

    source_label = quantity_source or "unknown"
    ORDERS_CREATE_SKIP_QUANTITY_TOTAL.labels(
        symbol=symbol,
        side=side,
        source=source_label,
    ).inc()
    registro_metrico.registrar(
        "order_create_skip",
        {
            "symbol": symbol,
            "side": side,
            "reason": "quantity",
            "source": source_label,
        },
    )


def registrar_registro_pendiente(symbol: str) -> None:
    """Marca una orden con registro pendiente y dispara alertas si persiste."""

    previously_active = symbol in _ordenes_registro_pendiente_activas
    ORDERS_REGISTRO_PENDIENTE.labels(symbol=symbol).set(1)
    rate = alert_manager.record("registro_pendiente", symbol)
    if alert_manager.should_alert("registro_pendiente", symbol):
        log.warning(
            "⚠️ Registro pendiente persistente para %s (rate=%.4f/s)",
            symbol,
            rate,
        )
    if not previously_active:
        _ordenes_registro_pendiente_activas.add(symbol)
        ORDERS_REGISTRO_PENDIENTE_TOTAL.labels(symbol=symbol).inc()
        registro_metrico.registrar(
            "order_register_pending",
            {"symbol": symbol, "status": "active"},
        )


def limpiar_registro_pendiente(symbol: str) -> None:
    """Limpia el estado pendiente cuando la orden queda registrada."""

    ORDERS_REGISTRO_PENDIENTE.labels(symbol=symbol).set(0)
    if symbol in _ordenes_registro_pendiente_activas:
        _ordenes_registro_pendiente_activas.remove(symbol)
        registro_metrico.registrar(
            "order_register_pending",
            {"symbol": symbol, "status": "resolved"},
        )


def registrar_orders_retry_scheduled(symbol: str, reason: str) -> None:
    """Anota un reintento programado tras una falla de persistencia."""

    ORDERS_RETRY_SCHEDULED_BY_REASON_TOTAL.labels(symbol=symbol, reason=reason).inc()
    if _GUARDRAIL_ORDERS_RETRY_TOTAL is not None:
        _GUARDRAIL_ORDERS_RETRY_TOTAL.labels(symbol=symbol).inc()
    registro_metrico.registrar(
        "orders_retry_scheduled",
        {"symbol": symbol, "reason": reason},
    )


def registrar_orders_sync_success() -> None:
    ORDERS_SYNC_SUCCESS_TOTAL.inc()
    registro_metrico.registrar("orders_sync", {"status": "success"})


def registrar_orders_sync_failure(reason: str) -> None:
    ORDERS_SYNC_FAILURE_TOTAL.labels(reason=reason).inc()
    registro_metrico.registrar(
        "orders_sync",
        {"status": "failure", "reason": reason},
    )


def registrar_market_retry_exhausted(side: str, symbol: str) -> None:
    ORDERS_MARKET_RETRY_EXHAUSTED_TOTAL.labels(side=side, symbol=symbol).inc()
    registro_metrico.registrar(
        "order_market_retry_exhausted",
        {"side": side, "symbol": symbol},
    )


def registrar_candles_duplicadas(symbol: str, count: int, timeframe: str | None = None) -> None:
    if count <= 0:
        return
    timeframe_label = timeframe or "unknown"
    VELAS_DUPLICADAS.labels(symbol=symbol, timeframe=timeframe_label).inc(count)
    key = f"{symbol}:{timeframe_label}"
    rate = alert_manager.record("candles_duplicates", key, count)
    CANDLES_DUPLICADAS_RATE.labels(symbol=symbol, timeframe=timeframe_label).set(rate * 60)
    if alert_manager.should_alert("candles_duplicates", key):
        log.warning(
            "[%s %s] tasa de velas duplicadas %.2f/min excede umbral",
            symbol,
            timeframe_label,
            rate * 60,
        )


def _registrar_feed_missing(symbol: str) -> None:
    rate = alert_manager.record("feeds_missing", symbol)
    FEEDS_MISSING_RATE.labels(symbol=symbol).set(rate * 60)
    if alert_manager.should_alert("feeds_missing", symbol):
        log.warning(f"[{symbol}] tasa de feeds ausentes {rate * 60:.2f}/min excede umbral")


def registrar_feed_funding_missing(symbol: str, reason: str) -> None:
    FEEDS_FUNDING_MISSING.labels(symbol=symbol, reason=reason).inc()
    _registrar_feed_missing(symbol)
    registro_metrico.registrar("feed_funding_missing", {"symbol": symbol, "reason": reason})


def registrar_feed_open_interest_missing(symbol: str, reason: str) -> None:
    FEEDS_OPEN_INTEREST_MISSING.labels(symbol=symbol, reason=reason).inc()
    registro_metrico.registrar("feed_open_interest_missing", {"symbol": symbol, "reason": reason})


def registrar_funding_signal_decoration(symbol: str, side: str, outcome: str) -> None:
    """Registra el efecto aplicado por el decorador de funding."""

    try:
        FUNDING_SIGNAL_DECORATIONS_TOTAL.labels(
            symbol=str(symbol).upper(),
            side=str(side).lower(),
            outcome=str(outcome),
        ).inc()
    except Exception:
        pass
    registro_metrico.registrar(
        "funding_signal_decoration",
        {"symbol": str(symbol).upper(), "side": str(side).lower(), "outcome": str(outcome)},
    )


def registrar_watchdog_restart(task: str) -> None:
    WATCHDOG_RESTARTS.labels(task=task).inc()
    rate = alert_manager.record("watchdog_restart", task)
    WATCHDOG_RESTART_RATE.labels(task=task).set(rate * 60)
    if alert_manager.should_alert("watchdog_restart", task):
        log.warning(f"[{task}] tasa de reinicios del watchdog {rate * 60:.2f}/min excede umbral")
    registro_metrico.registrar("watchdog_restart", {"task": task})


def registrar_correlacion_btc(symbol: str, rho: float) -> None:
    _correlacion_btc[symbol] = rho
    registro_metrico.registrar("correlacion_btc", {"symbol": symbol, "rho": rho})


def registrar_warmup_progress(symbol: str, progress: float) -> None:
    WARMUP_PROGRESS.labels(symbol=symbol).set(progress)
    registro_metrico.registrar("warmup_progress", {"symbol": symbol, "progress": progress})


def registrar_vela_recibida(symbol: str, timeframe: str | None = None) -> None:
    timeframe_label = timeframe or "unknown"
    key = (symbol, timeframe_label)
    _velas_total[key] += 1
    VELAS_TOTAL.labels(symbol=symbol, timeframe=timeframe_label).inc()
    _actualizar_porcentaje(symbol, timeframe_label)


def registrar_vela_rechazada(symbol: str, reason: str, timeframe: str | None = None) -> None:
    timeframe_label = timeframe or "unknown"
    key = (symbol, timeframe_label)
    _velas_rechazadas[key][reason] += 1
    VELAS_RECHAZADAS.labels(symbol=symbol, timeframe=timeframe_label, reason=reason).inc()
    registro_metrico.registrar(
        "vela_rechazada",
        {"symbol": symbol, "timeframe": timeframe_label, "reason": reason},
    )
    pct = _actualizar_porcentaje(symbol, timeframe_label)
    if pct > UMBRAL_VELAS_RECHAZADAS:
        log.warning(
            "[%s %s] porcentaje de velas rechazadas %.2f%% supera umbral %s%%",
            symbol,
            timeframe_label,
            pct,
            UMBRAL_VELAS_RECHAZADAS,
        )


def _actualizar_porcentaje(symbol: str, timeframe: str) -> float:
    key = (symbol, timeframe)
    total = _velas_total[key]
    rechazadas = sum(_velas_rechazadas[key].values())
    pct = (rechazadas / total * 100) if total else 0.0
    VELAS_RECHAZADAS_PCT.labels(symbol=symbol, timeframe=timeframe).set(pct)
    return pct


def subscribe_simulated_order_metrics(bus) -> None:
    """Suscribe métricas a eventos de órdenes simuladas."""
    async def _on_open(_):
        registrar_orden('opened')
    async def _on_close(_):
        registrar_orden('closed')
    bus.subscribe('orden_simulada_creada', _on_open)
    bus.subscribe('orden_simulada_cerrada', _on_close)


def iniciar_exporter() -> Optional[WSGIServer]:
    """Inicia el servidor WSGI para exponer métricas (si Prometheus está disponible)."""
    port = int(os.getenv("METRICS_PORT", "8000"))
    if not HAVE_PROM:
        log.info("Prometheus no disponible; exporter deshabilitado")
        return None
    try:
        result = start_wsgi_server(port)
        server = result[0] if isinstance(result, tuple) else result
        if server is None:
            log.info("Prometheus no disponible; exporter deshabilitado")
            return None
    except OSError as exc:
        log.warning("No se pudo iniciar exporter en puerto %s: %s", port, exc)
        return None
    log.info(f"Prometheus exporter escuchando en puerto {port}")
    return server

