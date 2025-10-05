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

_velas_total: Dict[str, int] = defaultdict(int)
_velas_rechazadas: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

# ---- Definición de métricas ----
VELAS_DUPLICADAS = Counter(
    "candles_duplicates_total",
    "Velas duplicadas detectadas por símbolo",
    ["symbol"],
)
CANDLES_DUPLICADAS_RATE = Gauge(
    "candles_duplicates_rate",
    "Tasa de velas duplicadas por minuto",
    ["symbol"],
)

VELAS_TOTAL = Counter("velas_total", "Velas recibidas por símbolo", ["symbol"])
VELAS_RECHAZADAS = Counter(
    "velas_rechazadas_total",
    "Velas rechazadas por símbolo y razón",
    ["symbol", "reason"],
)
VELAS_RECHAZADAS_PCT = Gauge(
    "velas_rechazadas_pct",
    "Porcentaje de velas rechazadas por símbolo",
    ["symbol"],
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
QUEUE_SIZE = Gauge(
    "datafeed_queue_size",
    "Tamaño de cola de DataFeed",
    ["symbol"],
)
INGEST_LATENCY = Histogram(
    "datafeed_ingest_latency_seconds",
    "Latencia desde recepción hasta procesamiento",
    ["symbol"],
)

# Métricas de Trader
TRADER_QUEUE_SIZE = Gauge(
    "trader_queue_size",
    "Tamaño de la cola interna del Trader",
)
TRADER_PIPELINE_LATENCY = Histogram(
    "trader_pipeline_latency_seconds",
    "Latencia del procesamiento de velas en el trader modular",
    ["symbol"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)
TRADER_PIPELINE_QUEUE_WAIT = Histogram(
    "trader_pipeline_queue_wait_seconds",
    "Tiempo que una vela espera en la cola interna del trader",
    ["symbol"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
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

_METRICS_WITH_FALLBACK = [
    "VELAS_DUPLICADAS",
    "CANDLES_DUPLICADAS_RATE",
    "VELAS_TOTAL",
    "VELAS_RECHAZADAS",
    "VELAS_RECHAZADAS_PCT",
    "WARMUP_PROGRESS",
    "WARMUP_RESTANTE",
    "LAST_BAR_AGE",
    "FEEDS_FUNDING_MISSING",
    "FEEDS_OPEN_INTEREST_MISSING",
    "FEEDS_MISSING_RATE",
    "QUEUE_SIZE",
    "INGEST_LATENCY",
    "TRADER_QUEUE_SIZE",
    "TRADER_PIPELINE_LATENCY",
    "TRADER_PIPELINE_QUEUE_WAIT",
    "WATCHDOG_RESTARTS",
    "WATCHDOG_RESTART_RATE",
    "BINANCE_WEIGHT_USED_1M",
    "PARTIAL_CLOSE_COLLISION",
    "CONTADOR_REGISTRO_ERRORES",
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


def registrar_candles_duplicadas(symbol: str, count: int) -> None:
    if count <= 0:
        return
    VELAS_DUPLICADAS.labels(symbol=symbol).inc(count)
    rate = alert_manager.record("candles_duplicates", symbol, count)
    CANDLES_DUPLICADAS_RATE.labels(symbol=symbol).set(rate * 60)
    if alert_manager.should_alert("candles_duplicates", symbol):
        log.warning(f"[{symbol}] tasa de velas duplicadas {rate * 60:.2f}/min excede umbral")


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


def registrar_vela_recibida(symbol: str) -> None:
    _velas_total[symbol] += 1
    VELAS_TOTAL.labels(symbol=symbol).inc()
    _actualizar_porcentaje(symbol)


def registrar_vela_rechazada(symbol: str, reason: str) -> None:
    _velas_rechazadas[symbol][reason] += 1
    VELAS_RECHAZADAS.labels(symbol=symbol, reason=reason).inc()
    registro_metrico.registrar("vela_rechazada", {"symbol": symbol, "reason": reason})
    pct = _actualizar_porcentaje(symbol)
    if pct > UMBRAL_VELAS_RECHAZADAS:
        log.warning(f"[{symbol}] porcentaje de velas rechazadas {pct:.2f}% supera umbral {UMBRAL_VELAS_RECHAZADAS}%")


def _actualizar_porcentaje(symbol: str) -> float:
    total = _velas_total[symbol]
    rechazadas = sum(_velas_rechazadas[symbol].values())
    pct = (rechazadas / total * 100) if total else 0.0
    VELAS_RECHAZADAS_PCT.labels(symbol=symbol).set(pct)
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

