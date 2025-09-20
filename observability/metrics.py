"""Métricas Prometheus para observability del bot."""

from __future__ import annotations
import time
from asyncio import sleep as async_sleep
from typing import Dict, Callable, Sequence

from prometheus_client import Counter, Gauge, Histogram, REGISTRY
from prometheus_client.metrics import MetricWrapperBase


def _get_metric(
    metric_cls: Callable[..., MetricWrapperBase],
    name: str,
    documentation: str,
    labelnames: Sequence[str] | None = None,
    **kwargs,
) -> MetricWrapperBase:
    """Obtiene un colector existente o crea uno nuevo si no está registrado."""
    existing = REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]
    if existing:
        return existing
    if labelnames is None:
        return metric_cls(name, documentation, **kwargs)
    return metric_cls(name, documentation, labelnames, **kwargs)

# Métricas de DataFeed
PRODUCER_RATE = _get_metric(
    Gauge,
    "datafeed_producer_rate",
    "Velocidad de producción de velas (eventos/segundo)",
    ["symbol"],
)
CONSUMER_RATE = _get_metric(
    Gauge,
    "datafeed_consumer_rate",
    "Velocidad de consumo de velas (eventos/segundo)",
    ["symbol"],
)
CONSUMER_STATE = _get_metric(
    Gauge,
    "datafeed_consumer_state",
    "Estado del consumidor (0=STARTING,1=HEALTHY,2=STALLED,3=LOOP)",
    ["symbol"],
)
CONSUMER_LAST_TIMESTAMP = _get_metric(
    Gauge,
    "datafeed_consumer_last_timestamp_seconds",
    "Último timestamp procesado por el consumer (segundos desde epoch)",
    ["symbol"],
)
CONSUMER_STALLS = _get_metric(
    Counter,
    "datafeed_consumer_stalls_total",
    "Detecciones de consumer atascado",
    ["symbol"],
)
CONSUMER_PROGRESS_FAILURES = _get_metric(
    Counter,
    "datafeed_consumer_progress_errors_total",
    "Errores de progreso (timestamp no avanza)",
    ["symbol"],
)
QUEUE_DROPS = _get_metric(
    Counter,
    "datafeed_queue_drops_total",
    "Velas descartadas por cola llena",
    ["symbol"],
)
HANDLER_TIMEOUTS = _get_metric(
    Counter,
    "datafeed_handler_timeouts_total",
    "Velas descartadas por timeout en handler",
    ["symbol"],
)
QUEUE_SIZE_MIN = _get_metric(
    Gauge,
    "datafeed_queue_size_min",
    "Tamaño mínimo de cola en ventana reciente",
    ["symbol"],
)
QUEUE_SIZE_MAX = _get_metric(
    Gauge,
    "datafeed_queue_size_max",
    "Tamaño máximo de cola en ventana reciente",
    ["symbol"],
)
QUEUE_SIZE_AVG = _get_metric(
    Gauge,
    "datafeed_queue_size_avg",
    "Tamaño promedio de cola en ventana reciente",
    ["symbol"],
)

QUEUE_HIGH_WATERMARK_RATIO = _get_metric(
    Gauge,
    "datafeed_queue_high_watermark_ratio",
    "Ratio de ocupación registrado al superar el umbral de alerta",
    ["symbol"],
)

QUEUE_COALESCE_TOTAL = _get_metric(
    Counter,
    "datafeed_queue_coalesce_total",
    "Velas combinadas mediante coalescing",
    ["symbol"],
)

QUEUE_POLICY_FALLBACK_TOTAL = _get_metric(
    Counter,
    "datafeed_queue_policy_fallback_total",
    "Aplicaciones de la política de seguridad al manejar colas llenas",
    ["symbol", "policy"],
)

CANDLE_AGE = _get_metric(
    Histogram,
    "datafeed_candle_age_seconds",
    "Edad de las velas al momento de procesarse",
    ["symbol"],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30),
)

# Reconexiones fallidas y bandera de feed detenido
DATAFEED_FAILED_RESTARTS = _get_metric(
    Counter,
    "datafeed_reconexiones_fallidas_total",
    "Intentos fallidos de reconexión del datafeed",
)
FEED_STOPPED = _get_metric(
    Gauge,
    "alerta_feed_detenido",
    "Bandera que indica si el feed se ha detenido por circuito abierto",
)

# Métricas de señales
SIGNALS_TOTAL = _get_metric(
    Counter,
    "signals_total",
    "Señales de entrada generadas por tipo",
    ["symbol", "type"],
)
SIGNALS_CONFLICT = _get_metric(
    Counter,
    "signals_conflict_total",
    "Conflictos de señales buy/sell detectados",
    ["symbol"],
)

# Métricas de spread
SPREAD_OBSERVED = _get_metric(
    Gauge,
    "spread_observed_ratio",
    "Spread observado en el último intento de entrada",
    ["symbol"],
)
SPREAD_REJECTS = _get_metric(
    Counter,
    "spread_rejections_total",
    "Órdenes canceladas por spread excesivo",
    ["symbol"],
)

# Métricas de órdenes
ORDERS_OPEN = _get_metric(
    Gauge,
    "orders_open",
    "Órdenes abiertas activas por símbolo",
    ["symbol"],
)
ORDERS_SENT = _get_metric(
    Counter,
    "orders_sent_total",
    "Órdenes enviadas al exchange por tipo",
    ["type"],
)
ORDERS_CANCELLED = _get_metric(
    Counter,
    "orders_cancelled_total",
    "Órdenes canceladas por motivo",
    ["reason"],
)

PNL_DISCREPANCIA_SHORT = _get_metric(
    Gauge,
    "pnl_discrepancia_short",
    "Diferencia entre PnL esperado y aplicado en cierres cortos",
    ["symbol"],
)

# Métricas de evaluación de entradas
EVALUAR_ENTRADA_LATENCY_MS = _get_metric(
    Histogram,
    "evaluar_entrada_latency_ms",
    "Latencia de evaluar_condiciones_de_entrada (ms)",
    ["symbol"],
    buckets=(50, 100, 200, 500, 1000, 2000, 5000, 10000),
)

EVALUAR_ENTRADA_TIMEOUTS = _get_metric(
    Counter,
    "evaluar_entrada_timeouts_total",
    "Timeouts en evaluar_condiciones_de_entrada",
    ["symbol"],
)

# Telemetría de streams y latidos
TICKS_TOTAL = _get_metric(
    Counter,
    "tick_data_total",
    "Ticks de datos recibidos por símbolo",
    ["symbol"],
)
TICKS_POR_MINUTO = _get_metric(
    Gauge,
    "ticks_por_minuto",
    "Tasa de ticks por minuto (EMA 60s)",
    ["symbol"],
)
MS_DESDE_ULTIMO_TICK = _get_metric(
    Gauge,
    "ms_desde_ultimo_tick",
    "Milisegundos transcurridos desde el último tick",
    ["symbol"],
)
REINTENTOS_RECONEXION_TOTAL = _get_metric(
    Counter,
    "reintentos_reconexion_total",
    "Reintentos de reconexión de streams por símbolo",
    ["symbol"],
)
BACKOFF_SECONDS = _get_metric(
    Histogram,
    "backoff_seconds",
    "Tiempo de espera aplicado antes de reintentar conexiones",
)
STREAMS_ACTIVOS = _get_metric(
    Gauge,
    "streams_activos",
    "Estado del stream (1=activo, 0=caído)",
    ["symbol"],
)

TRADER_BACKPRESSURE_TOTAL = _get_metric(
    Counter,
    "trader_backpressure_total",
    "Activaciones del mecanismo de backpressure en el trader modular",
)

TRADER_BACKPRESSURE_DURATION = _get_metric(
    Histogram,
    "trader_backpressure_duration_seconds",
    "Duración de las pausas aplicadas por backpressure en el trader modular",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

TRADER_BACKPRESSURE_ACTIVE = _get_metric(
    Gauge,
    "trader_backpressure_active",
    "Indicador binario de backpressure activo en el trader modular",
)

TRADER_FASTPATH_ACTIVE = _get_metric(
    Gauge,
    "trader_fastpath_active",
    "Indicador binario del modo degradado del trader",
)

TRADER_FASTPATH_TRANSITIONS = _get_metric(
    Counter,
    "trader_fastpath_transitions_total",
    "Activaciones del modo degradado del trader",
    ["state"],
)

HEARTBEAT_OK_TOTAL = _get_metric(
    Counter,
    "heartbeat_ok_total",
    "Latidos registrados por tarea",
    ["task"],
)
HEARTBEAT_INTERVAL_MS = _get_metric(
    Gauge,
    "heartbeat_interval_ms",
    "Intervalo entre latidos sucesivos (ms)",
    ["task"],
)
HEARTBEAT_JITTER_MS = _get_metric(
    Histogram,
    "heartbeat_jitter_ms",
    "Retraso observado del heartbeat (ms)",
)

ULTIMO_TICK: Dict[str, float] = {}
EMA_TICKS_PM: Dict[str, float] = {}
EMA_ALPHA = 2 / (60 + 1)

# Monitoreo de latencia del loop
EVENT_LOOP_LAG_MS = _get_metric(
    Gauge,
    "event_loop_lag_ms",
    "Latencia del loop de eventos (ms)",
)



async def monitor_event_loop_lag(interval: float = 0.5) -> None:
    """Monitorea la latencia del loop de eventos y actualiza la métrica."""
    while True:
        start = time.monotonic()
        await async_sleep(interval)
        lag_ms = (time.monotonic() - start - interval) * 1000
        EVENT_LOOP_LAG_MS.set(lag_ms)


def registrar_tick_data(symbol: str, reinicio: bool = False) -> None:
    """Actualiza métricas asociadas a un tick o reinicio de stream."""
    ahora = time.monotonic()
    if reinicio:
        ULTIMO_TICK[symbol] = ahora
        MS_DESDE_ULTIMO_TICK.labels(symbol).set(0)
        STREAMS_ACTIVOS.labels(symbol).set(0)
        return
    ultimo = ULTIMO_TICK.get(symbol)
    if ultimo is not None:
        dt = ahora - ultimo
        rate = 60.0 / dt if dt > 0 else 0.0
        ema_prev = EMA_TICKS_PM.get(symbol, rate)
        ema = EMA_ALPHA * rate + (1 - EMA_ALPHA) * ema_prev
        EMA_TICKS_PM[symbol] = ema
        TICKS_POR_MINUTO.labels(symbol).set(ema)
    else:
        EMA_TICKS_PM[symbol] = 0.0
        TICKS_POR_MINUTO.labels(symbol).set(0.0)
    ULTIMO_TICK[symbol] = ahora
    MS_DESDE_ULTIMO_TICK.labels(symbol).set(0)
    STREAMS_ACTIVOS.labels(symbol).set(1)
    TICKS_TOTAL.labels(symbol).inc()


async def monitor_ms_desde_ultimo_tick(interval: float = 1.0) -> None:
    """Actualiza periódicamente ``ms_desde_ultimo_tick`` para cada símbolo."""
    while True:
        ahora = time.monotonic()
        for sym, ts in ULTIMO_TICK.items():
            MS_DESDE_ULTIMO_TICK.labels(sym).set((ahora - ts) * 1000)
        await async_sleep(interval)
        
