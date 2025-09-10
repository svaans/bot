"""Métricas Prometheus para observabilidad del bot."""

from prometheus_client import Counter, Gauge
from asyncio import sleep as async_sleep
import time

# Métricas de DataFeed
PRODUCER_RATE = Gauge(
    "datafeed_producer_rate",
    "Velocidad de producción de velas (eventos/segundo)",
    ["symbol"],
)
CONSUMER_RATE = Gauge(
    "datafeed_consumer_rate",
    "Velocidad de consumo de velas (eventos/segundo)",
    ["symbol"],
)
QUEUE_DROPS = Counter(
    "datafeed_queue_drops_total",
    "Velas descartadas por cola llena",
    ["symbol"],
)
HANDLER_TIMEOUTS = Counter(
    "datafeed_handler_timeouts_total",
    "Velas descartadas por timeout en handler",
    ["symbol"],
)
QUEUE_SIZE_MIN = Gauge(
    "datafeed_queue_size_min",
    "Tamaño mínimo de cola en ventana reciente",
    ["symbol"],
)
QUEUE_SIZE_MAX = Gauge(
    "datafeed_queue_size_max",
    "Tamaño máximo de cola en ventana reciente",
    ["symbol"],
)
QUEUE_SIZE_AVG = Gauge(
    "datafeed_queue_size_avg",
    "Tamaño promedio de cola en ventana reciente",
    ["symbol"],
)

# Métricas de señales
SIGNALS_TOTAL = Counter(
    "signals_total",
    "Señales de entrada generadas por tipo",
    ["symbol", "type"],
)
SIGNALS_CONFLICT = Counter(
    "signals_conflict_total",
    "Conflictos de señales buy/sell detectados",
    ["symbol"],
)

# Métricas de spread
SPREAD_OBSERVED = Gauge(
    "spread_observed_ratio",
    "Spread observado en el último intento de entrada",
    ["symbol"],
)
SPREAD_REJECTS = Counter(
    "spread_rejections_total",
    "Órdenes canceladas por spread excesivo",
    ["symbol"],
)

# Métricas de órdenes
ORDERS_OPEN = Gauge(
    "orders_open",
    "Órdenes abiertas activas por símbolo",
    ["symbol"],
)
ORDERS_SENT = Counter(
    "orders_sent_total",
    "Órdenes enviadas al exchange por tipo",
    ["type"],
)
ORDERS_CANCELLED = Counter(
    "orders_cancelled_total",
    "Órdenes canceladas por motivo",
    ["reason"],
)

PNL_DISCREPANCIA_SHORT = Gauge(
    "pnl_discrepancia_short",
    "Diferencia entre PnL esperado y aplicado en cierres cortos",
    ["symbol"],
)

# Monitoreo de latencia del loop
EVENT_LOOP_LAG_MS = Gauge(
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
        
