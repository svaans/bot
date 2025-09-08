"""Métricas adicionales de observabilidad.

Incluye contadores y gauges expuestos vía Prometheus para los distintos
componentes del bot. Inicialmente se empleaba sólo para el ``DataFeed``,
pero ahora también registra métricas del ``StrategyEngine`` y del módulo
``Trader``.
"""

from prometheus_client import Counter, Gauge

# Tasa de producción de velas por símbolo (velas por segundo)
PRODUCER_RATE = Gauge(
    "datafeed_producer_rate",
    "Tasa de producción de velas por segundo",
    ["symbol"],
)

# Conteo de velas descartadas por cola llena
QUEUE_DROPS = Counter(
    "datafeed_queue_drops_total",
    "Velas descartadas por cola llena",
    ["symbol"],
)

# Tasa de consumo de velas por símbolo (velas por segundo)
CONSUMER_RATE = Gauge(
    "datafeed_consumer_rate",
    "Tasa de procesamiento de velas por segundo",
    ["symbol"],
)

# Watermarks de tamaño de cola en ventana de 60 segundos
QUEUE_SIZE_MIN = Gauge(
    "datafeed_queue_size_min",
    "Tamaño mínimo de la cola en ventana de 60s",
    ["symbol"],
)
QUEUE_SIZE_MAX = Gauge(
    "datafeed_queue_size_max",
    "Tamaño máximo de la cola en ventana de 60s",
    ["symbol"],
)
QUEUE_SIZE_AVG = Gauge(
    "datafeed_queue_size_avg",
    "Tamaño promedio de la cola en ventana de 60s",
    ["symbol"],
)

# =============================== Trader ===============================
# Métricas relacionadas con la operación y las señales de trading.

# Spread observado para cada entrada (como ratio)
SPREAD_OBSERVED = Gauge(
    "trader_spread_observed_ratio",
    "Spread observado antes de ejecutar una orden",
    ["symbol"],
)

# Rechazos de orden por superar el spread permitido
SPREAD_REJECTS = Counter(
    "trader_spread_rejects_total",
    "Órdenes rechazadas por spread excesivo",
    ["symbol"],
)

# Órdenes canceladas diferenciadas por motivo
ORDERS_CANCELLED = Counter(
    "trader_orders_cancelled_total",
    "Órdenes canceladas por motivo",
    ["reason"],
)

# Órdenes enviadas al exchange por tipo (buy/sell)
ORDERS_SENT = Counter(
    "trader_orders_sent_total",
    "Órdenes enviadas clasificadas por tipo",
    ["type"],
)

# Órdenes actualmente abiertas por símbolo
ORDERS_OPEN = Gauge(
    "trader_orders_open",
    "Órdenes abiertas activas por símbolo",
    ["symbol"],
)

# Señales generadas por símbolo y tipo (buy/sell)
SIGNALS_TOTAL = Counter(
    "trader_signals_total",
    "Total de señales generadas por símbolo y tipo",
    ["symbol", "type"],
)

# Conflictos de señales BUY/SELL detectados en el motor de estrategias
SIGNALS_CONFLICT = Counter(
    "trader_signals_conflict_total",
    "Conflictos de señales BUY/SELL detectados",
    ["symbol"],
)

__all__ = [
    "PRODUCER_RATE",
    "QUEUE_DROPS",
    "CONSUMER_RATE",
    "QUEUE_SIZE_MIN",
    "QUEUE_SIZE_MAX",
    "QUEUE_SIZE_AVG",
    "SPREAD_OBSERVED",
    "SPREAD_REJECTS",
    "ORDERS_CANCELLED",
    "ORDERS_SENT",
    "ORDERS_OPEN",
    "SIGNALS_TOTAL",
    "SIGNALS_CONFLICT",
]
