"""Métricas adicionales de observabilidad para DataFeed.

Estas métricas complementan a ``core.metrics`` proporcionando tasas de
producción y consumo, así como watermarks de tamaño de cola.
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

__all__ = [
    "PRODUCER_RATE",
    "QUEUE_DROPS",
    "CONSUMER_RATE",
    "QUEUE_SIZE_MIN",
    "QUEUE_SIZE_MAX",
    "QUEUE_SIZE_AVG",
]