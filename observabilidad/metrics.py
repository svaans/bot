"""Métricas Prometheus para observabilidad del bot."""

from prometheus_client import Counter, Gauge, Histogram
from asyncio import sleep as async_sleep
import time
from typing import Dict

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

# Reconexiones fallidas y bandera de feed detenido
DATAFEED_FAILED_RESTARTS = Counter(
    "datafeed_reconexiones_fallidas_total",
    "Intentos fallidos de reconexión del datafeed",
)
FEED_STOPPED = Gauge(
    "alerta_feed_detenido",
    "Bandera que indica si el feed se ha detenido por circuito abierto",
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

# Telemetría de streams y latidos
TICKS_TOTAL = Counter(
    "tick_data_total",
    "Ticks de datos recibidos por símbolo",
    ["symbol"],
)
TICKS_POR_MINUTO = Gauge(
    "ticks_por_minuto",
    "Tasa de ticks por minuto (EMA 60s)",
    ["symbol"],
)
MS_DESDE_ULTIMO_TICK = Gauge(
    "ms_desde_ultimo_tick",
    "Milisegundos transcurridos desde el último tick",
    ["symbol"],
)
REINTENTOS_RECONEXION_TOTAL = Counter(
    "reintentos_reconexion_total",
    "Reintentos de reconexión de streams por símbolo",
    ["symbol"],
)
STREAMS_ACTIVOS = Gauge(
    "streams_activos",
    "Estado del stream (1=activo, 0=caído)",
    ["symbol"],
)

HEARTBEAT_OK_TOTAL = Counter(
    "heartbeat_ok_total",
    "Latidos registrados por tarea",
    ["task"],
)
HEARTBEAT_JITTER_MS = Gauge(
    "heartbeat_jitter_ms",
    "Intervalo entre latidos sucesivos (ms)",
    ["task"],
)

ULTIMO_TICK: Dict[str, float] = {}
EMA_TICKS_PM: Dict[str, float] = {}
EMA_ALPHA = 2 / (60 + 1)

# Monitoreo de latencia del loop
EVENT_LOOP_LAG_MS = Gauge(
    "event_loop_lag_ms",
    "Latencia del loop de eventos (ms)",
)

HEARTBEAT_JITTER_MS = Histogram(
    "heartbeat_jitter_ms",
    "Retraso observado del heartbeat (ms)",
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
        
