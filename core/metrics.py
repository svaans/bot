"""Contadores básicos de métricas para el bot.

Este módulo implementa contadores simples en memoria y los registra en
``RegistroMetrico`` para su persistencia. Las métricas expuestas son:

``decisions_total{symbol,action}`` – Número de decisiones tomadas por
símbolo y acción.
``orders_total{status}`` – Conteo de órdenes por estado.
``correlacion_btc{symbol}`` – Última correlación conocida con BTC.
"""

from __future__ import annotations

import os
from collections import defaultdict
from typing import Dict

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from core.registro_metrico import registro_metrico
from core.utils.logger import configurar_logger
from core.alertas import alert_manager


_decisions: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
_orders: Dict[str, int] = defaultdict(int)
_buy_rejected_insufficient_funds: int = 0
_correlacion_btc: Dict[str, float] = {}

_velas_total: Dict[str, int] = defaultdict(int)
_velas_rechazadas: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

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

UMBRAL_VELAS_RECHAZADAS = float(os.getenv("UMBRAL_VELAS_RECHAZADAS", 5))

log = configurar_logger("metrics")



def registrar_decision(symbol: str, action: str) -> None:
    """Incrementa ``decisions_total`` para ``symbol`` y ``action``."""

    _decisions[symbol][action] += 1
    registro_metrico.registrar("decision", {"symbol": symbol, "action": action})


def registrar_orden(status: str) -> None:
    """Incrementa ``orders_total`` para ``status``."""

    _orders[status] += 1
    registro_metrico.registrar("orden", {"status": status})


def registrar_binance_weight(weight: int) -> None:
    """Actualiza el peso usado en el último minuto."""
    BINANCE_WEIGHT_USED_1M.set(weight)
    
def registrar_buy_rejected_insufficient_funds() -> None:
    """Incrementa ``buy_rejected_insufficient_funds_total``."""

    global _buy_rejected_insufficient_funds
    _buy_rejected_insufficient_funds += 1
    registro_metrico.registrar("buy_rejected", {"reason": "insufficient_funds"})


def registrar_partial_close_collision(symbol: str) -> None:
    """Incrementa ``partial_close_collision_total`` para ``symbol``."""

    PARTIAL_CLOSE_COLLISION.labels(symbol=symbol).inc()
    registro_metrico.registrar("partial_close_collision", {"symbol": symbol})


def registrar_registro_error() -> None:
    """Incrementa ``order_register_errors_total``."""

    CONTADOR_REGISTRO_ERRORES.inc()
    registro_metrico.registrar("order_register_error", {})


def registrar_candles_duplicadas(symbol: str, count: int) -> None:
    """Registra velas duplicadas y actualiza tasa."""

    if count <= 0:
        return
    VELAS_DUPLICADAS.labels(symbol=symbol).inc(count)
    rate = alert_manager.record("candles_duplicates", symbol, count)
    CANDLES_DUPLICADAS_RATE.labels(symbol=symbol).set(rate * 60)
    if alert_manager.should_alert("candles_duplicates", symbol):
        log.warning(
            f"[{symbol}] tasa de velas duplicadas {rate * 60:.2f}/min excede umbral"
        )


def _registrar_feed_missing(symbol: str) -> None:
    rate = alert_manager.record("feeds_missing", symbol)
    FEEDS_MISSING_RATE.labels(symbol=symbol).set(rate * 60)
    if alert_manager.should_alert("feeds_missing", symbol):
        log.warning(
            f"[{symbol}] tasa de feeds ausentes {rate * 60:.2f}/min excede umbral"
        )


def registrar_feed_funding_missing(symbol: str, reason: str) -> None:
    """Registra ausencia de funding rate."""

    FEEDS_FUNDING_MISSING.labels(symbol=symbol, reason=reason).inc()
    _registrar_feed_missing(symbol)
    registro_metrico.registrar(
        "feed_funding_missing", {"symbol": symbol, "reason": reason}
    )


def registrar_feed_open_interest_missing(symbol: str, reason: str) -> None:
    """Registra ausencia de open interest."""

    FEEDS_OPEN_INTEREST_MISSING.labels(symbol=symbol, reason=reason).inc()
    registro_metrico.registrar(
        "feed_open_interest_missing", {"symbol": symbol, "reason": reason}
    )


def registrar_watchdog_restart(task: str) -> None:
    """Registra un reinicio de ``task`` provocado por el watchdog."""

    WATCHDOG_RESTARTS.labels(task=task).inc()
    rate = alert_manager.record("watchdog_restart", task)
    WATCHDOG_RESTART_RATE.labels(task=task).set(rate * 60)
    if alert_manager.should_alert("watchdog_restart", task):
        log.warning(
            f"[{task}] tasa de reinicios del watchdog {rate * 60:.2f}/min excede umbral"
        )
    registro_metrico.registrar("watchdog_restart", {"task": task})
    
def registrar_correlacion_btc(symbol: str, rho: float) -> None:
    """Registra la correlación de un símbolo con BTC."""

    _correlacion_btc[symbol] = rho
    registro_metrico.registrar("correlacion_btc", {"symbol": symbol, "rho": rho})


def registrar_warmup_progress(symbol: str, progress: float) -> None:
    """Actualiza la métrica de progreso de warmup para ``symbol``."""
    WARMUP_PROGRESS.labels(symbol=symbol).set(progress)
    registro_metrico.registrar(
        "warmup_progress", {"symbol": symbol, "progress": progress}
    )

    



def registrar_vela_recibida(symbol: str) -> None:
    """Incrementa el total de velas recibidas para ``symbol``."""

    _velas_total[symbol] += 1
    VELAS_TOTAL.labels(symbol=symbol).inc()
    _actualizar_porcentaje(symbol)


def registrar_vela_rechazada(symbol: str, reason: str) -> None:
    """Incrementa el contador de velas rechazadas para ``symbol`` y ``reason``."""

    _velas_rechazadas[symbol][reason] += 1
    VELAS_RECHAZADAS.labels(symbol=symbol, reason=reason).inc()
    registro_metrico.registrar("vela_rechazada", {"symbol": symbol, "reason": reason})
    pct = _actualizar_porcentaje(symbol)
    if pct > UMBRAL_VELAS_RECHAZADAS:
        log.warning(
            f"[{symbol}] porcentaje de velas rechazadas {pct:.2f}% supera umbral {UMBRAL_VELAS_RECHAZADAS}%"
        )


def _actualizar_porcentaje(symbol: str) -> float:
    """Calcula y actualiza el porcentaje de velas rechazadas."""

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


def iniciar_exporter() -> None:
    """Inicia el servidor HTTP para exponer métricas."""

    port = int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(port)
    log.info(f"Prometheus exporter escuchando en puerto {port}")
