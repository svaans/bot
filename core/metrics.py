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

from prometheus_client import Counter, Gauge

from core.registro_metrico import registro_metrico
from core.utils.logger import configurar_logger


_decisions: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
_orders: Dict[str, int] = defaultdict(int)
_buy_rejected_insufficient_funds: int = 0
_correlacion_btc: Dict[str, float] = {}

_velas_total: Dict[str, int] = defaultdict(int)
_velas_rechazadas: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

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

def registrar_buy_rejected_insufficient_funds() -> None:
    """Incrementa ``buy_rejected_insufficient_funds_total``."""

    global _buy_rejected_insufficient_funds
    _buy_rejected_insufficient_funds += 1
    registro_metrico.registrar("buy_rejected", {"reason": "insufficient_funds"})


def registrar_feed_funding_missing(symbol: str, reason: str) -> None:
    """Registra ausencia de funding rate."""

    FEEDS_FUNDING_MISSING.labels(symbol=symbol, reason=reason).inc()
    registro_metrico.registrar(
        "feed_funding_missing", {"symbol": symbol, "reason": reason}
    )


def registrar_feed_open_interest_missing(symbol: str, reason: str) -> None:
    """Registra ausencia de open interest."""

    FEEDS_OPEN_INTEREST_MISSING.labels(symbol=symbol, reason=reason).inc()
    registro_metrico.registrar(
        "feed_open_interest_missing", {"symbol": symbol, "reason": reason}
    )
    
def registrar_correlacion_btc(symbol: str, rho: float) -> None:
    """Registra la correlación de un símbolo con BTC."""

    _correlacion_btc[symbol] = rho
    registro_metrico.registrar("correlacion_btc", {"symbol": symbol, "rho": rho})


def decisions_total() -> Dict[str, Dict[str, int]]:
    """Retorna una copia de los contadores de decisiones."""

    return {s: dict(a) for s, a in _decisions.items()}


def orders_total() -> Dict[str, int]:
    """Retorna una copia de los contadores de órdenes."""

    return dict(_orders)


def buy_rejected_insufficient_funds_total() -> int:
    """Retorna el total de compras rechazadas por fondos insuficientes."""

    return _buy_rejected_insufficient_funds


def correlacion_btc() -> Dict[str, float]:
    """Retorna la última correlación registrada con BTC por símbolo."""

    return dict(_correlacion_btc)


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


def obtener_velas_rechazadas() -> Dict[str, Dict[str, int]]:
    """Retorna los contadores de velas rechazadas."""

    return {s: dict(r) for s, r in _velas_rechazadas.items()}


def reset_velas_metrics() -> None:
    """Reinicia los contadores internos de velas."""

    _velas_total.clear()
    _velas_rechazadas.clear()
