"""Contadores básicos de métricas para el bot.

Este módulo implementa contadores simples en memoria y los registra en
``RegistroMetrico`` para su persistencia. Las métricas expuestas son:

``decisions_total{symbol,action}`` – Número de decisiones tomadas por
símbolo y acción.
``orders_total{status}`` – Conteo de órdenes por estado.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict

from core.registro_metrico import registro_metrico


_decisions: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
_orders: Dict[str, int] = defaultdict(int)


def registrar_decision(symbol: str, action: str) -> None:
    """Incrementa ``decisions_total`` para ``symbol`` y ``action``."""

    _decisions[symbol][action] += 1
    registro_metrico.registrar(
        "decision", {"symbol": symbol, "action": action}
    )


def registrar_orden(status: str) -> None:
    """Incrementa ``orders_total`` para ``status``."""

    _orders[status] += 1
    registro_metrico.registrar("orden", {"status": status})


def decisions_total() -> Dict[str, Dict[str, int]]:
    """Retorna una copia de los contadores de decisiones."""

    return {s: dict(a) for s, a in _decisions.items()}


def orders_total() -> Dict[str, int]:
    """Retorna una copia de los contadores de órdenes."""

    return dict(_orders)