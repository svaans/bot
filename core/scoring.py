"""Funciones de scoring técnico con normalización por símbolo."""
from __future__ import annotations

from typing import Optional, Dict
import numpy as np
import pandas as pd

from core.market_regime import detectar_regimen
from core.config.pesos import PESOS_SCORE_TECNICO, PESOS_SCORE_REGIMEN

# Estadísticas para normalización por símbolo
_STATS: Dict[str, Dict[str, float]] = {}
_ALPHA = 0.1


def _norm(value: float | None, min_val: float, max_val: float) -> float:
    """Normaliza ``value`` a un rango 0-1."""
    if value is None or np.isnan(value):
        return 0.5
    if max_val == min_val:
        return 0.5
    return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))


def _normalizar_por_symbol(symbol: str, score: float) -> float:
    """Aplica una normalización por símbolo usando media móvil."""
    stats = _STATS.get(symbol)
    if stats is None:
        _STATS[symbol] = {"avg": score, "std": 0.0}
        return score
    avg = stats["avg"] * (1 - _ALPHA) + score * _ALPHA
    std = stats["std"] * (1 - _ALPHA) + abs(score - avg) * _ALPHA
    stats["avg"] = avg
    stats["std"] = std
    if std == 0:
        return 0.5
    norm = (score - avg) / (2 * std) + 0.5
    return max(0.0, min(1.0, norm))


def calcular_score_tecnico(
    df: pd.DataFrame,
    rsi: Optional[float],
    momentum: Optional[float],
    slope: Optional[float],
    tendencia: str,
    *,
    symbol: str | None = None,
) -> float:
    """Calcula un score técnico continuo entre 0 y 1."""
    regimen = detectar_regimen(df)
    pesos = PESOS_SCORE_REGIMEN.get(regimen, PESOS_SCORE_TECNICO)
    total_pesos = sum(pesos.values()) or 1.0
    score = 0.0
    score += pesos.get("RSI", 0.0) * _norm(rsi, 30, 70)
    score += pesos.get("Momentum", 0.0) * _norm(momentum, -0.02, 0.02)
    score += pesos.get("Slope", 0.0) * _norm(slope, -0.001, 0.001)
    score += pesos.get("Tendencia", 0.0) * (1.0 if tendencia in {"alcista", "bajista"} else 0.5)
    score /= total_pesos

    if symbol:
        score = _normalizar_por_symbol(symbol, score)
    return round(score, 4)