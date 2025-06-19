"""Cálculo de umbral adaptativo para entradas."""
from __future__ import annotations

import json
from pathlib import Path

from typing import Dict, Optional

import pandas as pd

from indicadores import rsi as indicador_rsi, slope as indicador_slope

from core.logger import configurar_logger

log = configurar_logger("adaptador_umbral")

RUTA_CONFIG = Path("config/configuraciones_optimas.json")
_CONFIG_CACHE: Dict[str, dict] | None = None


def _cargar_config() -> Dict[str, dict]:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        if RUTA_CONFIG.exists():
            with open(RUTA_CONFIG, "r", encoding="utf-8") as fh:
                _CONFIG_CACHE = json.load(fh)
        else:
            _CONFIG_CACHE = {}
    return _CONFIG_CACHE


def calcular_score_tecnico(
    df: pd.DataFrame,
    rsi: float | None,
    momentum: float | None,
    slope: float | None,
    tendencia: str,
) -> float:
    """Calcula un puntaje técnico simple basado en indicadores."""
    score = 0.0
    if rsi is not None:
        if tendencia == "alcista" and rsi > 50:
            score += 1
        elif tendencia == "bajista" and rsi < 50:
            score += 1
        elif 45 <= rsi <= 55:
            score += 0.5
    if momentum is not None and momentum > 0:
        score += 1
    if slope is not None:
        if tendencia == "alcista" and slope > 0:
            score += 1
        elif tendencia == "bajista" and slope < 0:
            score += 1
    return score


def calcular_umbral_adaptativo(
    symbol: str,
    df: pd.DataFrame,
    contexto: Optional[Dict] = None,
) -> float:
    """Devuelve un umbral adaptativo basado en datos técnicos."""
    config = _cargar_config().get(symbol, {})
    factor = config.get("factor_umbral", 1.0)
    base = config.get("peso_minimo_total", 2.0)

    volatilidad = 0.0
    if df is not None and len(df) >= 20:
        cambios = df["close"].pct_change().dropna().tail(20)
        volatilidad = cambios.std() if not cambios.empty else 0.0

    rsi_val = contexto.get("rsi") if contexto else indicador_rsi(df)
    slope_val = contexto.get("slope") if contexto else indicador_slope(df)

    umbral = base * factor * (1 + volatilidad * 10)
    if slope_val is not None:
        umbral *= 1 + abs(slope_val)
    if rsi_val is not None:
        umbral *= 1 + abs(rsi_val - 50) / 100

    return round(max(1.0, umbral), 2)


def calcular_umbral_salida_adaptativo(symbol: str, config: Dict | None = None, contexto: Optional[Dict] = None) -> float:
    """Calcula un umbral dinámico para salidas basado en contexto de mercado."""
    if config is None:
        config = {}
    base = config.get("umbral_salida_base", 1.5)
    volatilidad = contexto.get("volatilidad", 1.0) if contexto else 1.0
    tendencia = contexto.get("tendencia", "lateral") if contexto else "lateral"
    factor_tend = 1.2 if tendencia in ["alcista", "bajista"] else 1.0
    umbral = base * volatilidad * factor_tend
    log.debug(
        f"[{symbol}] Umbral salida adaptativo: {umbral:.2f} | Base: {base} | Vol: {volatilidad:.3f} | Tend: {tendencia}"
    )
    return umbral