"""Cálculo de umbral adaptativo para entradas."""
from __future__ import annotations

import json
from pathlib import Path

from typing import Dict, Optional

import pandas as pd

from indicators import rsi as indicador_rsi, slope as indicador_slope
from core.scoring import calcular_score_tecnico
from core.utils.utils import configurar_logger
from core.utils.umbral_helper import calcular_umbral_avanzado

log = configurar_logger("adaptador_umbral")

RUTA_CONFIG = Path("config/configuraciones_optimas.json")
_CONFIG_CACHE: Dict[str, dict] | None = None

_UMBRAL_SUAVIZADO: Dict[str, float] = {}
ALPHA = 0.3


def _cargar_config() -> Dict[str, dict]:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        if RUTA_CONFIG.exists():
            with open(RUTA_CONFIG, "r", encoding="utf-8") as fh:
                _CONFIG_CACHE = json.load(fh)
        else:
            _CONFIG_CACHE = {}
    return _CONFIG_CACHE





def calcular_umbral_adaptativo(
    symbol: str,
    df: pd.DataFrame,
    contexto: Optional[Dict] = None,
    *,
    alpha: float | None = None,
) -> float:
    """Devuelve un umbral adaptativo basado en datos técnicos."""
    config = _cargar_config().get(symbol, {})
    estrategias = contexto.get("estrategias_activas", {}) if contexto else {}
    pesos_symbol = contexto.get("pesos_symbol", {}) if contexto else {}
    persistencia = contexto.get("persistencia", 0.0) if contexto else 0.0

    umbral = calcular_umbral_avanzado(
        symbol,
        df,
        estrategias,
        pesos_symbol,
        persistencia=persistencia,
        config=config,
    )

    alpha = alpha if alpha is not None else config.get("alpha", ALPHA)
    previo = _UMBRAL_SUAVIZADO.get(symbol)
    suavizado = umbral if previo is None else previo + alpha * (umbral - previo)
    _UMBRAL_SUAVIZADO[symbol] = suavizado
    return round(suavizado, 2)


def calcular_umbral_salida_adaptativo(
    symbol: str,
    df: pd.DataFrame,
    config: Dict | None = None,
    contexto: Optional[Dict] = None,
    *,
    alpha: float | None = None,
) -> float:
    """Calcula un umbral dinámico para salidas basado en contexto de mercado."""
    if config is None:
        config = {}
    estrategias = contexto.get("estrategias_activas", {}) if contexto else {}
    pesos_symbol = contexto.get("pesos_symbol", {}) if contexto else {}
    persistencia = contexto.get("persistencia", 0.0) if contexto else 0.0

    config_local = {
        "factor_umbral": config.get("factor_umbral_salida", 1.0),
        "peso_minimo_total": config.get("umbral_salida_base", 1.5),
    }

    umbral = calcular_umbral_avanzado(
        symbol,
        df,
        estrategias,
        pesos_symbol,
        persistencia=persistencia,
        config=config_local,
    )

    alpha = alpha if alpha is not None else config.get("alpha", ALPHA)
    previo = _UMBRAL_SUAVIZADO.get(f"exit_{symbol}")
    suavizado = umbral if previo is None else previo + alpha * (umbral - previo)
    _UMBRAL_SUAVIZADO[f"exit_{symbol}"] = suavizado
    log.debug(
        f"[{symbol}] Umbral salida adaptativo: {suavizado:.2f} | Base: {config_local['peso_minimo_total']}"
    )
    return round(suavizado, 2)