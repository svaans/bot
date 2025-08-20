"""Cálculo de umbral adaptativo para entradas.

Este módulo calcula un umbral mínimo para validar señales de entrada. El
resultado se obtiene combinando factores derivados de la volatilidad,
la pendiente y el RSI actuales. Cada componente se calcula por separado
para facilitar la auditoría del proceso.
"""
from __future__ import annotations
import json
import math
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
from prometheus_client import Counter, Gauge
from indicators import rsi as indicador_rsi, slope as indicador_slope
from indicators.retornos_volatilidad import (
    retornos_log,
    retornos_simples,
    verificar_consistencia,
    volatilidad_welford,
)
from core.utils.utils import configurar_logger
log = configurar_logger("adaptador_umbral")
RUTA_CONFIG = Path("config/configuraciones_optimas.json")
_CONFIG_CACHE: Dict[str, dict] | None = None
_UMBRAL_SUAVIZADO: Dict[str, float] = {}
ALPHA = 0.3
HISTERESIS = 0.05

UMBRAL_ADAPTATIVO_ACTUAL = Gauge(
    "umbral_adaptativo_actual",
    "Valor actual del umbral adaptativo",
    ["symbol"],
)
UMBRAL_HISTERESIS_SKIPS = Counter(
    "umbral_histeresis_skips_total",
    "Actualizaciones omitidas por la banda de histéresis",
    ["symbol"],
)


def _cargar_config() -> Dict[str, dict]:
    log.info("➡️ Entrando en _cargar_config()")
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        if RUTA_CONFIG.exists():
            with open(RUTA_CONFIG, "r", encoding="utf-8") as fh:
                _CONFIG_CACHE = json.load(fh)
        else:
            _CONFIG_CACHE = {}
    return _CONFIG_CACHE


def calcular_umbral_adaptativo(
    symbol: str, df: pd.DataFrame, contexto: Optional[Dict] = None
) -> float:
    log.info("➡️ Entrando en calcular_umbral_adaptativo()")
    """Devuelve un umbral adaptativo basado en datos técnicos actuales.

    El cálculo parte de los pesos configurados y aplica multiplicadores
    según tres indicadores:

    - **Volatilidad**: cuanto mayor sea, más se incrementa el umbral para
      filtrar entradas en mercados agitados.
    - **Slope**: la pendiente del precio actúa como factor de impulso, tanto
      al alza como a la baja.
    - **RSI**: alejarse de la zona neutra (50) aumenta la exigencia del
      umbral.
    """
    config = _cargar_config().get(symbol, {})
    base = config.get("peso_minimo_total", 0.5)
    factor = config.get("factor_umbral", 1.0)

    def _factor_volatilidad() -> float:
        if df is None or len(df) < 21 or "close" not in df:
            return 1.0
        precios = df["close"].tail(21)
        simples = retornos_simples(precios)
        logs = retornos_log(precios)
        if not verificar_consistencia(simples, logs):
            return 1.0
        vol = volatilidad_welford(simples.tail(20))
        return 1 + vol * 10

    def _factor_slope(valor: float | None) -> float:
        return 1 + abs(valor) if valor is not None else 1.0

    def _factor_rsi(valor: float | None) -> float:
        return 1 + abs(valor - 50) / 100 if valor is not None else 1.0

    rsi_val = contexto.get("rsi") if contexto else indicador_rsi(df)
    slope_val = contexto.get("slope") if contexto else indicador_slope(df)

    umbral = base * factor
    umbral *= _factor_volatilidad()
    umbral *= _factor_slope(slope_val)
    umbral *= _factor_rsi(rsi_val)
    umbral = max(1.0, umbral)
    if pd.isna(umbral) or not math.isfinite(umbral):
        return 1.0
    previo = _UMBRAL_SUAVIZADO.get(symbol)
    if previo is None:
        suavizado = umbral
    else:
        suavizado = previo + ALPHA * (umbral - previo)
    if pd.isna(suavizado) or not math.isfinite(suavizado):
        return 1.0

    if previo is not None and abs(suavizado - previo) < HISTERESIS:
        UMBRAL_HISTERESIS_SKIPS.labels(symbol=symbol).inc()
        UMBRAL_ADAPTATIVO_ACTUAL.labels(symbol=symbol).set(previo)
        return round(previo, 2)
    _UMBRAL_SUAVIZADO[symbol] = suavizado
    UMBRAL_ADAPTATIVO_ACTUAL.labels(symbol=symbol).set(suavizado)
    return round(suavizado, 2)


def calcular_umbral_salida_adaptativo(
    symbol: str, config: Dict | None = None, contexto: Optional[Dict] = None
) -> float:
    log.info("➡️ Entrando en calcular_umbral_salida_adaptativo()")
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
