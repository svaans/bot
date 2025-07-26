"""Cálculo de umbral adaptativo para entradas.

Este módulo calcula un umbral mínimo para validar señales de entrada. El
resultado se obtiene combinando factores derivados de la volatilidad,
la pendiente y el RSI actuales. Cada componente se calcula por separado
para facilitar la auditoría del proceso.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
from indicators import rsi as indicador_rsi, slope as indicador_slope
from core.score_tecnico import calcular_score_tecnico
from core.utils.utils import configurar_logger
log = configurar_logger("adaptador_umbral")
RUTA_CONFIG = Path("config/configuraciones_optimas.json")
_CONFIG_CACHE: Dict[str, dict] | None = None
_UMBRAL_SUAVIZADO: Dict[str, float] = {}
ALPHA = 0.3


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
    """Devuelve un umbral adaptativo basado en datos técnicos actuales."""
    config = _cargar_config().get(symbol, {})
    base = config.get("peso_minimo_total", 2.0)
    factor = config.get("factor_umbral", 1.0)

    def _factor_volatilidad() -> float:
        if df is None or len(df) < 20:
            return 1.0
        cambios = df["close"].pct_change().dropna().tail(20)
        vol = cambios.std() if not cambios.empty else 0.0
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
    previo = _UMBRAL_SUAVIZADO.get(symbol)
    if previo is None:
        suavizado = umbral
    else:
        suavizado = previo + ALPHA * (umbral - previo)
    _UMBRAL_SUAVIZADO[symbol] = suavizado
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
