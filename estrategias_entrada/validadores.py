"""Validadores de condiciones técnicas para entradas."""
from __future__ import annotations

import pandas as pd

from indicadores.slope import calcular_slope
from indicadores.bollinger import calcular_bollinger
from indicadores.rsi import calcular_rsi


def validar_volumen(df: pd.DataFrame) -> bool:
    """Valida que el volumen actual sea suficiente."""
    if df is None or len(df) < 30:
        return True
    vol_act = float(df["volume"].iloc[-1])
    vol_med = float(df["volume"].rolling(30).mean().iloc[-1])
    if vol_med == 0:
        return True
    return (vol_act / vol_med) >= 0.9


def validar_rsi(df: pd.DataFrame, direccion: str = "long") -> bool:
    valor = calcular_rsi(df)
    if valor is None:
        return True
    if valor > 75:
        return False
    if direccion == "short" and valor < 30:
        return False
    return True


def validar_slope(df: pd.DataFrame, tendencia: str | None) -> bool:
    pendiente = calcular_slope(df)
    if tendencia == "alcista" and pendiente < 0:
        return False
    if tendencia == "bajista" and pendiente > 0:
        return False
    if abs(pendiente) < 0.01:
        return False
    return True


def validar_bollinger(df: pd.DataFrame) -> bool:
    _, banda_sup, precio = calcular_bollinger(df)
    if banda_sup is None or precio is None:
        return True
    return abs(banda_sup - precio) / precio >= 0.01