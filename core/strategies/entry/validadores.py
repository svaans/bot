"""Validadores de condiciones técnicas para entradas."""
from __future__ import annotations

import pandas as pd

from indicators.slope import calcular_slope
from indicators.bollinger import calcular_bollinger
from indicators.rsi import calcular_rsi



def _distancia_relativa(valor_actual: float, extremo: float) -> float:
    """Devuelve la distancia relativa entre ``valor_actual`` y ``extremo``."""
    if extremo == 0:
        return 0.0
    return abs(valor_actual - extremo) / extremo

def validar_volumen(df: pd.DataFrame) -> float:
    """Devuelve un puntaje de confianza basado en el volumen relativo."""
    if df is None or len(df) < 30:
        return 1.0
    vol_act = float(df["volume"].iloc[-1])
    ventana = df["volume"].rolling(30)
    vol_med = float(ventana.mean().iloc[-1])
    if vol_med == 0:
        return 1.0
    vol_std = float(ventana.std().iloc[-1])
    den = vol_med + vol_std
    ratio = vol_act / den if den > 0 else 1.0
    return max(0.0, min(1.0, ratio))


def validar_rsi(df: pd.DataFrame, direccion: str = "long") -> float:
    """Evalúa el RSI devolviendo un nivel de confianza."""
    valor = calcular_rsi(df)
    if valor is None:
        return 1.0
    
    if valor > 80:
        return 0.0
    if direccion == "short" and valor < 20:
        return 0.0

    if direccion == "short":
        if valor < 30:
            return (valor - 20) / 10
        return 1.0

    if valor > 70:
        return max(0.0, (80 - valor) / 10)
    return 1.0


def validar_slope(df: pd.DataFrame, tendencia: str | None) -> float:
    """Devuelve la confianza basada en la pendiente y la tendencia esperada."""
    pendiente = calcular_slope(df)
    if pendiente is None:
        return 1.0
    if tendencia == "alcista" and pendiente < 0:
        return 0.0
    if tendencia == "bajista" and pendiente > 0:
        return 0.0
    intensidad = min(abs(pendiente) / 0.02, 1.0)
    return intensidad


def validar_bollinger(df: pd.DataFrame) -> float:
    """Evalúa la distancia a la banda superior como indicador de sobrecompra."""
    _, banda_sup, precio = calcular_bollinger(df)
    if banda_sup is None or precio is None:
        return 1.0
    dist = abs(banda_sup - precio) / precio
    return max(0.0, min(1.0, dist / 0.01))


def validar_max_min(
    df: pd.DataFrame, ventana: int = 30, umbral: float = 0.005
) -> float:
    """Retorna confianza basada en la distancia a máximos y mínimos recientes."""
    if df is None or len(df) < ventana:
        return 1.0
    precio = float(df["close"].iloc[-1])
    max_rec = float(df["high"].rolling(ventana).max().iloc[-1])
    min_rec = float(df["low"].rolling(ventana).min().iloc[-1])
    dist_max = _distancia_relativa(precio, max_rec)
    dist_min = _distancia_relativa(precio, min_rec)
    valor = min(dist_max, dist_min) / umbral
    return max(0.0, min(1.0, valor))


def validar_volumen_real(
    df: pd.DataFrame, factor: float | None = None, ventana: int = 30
) -> float:
    """Calcula la confianza basada en el volumen real respecto a su media."""
    if df is None or len(df) < ventana:
        return 1.0
    vol_act = float(df["volume"].iloc[-1])
    ventana_vol = df["volume"].tail(ventana)
    vol_med = float(ventana_vol.mean())
    if vol_med == 0:
        return 1.0
    
    if factor is None:
        vol_std = float(ventana_vol.std())
        coef = vol_std / vol_med
        factor = max(0.8, min(1.0 + coef, 1.5))
        
    ratio = vol_act / (vol_med * factor)
    return max(0.0, min(1.0, ratio))


def validar_spread(df: pd.DataFrame, max_spread: float = 0.002) -> float:
    """Retorna confianza inversa al spread de la vela actual."""
    if df is None or df.empty:
        return 1.0
    alto = float(df["high"].iloc[-1])
    bajo = float(df["low"].iloc[-1])
    cierre = float(df["close"].iloc[-1])
    if cierre == 0:
        return 1.0
    spread = (alto - bajo) / cierre
    return max(0.0, min(1.0, 1 - spread / max_spread))