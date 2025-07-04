"""Validadores de condiciones técnicas para entradas."""
from __future__ import annotations
import pandas as pd
from indicators.slope import calcular_slope
from indicators.bollinger import calcular_bollinger
from indicators.rsi import calcular_rsi


def _distancia_relativa(valor_actual: float, extremo: float) ->float:
    """Devuelve la distancia relativa entre ``valor_actual`` y ``extremo``."""
    if extremo == 0:
        return 0.0
    return abs(valor_actual - extremo) / extremo


def validar_volumen(df: pd.DataFrame) ->bool:
    """Valida que el volumen actual sea suficiente."""
    if df is None or len(df) < 30:
        return True
    vol_act = float(df['volume'].iloc[-1])
    vol_med = float(df['volume'].rolling(30).mean().iloc[-1])
    if vol_med == 0:
        return True
    return vol_act / vol_med >= 0.9


def validar_rsi(df: pd.DataFrame, direccion: str='long') ->bool:
    valor = calcular_rsi(df)
    if valor is None:
        return True
    if valor > 75:
        return False
    if direccion == 'short' and valor < 30:
        return False
    return True


def validar_slope(df: pd.DataFrame, tendencia: (str | None)) ->bool:
    pendiente = calcular_slope(df)
    if tendencia == 'alcista' and pendiente < 0:
        return False
    if tendencia == 'bajista' and pendiente > 0:
        return False
    if abs(pendiente) < 0.01:
        return False
    return True


def validar_bollinger(df: pd.DataFrame) ->bool:
    _, banda_sup, precio = calcular_bollinger(df)
    if banda_sup is None or precio is None:
        return True
    return abs(banda_sup - precio) / precio >= 0.01


def validar_max_min(df: pd.DataFrame, ventana: int=30, umbral: float=0.005
    ) ->bool:
    """Evita operar cerca de máximos o mínimos recientes."""
    if df is None or len(df) < ventana:
        return True
    precio = float(df['close'].iloc[-1])
    max_rec = float(df['high'].rolling(ventana).max().iloc[-1])
    min_rec = float(df['low'].rolling(ventana).min().iloc[-1])
    dist_max = _distancia_relativa(precio, max_rec)
    dist_min = _distancia_relativa(precio, min_rec)
    return dist_max > umbral and dist_min > umbral


def validar_volumen_real(df: pd.DataFrame, factor: float=1.0, ventana: int=30
    ) ->bool:
    """Comprueba que el volumen sea alto respecto a su media."""
    if df is None or len(df) < ventana:
        return True
    vol_act = float(df['volume'].iloc[-1])
    vol_med = float(df['volume'].tail(ventana).mean())
    if vol_med == 0:
        return True
    return vol_act >= vol_med * factor


def validar_spread(df: pd.DataFrame, max_spread: float=0.002) ->bool:
    """Valida que la vela actual no tenga un spread excesivo."""
    if df is None or df.empty:
        return True
    alto = float(df['high'].iloc[-1])
    bajo = float(df['low'].iloc[-1])
    cierre = float(df['close'].iloc[-1])
    if cierre == 0:
        return True
    spread = (alto - bajo) / cierre
    return spread <= max_spread
