import pandas as pd
"""Funciones de análisis adicional para salidas."""
from indicadores.ema import calcular_cruce_ema


def _patron_reversion(df: pd.DataFrame) ->bool:
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    cuerpo_prev = prev['close'] - prev['open']
    cuerpo_curr = curr['close'] - curr['open']
    engulfing = cuerpo_prev < 0 and cuerpo_curr > 0 and curr['close'] >= prev[
        'open'] and curr['open'] <= prev['close']
    rango = curr['high'] - curr['low']
    mecha_inf = min(curr['open'], curr['close']) - curr['low']
    hammer = cuerpo_curr > 0 and rango > 0 and mecha_inf > cuerpo_curr * 2
    return engulfing or hammer


def _volumen_extremo(df: pd.DataFrame, ventana: int=20) ->bool:
    if 'volume' not in df or len(df) < ventana:
        return False
    ventana_vol = df['volume'].tail(ventana)
    return ventana_vol.iloc[-1] > ventana_vol.mean() * 1.5


def patron_tecnico_fuerte(df: pd.DataFrame) ->bool:
    """Detecta señales fuertes para evitar un cierre."""
    return _patron_reversion(df) or _volumen_extremo(df) or calcular_cruce_ema(
        df)
