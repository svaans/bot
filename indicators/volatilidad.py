import pandas as pd
import numpy as np


def calcular_volatility_breakout(df: pd.DataFrame, ventana: int=14, factor:
    float=1.5) ->bool:
    if 'high' not in df or 'low' not in df or 'close' not in df or len(df
        ) < ventana + 1:
        return False
    df = df.copy()
    rango = df['high'] - df['low']
    rango_medio = rango.rolling(window=ventana).mean()
    breakout = rango.iloc[-1] > rango_medio.iloc[-2] * factor
    return breakout


def calcular_volatilidad_normalizada(df: pd.DataFrame, ventana: int = 14) -> float:
    """Calcula la volatilidad normalizada de los retornos del cierre."""
    if not isinstance(df, pd.DataFrame) or 'close' not in df:
        return 0.0

    df_filtrado = df[df['is_closed']] if 'is_closed' in df else df
    retornos = df_filtrado['close'].pct_change().dropna()
    if len(retornos) < ventana:
        return 0.0

    serie = retornos.tail(ventana)
    volatilidad = serie.std()
    max_vol = serie.abs().max()
    if max_vol == 0 or not np.isfinite(volatilidad):
        return 0.0
    vol_norm = volatilidad / max_vol
    if not np.isfinite(vol_norm):
        return 0.0
    return float(np.clip(vol_norm, 0.0, 1.0))
