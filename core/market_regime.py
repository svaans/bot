import pandas as pd
from indicators.helpers import get_atr, get_slope


def medir_volatilidad(df: pd.DataFrame, periodo: int=14) ->float:
    """Devuelve la volatilidad relativa usando ATR."""
    atr = get_atr(df, periodo)
    if atr is None:
        return 0.0
    cierre = float(df['close'].iloc[-1]) if 'close' in df else 0.0
    if cierre == 0:
        return 0.0
    return float(atr) / cierre


def pendiente_medias(df: pd.DataFrame, ventana: int=30) ->float:
    """Calcula la pendiente normalizada de una media móvil."""
    if 'close' not in df or len(df) < ventana + 5:
        return 0.0
    sma = df['close'].rolling(window=ventana).mean().dropna()
    if sma.empty:
        return 0.0
    slope = get_slope(pd.DataFrame({'close': sma}))
    base = sma.iloc[-1]
    if base == 0:
        return 0.0
    return slope / base


def detectar_regimen(df: pd.DataFrame) ->str:
    """Clasifica el régimen de mercado según volatilidad y pendiente."""
    vol = medir_volatilidad(df)
    slope = pendiente_medias(df)
    if vol > 0.02:
        return 'alta_volatilidad'
    if abs(slope) > 0.001:
        return 'tendencial'
    return 'lateral'
