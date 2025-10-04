import pandas as pd
from math import isclose
from indicadores.helpers import get_atr
from core.utils.market_utils import calcular_slope_pct


def medir_volatilidad(df: pd.DataFrame, periodo: int=14) ->float:
    """Devuelve la volatilidad relativa usando ATR."""
    atr = get_atr(df, periodo)
    if atr is None:
        return 0.0
    cierre = float(df['close'].iloc[-1]) if 'close' in df else 0.0
    if isclose(cierre, 0.0, rel_tol=1e-12, abs_tol=1e-12):
        return 0.0
    return float(atr) / cierre


def pendiente_medias(df: pd.DataFrame, ventana: int=30) ->float:
    """Calcula la pendiente porcentual de una media móvil."""
    if 'close' not in df or len(df) < ventana + 5:
        return 0.0
    sma = df['close'].rolling(window=ventana).mean().dropna()
    if len(sma) < 2:
        return 0.0
    return calcular_slope_pct(sma)


def detectar_regimen(df: pd.DataFrame) ->str:
    """Clasifica el régimen de mercado según volatilidad y pendiente."""
    vol = medir_volatilidad(df)
    slope_pct = pendiente_medias(df)
    if vol > 0.02:
        return 'alta_volatilidad'
    if abs(slope_pct) > 0.002:
        return 'tendencial'
    return 'lateral'
