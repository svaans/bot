import pandas as pd
from indicators.rsi import calcular_rsi
from indicators.momentum import calcular_momentum
from indicators.atr import calcular_atr


def _cached_value(df: pd.DataFrame, key: tuple, compute):
    cache = df.attrs.setdefault('_indicators_cache', {})
    if key not in cache:
        cache[key] = compute(df)
    return cache[key]


def get_rsi(df: pd.DataFrame, periodo: int = 14, serie_completa: bool = False):
    """Obtiene el RSI utilizando cache por DataFrame."""
    key = ('rsi', periodo, serie_completa)
    return _cached_value(df, key, lambda d: calcular_rsi(d, periodo, serie_completa))


def get_momentum(df: pd.DataFrame, periodo: int = 10):
    """Obtiene el momentum con cache simple."""
    key = ('momentum', periodo)
    return _cached_value(df, key, lambda d: calcular_momentum(d, periodo))


def get_atr(df: pd.DataFrame, periodo: int = 14):
    """Obtiene el ATR con cache simple."""
    key = ('atr', periodo)
    return _cached_value(df, key, lambda d: calcular_atr(d, periodo))
