import pandas as pd
from indicators.momentum import calcular_momentum
from indicators.atr import calcular_atr
from indicators.slope import calcular_slope


def filtrar_cerradas(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve solo las filas con velas cerradas.

    Si la columna ``is_closed`` no está presente, se devuelve el ``DataFrame``
    original sin modificar.
    """
    if 'is_closed' in df.columns:
        return df[df['is_closed']]
    return df


def serie_cierres(data) -> pd.Series:
    """Obtiene la serie de cierres usando solo velas cerradas."""
    if isinstance(data, pd.DataFrame):
        return filtrar_cerradas(data)['close']
    return data


def _cached_value(df: pd.DataFrame, key: tuple, compute):
    cache = df.attrs.setdefault('_indicators_cache', {})
    if key not in cache:
        cache[key] = compute(df)
    return cache[key]


def clear_cache(df: pd.DataFrame) -> None:
    """Elimina el cache de indicadores asociado al DataFrame."""
    df.attrs.pop('_indicators_cache', None)


def get_rsi(data, periodo: int = 14, serie_completa: bool = False):
    """Obtiene el RSI con soporte para ``DataFrame`` o ``Series``."""
    from indicators.rsi import calcular_rsi
    if isinstance(data, pd.Series):
        return calcular_rsi(data, periodo, serie_completa)
    key = ('rsi', periodo, serie_completa)
    return _cached_value(data, key, lambda d: calcular_rsi(d, periodo, serie_completa))

def get_momentum(df: pd.DataFrame, periodo: int = 10):
    """Obtiene el momentum con cache simple."""
    key = ('momentum', periodo)
    return _cached_value(df, key, lambda d: calcular_momentum(d, periodo))


def get_atr(df: pd.DataFrame, periodo: int = 14):
    """Obtiene el ATR con cache simple."""
    key = ('atr', periodo)
    return _cached_value(df, key, lambda d: calcular_atr(d, periodo))


def get_slope(df: pd.DataFrame, periodo: int = 5):
    """Obtiene la pendiente (slope) con cache simple."""
    key = ('slope', periodo)
    return _cached_value(df, key, lambda d: calcular_slope(d, periodo))
