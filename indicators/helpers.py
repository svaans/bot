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


def sanitize_series(
    serie: pd.Series,
    *,
    sort: bool = True,
    fill_method: str = 'ffill',
    clip_min: float | None = None,
    clip_max: float | None = None,
    normalize: bool = True,
) -> pd.Series:
    """Normaliza y limpia una serie numérica para garantizar determinismo.

    - Convierte a ``float`` y opcionalmente ordena por índice para evitar
      efectos de reordenamiento.
    - Rellena huecos según ``fill_method`` (``ffill`` por defecto) para manejar
      gaps de datos.
    - Si ``normalize`` es ``True`` aplica una normalización min-max.
    - Opcionalmente clipea los valores entre ``clip_min`` y ``clip_max``.
    """
    serie = serie.astype(float)
    if sort:
        serie = serie.sort_index()
    if fill_method == 'ffill':
        serie = serie.ffill().bfill()
    elif fill_method == 'interpolate':
        serie = serie.interpolate().ffill().bfill()
    if normalize and not serie.empty:
        minimo = serie.min()
        maximo = serie.max()
        rango = maximo - minimo
        if rango != 0:
            serie = (serie - minimo) / rango
        else:
            serie = serie - minimo
    if clip_min is not None or clip_max is not None:
        serie = serie.clip(lower=clip_min, upper=clip_max)
    return serie


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
