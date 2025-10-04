import pandas as pd
import numpy as np
from indicators.helpers import filtrar_cerradas, serie_cierres, sanitize_series
try:
    from numba import jit
except Exception:

    def jit(*args, **kwargs):

        def wrapper(func):
            return func
        return wrapper


@jit(nopython=True)
def _rsi_numba(close: np.ndarray, periodo: int) ->np.ndarray:
    """Calcula el RSI de forma acelerada usando numba."""
    n = close.shape[0]
    if n < periodo + 1:
        return np.empty(0, dtype=np.float64)
    delta = np.empty(n, dtype=np.float64)
    delta[0] = np.nan
    for i in range(1, n):
        delta[i] = close[i] - close[i - 1]
    gains = np.where(delta > 0, delta, 0.0)
    losses = np.where(delta < 0, -delta, 0.0)
    rsi = np.empty(n, dtype=np.float64)
    rsi[:] = np.nan
    alpha = 1.0 / periodo
    prev_gain = np.nan
    prev_loss = np.nan
    for i in range(1, n):
        cambio = close[i] - close[i - 1]
        gain = cambio if cambio > 0 else 0.0
        loss = -cambio if cambio < 0 else 0.0
        if np.isnan(prev_gain):
            prev_gain = gain
            prev_loss = loss
        else:
            prev_gain = (1.0 - alpha) * prev_gain + alpha * gain
            prev_loss = (1.0 - alpha) * prev_loss + alpha * loss
        if i >= periodo:
            rs = prev_gain / prev_loss
            rsi[i] = 100.0 - 100.0 / (1.0 + rs)
    return rsi


def calcular_rsi(
    data,
    periodo: int = 14,
    serie_completa: bool = False,
    adaptativo: bool = False,
    k: float = 1.0,
) -> float | pd.Series | tuple:
    """Calcula el RSI usando el método de Wilder (EMA).

    La serie de entrada se normaliza y se rellenan gaps para garantizar
    determinismo. El suavizado se realiza con medias exponenciales (EMA) y,
    opcionalmente, se calculan umbrales adaptativos ``media ± k·desviación``.
    """
    serie = serie_cierres(data)
    if serie is None or len(serie) < periodo + 1:
        return None
    serie = sanitize_series(serie, clip_min=0.0, clip_max=1.0)
    delta = serie.diff()
    ganancia = delta.clip(lower=0)
    perdida = -delta.clip(upper=0)
    avg_gain = ganancia.ewm(alpha=1 / periodo, adjust=False, min_periods=periodo).mean()
    avg_loss = perdida.ewm(alpha=1 / periodo, adjust=False, min_periods=periodo).mean()
    epsilon = 1e-10
    rs = avg_gain / (avg_loss + epsilon)
    rsi = (100 - 100 / (1 + rs)).clip(lower=0, upper=100).fillna(50.0)
    if adaptativo:
        media = rsi.mean()
        desv = rsi.std(ddof=0)
        umbral_alto = media + k * desv
        umbral_bajo = media - k * desv
        if serie_completa:
            return rsi, (umbral_bajo, umbral_alto)
        return rsi.iloc[-1] if not rsi.empty else None, (umbral_bajo, umbral_alto)
    if serie_completa:
        return rsi
    return rsi.iloc[-1] if not rsi.empty else None


def calcular_rsi_fast(
    df: pd.DataFrame,
    periodo: int = 14,
    serie_completa: bool = False,
    adaptativo: bool = False,
    k: float = 1.0,
) -> float | pd.Series | tuple:
    """Versión acelerada de :func:`calcular_rsi` usando numba si está disponible."""
    df = filtrar_cerradas(df)
    if 'close' not in df:
        return None
    serie = sanitize_series(df['close'], clip_min=0.0, clip_max=1.0)
    if len(serie) < periodo + 1:
        return None
    close = serie.to_numpy(dtype=float)
    valores = _rsi_numba(close, periodo)
    if valores.size == 0:
        return None
    rsi = pd.Series(np.clip(valores, 0.0, 100.0), index=serie.index).fillna(50.0)
    if adaptativo:
        media = rsi.mean()
        desv = rsi.std(ddof=0)
        umbral_alto = media + k * desv
        umbral_bajo = media - k * desv
        if serie_completa:
            return rsi, (umbral_bajo, umbral_alto)
        return float(rsi.iloc[-1]), (umbral_bajo, umbral_alto)
    if serie_completa:
        return rsi
    return float(rsi.iloc[-1])
