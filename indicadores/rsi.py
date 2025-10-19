"""Implementaciones del Relative Strength Index (RSI)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from indicadores.helpers import filtrar_cerradas, serie_cierres, sanitize_series
try:
    from numba import jit
except Exception:

    def jit(*args, **kwargs):

        def wrapper(func):
            return func
        return wrapper


@jit(nopython=True)
def _rsi_numba(close: np.ndarray, periodo: int) -> np.ndarray:
    """Calcula el RSI con el método de Wilder usando ``numba``.

    El algoritmo replica el cálculo tradicional: se obtienen las variaciones
    de precio, se calculan medias suavizadas de ganancias y pérdidas y se
    deriva el valor del RSI a partir de la relación ``RS``. Los primeros
    ``periodo`` valores permanecen en ``NaN`` para igualar el comportamiento de
    las bibliotecas de referencia.
    """
    n = close.shape[0]
    if n < periodo + 1:
        return np.empty(0, dtype=np.float64)
    
    rsi = np.empty(n, dtype=np.float64)
    rsi[:] = np.nan

    delta = np.empty(n, dtype=np.float64)
    delta[0] = 0.0
    for i in range(1, n):
        delta[i] = close[i] - close[i - 1]
    gains = np.empty(n, dtype=np.float64)
    losses = np.empty(n, dtype=np.float64)
    for i in range(n):
        value = delta[i]
        if value > 0:
            gains[i] = value
            losses[i] = 0.0
        elif value < 0:
            gains[i] = 0.0
            losses[i] = -value
        else:
            gains[i] = 0.0
            losses[i] = 0.0
    alpha = 1.0 / periodo
    avg_gain = np.empty(n, dtype=np.float64)
    avg_loss = np.empty(n, dtype=np.float64)
    avg_gain[0] = gains[0]
    avg_loss[0] = losses[0]
    for i in range(1, n):
        prev_gain = avg_gain[i - 1]
        prev_loss = avg_loss[i - 1]
        avg_gain[i] = (1.0 - alpha) * prev_gain + alpha * gains[i]
        avg_loss[i] = (1.0 - alpha) * prev_loss + alpha * losses[i]

    epsilon = 1e-10
    for i in range(periodo - 1, n):
        loss = avg_loss[i]
        if np.isnan(loss):
            continue
        if loss <= epsilon:
            rsi[i] = 100.0
        else:
            rs = avg_gain[i] / (loss + epsilon)
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
    serie = sanitize_series(serie, normalize=False, clip_min=None, clip_max=None)
    delta = serie.diff()
    ganancia = delta.where(delta > 0, 0.0)
    perdida = -delta.where(delta < 0, 0.0)
    avg_gain = ganancia.ewm(alpha=1 / periodo, adjust=False, min_periods=periodo).mean()
    avg_loss = perdida.ewm(alpha=1 / periodo, adjust=False, min_periods=periodo).mean()
    epsilon = 1e-10
    rs = avg_gain / (avg_loss + epsilon)
    rsi = pd.Series(
        np.where(avg_loss <= epsilon, 100.0, 100.0 - 100.0 / (1.0 + rs)),
        index=serie.index,
        name='rsi',
    )
    if adaptativo:
        valores = rsi.dropna()
        if valores.empty:
            return (None, (None, None)) if not serie_completa else (rsi, (None, None))
        media = valores.mean()
        desv = valores.std(ddof=0)
        umbral_alto = media + k * desv
        umbral_bajo = media - k * desv
        if serie_completa:
            return rsi, (umbral_bajo, umbral_alto)
        ultimo = rsi.iloc[-1]
        return (float(ultimo) if pd.notna(ultimo) else None, (umbral_bajo, umbral_alto))
    if serie_completa:
        return rsi
    ultimo = rsi.iloc[-1]
    return float(ultimo) if pd.notna(ultimo) else None


def calcular_rsi_fast(
    df: pd.DataFrame,
    periodo: int = 14,
    serie_completa: bool = False,
    adaptativo: bool = False,
    k: float = 1.0,
) -> float | pd.Series | tuple:
    """Versión acelerada de :func:`calcular_rsi` usando numba si está disponible."""
    df = filtrar_cerradas(df)
    if 'close' not in df.columns:
        return None
    serie = sanitize_series(df['close'], normalize=False, clip_min=None, clip_max=None)
    if len(serie) < periodo + 1:
        return None
    close = serie.to_numpy(dtype=float)
    valores = _rsi_numba(close, periodo)
    if valores.size == 0:
        return None
    rsi = pd.Series(np.clip(valores, 0.0, 100.0), index=serie.index, name='rsi')
    if adaptativo:
        valores_validos = rsi.dropna()
        if valores_validos.empty:
            return (None, (None, None)) if not serie_completa else (rsi, (None, None))
        media = valores_validos.mean()
        desv = valores_validos.std(ddof=0)
        umbral_alto = media + k * desv
        umbral_bajo = media - k * desv
        if serie_completa:
            return rsi, (umbral_bajo, umbral_alto)
        ultimo = rsi.iloc[-1]
        return (float(ultimo) if pd.notna(ultimo) else None, (umbral_bajo, umbral_alto))
    if serie_completa:
        return rsi
    ultimo = rsi.iloc[-1]
    return float(ultimo) if pd.notna(ultimo) else None
