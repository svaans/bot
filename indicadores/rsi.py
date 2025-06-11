import pandas as pd
import numpy as np

try:
    from numba import jit
except Exception:  # pragma: no cover - numba es opcional
    def jit(*args, **kwargs):  # type: ignore
        def wrapper(func):
            return func

        return wrapper


@jit(nopython=True)
def _rsi_numba(close: np.ndarray, periodo: int) -> np.ndarray:
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
    df: pd.DataFrame, periodo: int = 14, serie_completa: bool = False
) -> float | pd.Series:
    """Calcula el RSI usando el método de Wilder (EMA).

    Si ``serie_completa`` es ``True`` devuelve la serie completa del RSI.
    En caso contrario se retorna solo el último valor.
    """

    if "close" not in df or len(df) < periodo + 1:
        return None

    delta = df["close"].diff()
    ganancia = delta.clip(lower=0)
    perdida = -delta.clip(upper=0)

    avg_gain = ganancia.ewm(
        alpha=1 / periodo, adjust=False, min_periods=periodo
    ).mean()
    avg_loss = perdida.ewm(
        alpha=1 / periodo, adjust=False, min_periods=periodo
    ).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - 100 / (1 + rs)

    if serie_completa:
        return rsi
    return rsi.iloc[-1] if not rsi.empty else None


def calcular_rsi_fast(
    df: pd.DataFrame, periodo: int = 14, serie_completa: bool = False
) -> float | pd.Series:
    """Versión acelerada de :func:`calcular_rsi` usando numba si está disponible."""
    if "close" not in df or len(df) < periodo + 1:
        return None

    close = df["close"].to_numpy(dtype=float)
    valores = _rsi_numba(close, periodo)
    if valores.size == 0:
        return None

    if serie_completa:
        return pd.Series(valores, index=df.index)
    return float(valores[-1])
