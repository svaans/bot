import numpy as np
import pandas as pd
from indicadores.helpers import filtrar_cerradas
try:
    from numba import jit
except Exception:

    def jit(*args, **kwargs):

        def wrapper(func):
            return func
        return wrapper


@jit(nopython=True)
def _ema_numba(valores: np.ndarray, periodo: int) ->np.ndarray:
    alpha = 2.0 / (periodo + 1.0)
    resultado = np.empty_like(valores)
    resultado[0] = valores[0]
    for i in range(1, valores.size):
        resultado[i] = alpha * valores[i] + (1 - alpha) * resultado[i - 1]
    return resultado


def calcular_cruce_ema(df: pd.DataFrame, rapida=12, lenta=26) ->bool:
    df = filtrar_cerradas(df)
    if len(df) < lenta + 2 or 'close' not in df:
        return False
    ema_fast = df['close'].ewm(span=rapida, adjust=False).mean()
    ema_slow = df['close'].ewm(span=lenta, adjust=False).mean()
    cruce_anterior = ema_fast.iloc[-2] < ema_slow.iloc[-2]
    cruce_actual = ema_fast.iloc[-1] > ema_slow.iloc[-1]
    return cruce_anterior and cruce_actual


def calcular_cruce_ema_fast(df: pd.DataFrame, rapida=12, lenta=26) ->bool:
    """Versión acelerada de :func:`calcular_cruce_ema` usando numba."""
    df = filtrar_cerradas(df)
    if len(df) < lenta + 2 or 'close' not in df:
        return False
    close = df['close'].to_numpy(dtype=float)
    ema_fast = _ema_numba(close, rapida)
    ema_slow = _ema_numba(close, lenta)
    cruce_anterior = ema_fast[-2] < ema_slow[-2]
    cruce_actual = ema_fast[-1] > ema_slow[-1]
    return cruce_anterior and cruce_actual
