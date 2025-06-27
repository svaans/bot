import pandas as pd
import numpy as np

from fast_indicators import rsi as _rsi_fast

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
    """Versión acelerada de :func:`calcular_rsi` usando la extensión en C++."""
    if "close" not in df or len(df) < periodo + 1:
        return None

    close = df["close"].to_numpy(dtype=float)
    valores = _rsi_fast(close, periodo)
    if valores.size == 0:
        return None

    if serie_completa:
        return pd.Series(valores, index=df.index)
    return float(valores[-1])
