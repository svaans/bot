import pandas as pd

from indicadores.helpers import filtrar_cerradas


def calcular_vwap(df: pd.DataFrame) -> float:
    df = filtrar_cerradas(df)
    if not all(col in df for col in ["high", "low", "close", "volume"]) or len(df) < 2:
        return None
    df = df.copy()
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    vwap = (typical_price * df["volume"]).cumsum() / df["volume"].cumsum()
    return vwap.iloc[-1] if not vwap.empty else None
