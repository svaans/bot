import pandas as pd

def calcular_rsi(df: pd.DataFrame, periodo: int = 14) -> float:
    if "close" not in df or len(df) < periodo:
        return None

    delta = df["close"].diff()
    ganancia = delta.where(delta > 0, 0.0)
    perdida = -delta.where(delta < 0, 0.0)
    media_ganancia = ganancia.rolling(window=periodo).mean()
    media_perdida = perdida.rolling(window=periodo).mean()
    rs = media_ganancia / media_perdida
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not rsi.empty else None
