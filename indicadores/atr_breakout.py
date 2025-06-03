
import pandas as pd

def calcular_atr_breakout(df: pd.DataFrame, periodo: int = 14) -> bool:
    if not all(col in df for col in ["high", "low", "close"]) or len(df) < periodo + 1:
        return False

    df = df.copy()
    df["tr"] = df[["high", "low", "close"]].max(axis=1) - df[["high", "low", "close"]].min(axis=1)
    atr = df["tr"].rolling(window=periodo).mean().iloc[-1]

    return df["close"].iloc[-1] > df["close"].iloc[-2] + atr
