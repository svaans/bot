import pandas as pd

def calcular_atr(df: pd.DataFrame, periodo: int = 14):
    df = df.copy()
    df["hl"] = df["high"] - df["low"]
    df["hc"] = abs(df["high"] - df["close"].shift(1))
    df["lc"] = abs(df["low"] - df["close"].shift(1))
    df["tr"] = df[["hl", "hc", "lc"]].max(axis=1)
    atr = df["tr"].rolling(window=periodo).mean()
    return atr