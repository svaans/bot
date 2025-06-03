import pandas as pd

def calcular_momentum(df: pd.DataFrame, periodo: int = 10) -> float:
    if "close" not in df or len(df) < periodo + 1:
        return None

    return df["close"].iloc[-1] - df["close"].iloc[-periodo]

