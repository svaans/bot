import pandas as pd

def calcular_cruce_sma(df: pd.DataFrame, rapida=20, lenta=50) -> bool:
    if "close" not in df or len(df) < lenta:
        return False

    sma_rapida = df["close"].rolling(window=rapida).mean()
    sma_lenta = df["close"].rolling(window=lenta).mean()

    return sma_rapida.iloc[-1] > sma_lenta.iloc[-1]

