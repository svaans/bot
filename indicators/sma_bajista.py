import pandas as pd

def calcular_cruce_sma_bajista(df: pd.DataFrame, rapida=10, lenta=20) -> bool:
    if "close" not in df or len(df) < lenta + 2:
        return False

    sma_rapida = df["close"].rolling(window=rapida).mean()
    sma_lenta = df["close"].rolling(window=lenta).mean()

    return sma_rapida.iloc[-2] > sma_lenta.iloc[-2] and sma_rapida.iloc[-1] < sma_lenta.iloc[-1]
