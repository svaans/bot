import pandas as pd

def calcular_cruce_medias(df: pd.DataFrame, rapida=10, lenta=20) -> bool:
    if len(df) < lenta + 2 or "close" not in df:
        return False

    sma_rapida = df["close"].rolling(window=rapida).mean()
    sma_lenta = df["close"].rolling(window=lenta).mean()

    cruce_anterior = sma_rapida.iloc[-2] < sma_lenta.iloc[-2]
    cruce_actual = sma_rapida.iloc[-1] > sma_lenta.iloc[-1]

    return cruce_anterior and cruce_actual
