import pandas as pd

def calcular_cruce_ema(df: pd.DataFrame, rapida=12, lenta=26) -> bool:
    if len(df) < lenta + 2 or "close" not in df:
        return False

    ema_fast = df["close"].ewm(span=rapida, adjust=False).mean()
    ema_slow = df["close"].ewm(span=lenta, adjust=False).mean()

    cruce_anterior = ema_fast.iloc[-2] < ema_slow.iloc[-2]
    cruce_actual = ema_fast.iloc[-1] > ema_slow.iloc[-1]

    return cruce_anterior and cruce_actual

