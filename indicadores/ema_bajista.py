import pandas as pd


def calcular_cruce_ema_bajista(df: pd.DataFrame, rapida=9, lenta=21) ->bool:
    if 'close' not in df or len(df) < lenta + 2:
        return False
    ema_rapida = df['close'].ewm(span=rapida, adjust=False).mean()
    ema_lenta = df['close'].ewm(span=lenta, adjust=False).mean()
    return ema_rapida.iloc[-2] > ema_lenta.iloc[-2] and ema_rapida.iloc[-1
        ] < ema_lenta.iloc[-1]
