import pandas as pd


def calcular_volatility_breakout(df: pd.DataFrame, ventana: int=14, factor:
    float=1.5) ->bool:
    if 'high' not in df or 'low' not in df or 'close' not in df or len(df
        ) < ventana + 1:
        return False
    df = df.copy()
    rango = df['high'] - df['low']
    rango_medio = rango.rolling(window=ventana).mean()
    breakout = rango.iloc[-1] > rango_medio.iloc[-2] * factor
    return breakout
