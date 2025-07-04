import pandas as pd
import numpy as np


def calcular_adx(df: pd.DataFrame, periodo: int=14) ->float:
    if not all(col in df for col in ['high', 'low', 'close']) or len(df
        ) < periodo + 1:
        return None
    df = df.copy()
    df['hl'] = df['high'] - df['low']
    df['hc'] = abs(df['high'] - df['close'].shift(1))
    df['lc'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['hl', 'hc', 'lc']].max(axis=1)
    df['+dm'] = df['high'].diff()
    df['-dm'] = df['low'].diff().abs()
    df['+dm'] = df['+dm'].where((df['+dm'] > df['-dm']) & (df['+dm'] > 0), 0.0)
    df['-dm'] = df['-dm'].where((df['-dm'] > df['+dm']) & (df['-dm'] > 0), 0.0)
    tr_sum = df['tr'].rolling(window=periodo).sum()
    plus_di = 100 * (df['+dm'].rolling(window=periodo).sum() / tr_sum)
    minus_di = 100 * (df['-dm'].rolling(window=periodo).sum() / tr_sum)
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
    adx = dx.rolling(window=periodo).mean()
    return adx.iloc[-1] if not adx.empty else None
