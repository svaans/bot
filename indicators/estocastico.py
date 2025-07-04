import pandas as pd


def calcular_cruce_estocastico(df: pd.DataFrame, k_period=14, d_period=3):
    if not all(col in df for col in ['high', 'low', 'close']) or len(df
        ) < k_period + d_period:
        return False
    df = df.copy()
    low_min = df['low'].rolling(window=k_period).min()
    high_max = df['high'].rolling(window=k_period).max()
    df['%K'] = 100 * ((df['close'] - low_min) / (high_max - low_min))
    df['%D'] = df['%K'].rolling(window=d_period).mean()
    k_prev, d_prev = df['%K'].iloc[-2], df['%D'].iloc[-2]
    k_curr, d_curr = df['%K'].iloc[-1], df['%D'].iloc[-1]
    return k_prev < d_prev and k_curr > d_curr
