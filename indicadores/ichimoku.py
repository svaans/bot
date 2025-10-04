import pandas as pd


def calcular_ichimoku_breakout(df: pd.DataFrame, tenkan_period=9,
    kijun_period=26):
    if 'high' not in df or 'low' not in df or len(df) < kijun_period + 1:
        return None, None
    nine_high = df['high'].rolling(window=tenkan_period).max()
    nine_low = df['low'].rolling(window=tenkan_period).min()
    tenkan_sen = (nine_high + nine_low) / 2
    twenty_six_high = df['high'].rolling(window=kijun_period).max()
    twenty_six_low = df['low'].rolling(window=kijun_period).min()
    kijun_sen = (twenty_six_high + twenty_six_low) / 2
    return tenkan_sen.iloc[-1], kijun_sen.iloc[-1]
