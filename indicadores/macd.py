import pandas as pd
from indicadores.helpers import filtrar_cerradas


def calcular_macd(df: pd.DataFrame, short=12, long=26, signal=9):
    df = filtrar_cerradas(df)
    if 'close' not in df or len(df) < long + signal:
        return None, None, None
    df = df.copy()
    ema_short = df['close'].ewm(span=short, adjust=False).mean()
    ema_long = df['close'].ewm(span=long, adjust=False).mean()
    macd = ema_short - ema_long
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histograma = macd - signal_line
    return macd.iloc[-1], signal_line.iloc[-1], histograma
