import pandas as pd


def calcular_bollinger(df: pd.DataFrame, periodo: int=20, desviacion: float=2.0
    ):
    if 'close' not in df or len(df) < periodo:
        return None, None, None
    ma = df['close'].rolling(window=periodo).mean()
    std = df['close'].rolling(window=periodo).std()
    banda_superior = ma + desviacion * std
    banda_inferior = ma - desviacion * std
    return banda_inferior.iloc[-1], banda_superior.iloc[-1], df['close'].iloc[
        -1]
