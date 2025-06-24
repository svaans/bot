import pandas as pd

def calcular_divergencia_rsi(df: pd.DataFrame, periodo: int = 14) -> bool:
    if "close" not in df or "low" not in df or len(df) < periodo + 3:
        return False

    delta = df["close"].diff()
    ganancia = delta.where(delta > 0, 0.0)
    perdida = -delta.where(delta < 0, 0.0)
    media_ganancia = ganancia.rolling(window=periodo).mean()
    media_perdida = perdida.rolling(window=periodo).mean()
    rs = media_ganancia / media_perdida
    rsi = 100 - (100 / (1 + rs))

    # Divergencia: precio hace mínimo más bajo, RSI hace mínimo más alto
    precio_bajos = df["low"].iloc[-3:]
    rsi_bajos = rsi.iloc[-3:]

    return precio_bajos.iloc[-1] < precio_bajos.iloc[0] and rsi_bajos.iloc[-1] > rsi_bajos.iloc[0]


def detectar_divergencia_alcista(df: pd.DataFrame, periodo: int = 14) -> bool:
    """Alias de :func:`calcular_divergencia_rsi` para mayor claridad."""
    return calcular_divergencia_rsi(df, periodo)