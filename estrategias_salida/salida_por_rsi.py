import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD


def salida_por_rsi(df: pd.DataFrame, umbral_bajo=30) -> dict:
    """
    Cierra si RSI cae por debajo del umbral (ej: 30)
    """
    if len(df) < 15:
        return {"cerrar": False, "razon": "Insuficientes datos"}

    rsi = RSIIndicator(close=df["close"], window=14).rsi()
    if rsi.iloc[-1] < umbral_bajo:
        return {"cerrar": True, "razon": f"RSI bajo ({rsi.iloc[-1]:.2f}) < {umbral_bajo}"}

    return {"cerrar": False, "razon": "RSI no bajo"}