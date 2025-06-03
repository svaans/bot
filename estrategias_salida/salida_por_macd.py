import pandas as pd
from ta.trend import MACD

def salida_por_macd(orden, df: pd.DataFrame) -> dict:
    """
    Cierra si MACD cruza a la baja.
    """
    try:
        if len(df) < 35:
            return {"cerrar": False, "razon": "Insuficientes datos"}

        macd = MACD(close=df["close"])
        macd_line = macd.macd()
        signal_line = macd.macd_signal()

        if macd_line.iloc[-2] > signal_line.iloc[-2] and macd_line.iloc[-1] < signal_line.iloc[-1]:
            return {"cerrar": True, "razon": "Cruce bajista de MACD"}

        return {"cerrar": False, "razon": "Sin cruce bajista de MACD"}

    except Exception as e:
        return {"cerrar": False, "razon": f"Error en MACD: {e}"}
