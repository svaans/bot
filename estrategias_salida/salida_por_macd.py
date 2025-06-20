import pandas as pd
from ta.trend import MACD
from core.logger import configurar_logger
from core.salida_utils import resultado_salida

log = configurar_logger("salida_por_macd")

def salida_por_macd(orden, df: pd.DataFrame) -> dict:
    """
    Cierra si MACD cruza a la baja.
    """
    try:
        if len(df) < 35:
            return resultado_salida(
                "Tecnico",
                False,
                "Insuficientes datos",
            )

        macd = MACD(close=df["close"])
        macd_line = macd.macd()
        signal_line = macd.macd_signal()

        if macd_line.iloc[-2] > signal_line.iloc[-2] and macd_line.iloc[-1] < signal_line.iloc[-1]:
            return resultado_salida(
                "Tecnico",
                True,
                "Cruce bajista de MACD",
                logger=log,
            )

        return resultado_salida("Tecnico", False, "Sin cruce bajista de MACD")

    except Exception as e:
        return resultado_salida("Tecnico", False, f"Error en MACD: {e}")
