import pandas as pd
from ta.trend import ADXIndicator




def calcular_adx(df: pd.DataFrame, periodo: int = 14) -> float:
    """Calcula el ADX utilizando la implementación optimizada de ``ta``.

    Devuelve el último valor del indicador o ``None`` si no hay datos
    suficientes o faltan columnas necesarias.
    """

    columnas = {"high", "low", "close"}
    if not columnas.issubset(df.columns) or len(df) < periodo + 1:
        return None
    
    adx_indicator = ADXIndicator(
        high=df["high"],
        low=df["low"],
        close=df["close"],
        window=periodo,
        fillna=False,
    )
    adx = adx_indicator.adx()
    return adx.iloc[-1] if not adx.empty else None
