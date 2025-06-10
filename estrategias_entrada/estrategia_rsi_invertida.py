import pandas as pd
from indicadores.rsi import calcular_rsi

def estrategia_rsi_invertida(df: pd.DataFrame) -> dict:
    if len(df) < 15:
        return {"activo": False, "mensaje": "Insuficientes datos"}

    rsi_serie = calcular_rsi(df, serie_completa=True)

    if rsi_serie is None or len(rsi_serie.dropna()) < 2:
        return {"activo": False, "mensaje": "RSI no disponible"}

    if rsi_serie.iloc[-2] > 70 and rsi_serie.iloc[-1] < 70:
        return {"activo": True, "mensaje": "RSI cruzando hacia abajo desde sobrecompra"}

    return {"activo": False, "mensaje": "Sin cruce descendente de RSI"}
