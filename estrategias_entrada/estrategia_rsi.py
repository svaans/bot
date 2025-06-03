import pandas as pd
from indicadores.rsi import calcular_rsi

def estrategia_rsi(df: pd.DataFrame) -> dict:
    if len(df) < 15:
        return {"activo": False, "mensaje": "Insuficientes datos"}

    rsi = calcular_rsi(df)
    if rsi is None:
        return {"activo": False, "mensaje": "RSI no disponible"}

    if rsi < 30:
        return {"activo": True, "mensaje": f"RSI bajo ({rsi:.2f}) → posible rebote"}

    return {"activo": False, "mensaje": f"RSI actual: {rsi:.2f}"}

