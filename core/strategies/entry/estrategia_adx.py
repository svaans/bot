import pandas as pd
from indicators.adx import calcular_adx

def estrategia_adx(df: pd.DataFrame) -> dict:
    if len(df) < 30:
        return {"activo": False, "mensaje": "Datos insuficientes"}

    adx = calcular_adx(df)

    if adx is None:
        return {"activo": False, "mensaje": "ADX no disponible"}

    if adx > 25:
        return {"activo": True, "mensaje": f"ADX alto ({adx:.2f}) → tendencia fuerte"}

    return {"activo": False, "mensaje": f"ADX bajo ({adx:.2f})"}

# Señal orientada a mercado alcista
estrategia_adx.tipo = "alcista"