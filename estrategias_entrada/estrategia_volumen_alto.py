import pandas as pd
from indicadores.volumen import calcular_volumen_alto

def estrategia_volumen_alto(df: pd.DataFrame) -> dict:
    if len(df) < 20:
        return {"activo": False, "mensaje": "Datos insuficientes"}

    volumen_alto = calcular_volumen_alto(df)

    if volumen_alto:
        return {"activo": True, "mensaje": "Volumen inusualmente alto detectado"}

    return {"activo": False, "mensaje": "Volumen dentro de rango normal"}

