import pandas as pd
from indicadores.momentum import calcular_momentum

def estrategia_momentum(df: pd.DataFrame) -> dict:
    if len(df) < 15:
        return {"activo": False, "mensaje": "Insuficientes datos"}

    momentum = calcular_momentum(df)

    if momentum is None:
        return {"activo": False, "mensaje": "Momentum no disponible"}

    if momentum > 0:
        return {"activo": True, "mensaje": f"Momentum positivo: +{momentum:.2f}"}

    return {"activo": False, "mensaje": f"Momentum negativo: {momentum:.2f}"}

