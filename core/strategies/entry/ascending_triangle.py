import pandas as pd

def ascending_triangle(df: pd.DataFrame) -> dict:
    if len(df) < 20:
        return {"activo": False, "mensaje": "Insuficientes datos"}

    ultimos = df.tail(10)
    altos = ultimos["high"]
    bajos = ultimos["low"]

    resistencia = max(altos)
    minimos = bajos.rolling(window=3).min()

    # Detecta si los mínimos son crecientes y los máximos están contenidos en una resistencia
    if minimos.is_monotonic_increasing:
        return {"activo": True, "mensaje": "Triángulo ascendente detectado"}

    return {"activo": False, "mensaje": "Sin triángulo ascendente"}

