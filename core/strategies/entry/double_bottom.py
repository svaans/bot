import pandas as pd

def double_bottom(df: pd.DataFrame) -> dict:
    if len(df) < 30:
        return {"activo": False, "mensaje": "Insuficientes datos"}

    df = df.tail(30)
    low1 = df["low"].iloc[10]
    low2 = df["low"].iloc[20]

    # Confirmamos que los dos mínimos están muy cerca entre sí (tolerancia del 2%)
    if abs(low1 - low2) / df["low"].mean() < 0.02:
        return {"activo": True, "mensaje": "Patrón Double Bottom detectado"}

    return {"activo": False, "mensaje": "Sin Double Bottom"}

