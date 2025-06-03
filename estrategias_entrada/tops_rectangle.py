import pandas as pd

def tops_rectangle(df: pd.DataFrame) -> dict:
    if len(df) < 20:
        return {"activo": False, "mensaje": "Datos insuficientes"}

    top = df["high"].max()
    diferencia = top - df["close"].iloc[-1]

    if abs(diferencia) < 0.01 * top:
        return {"activo": True, "mensaje": "Rectángulo superior de consolidación"}

    return {"activo": False, "mensaje": "Sin patrón de rectángulo en la cima"}