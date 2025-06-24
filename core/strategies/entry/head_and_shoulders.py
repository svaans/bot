import pandas as pd

def head_and_shoulders(df: pd.DataFrame) -> dict:
    if len(df) < 40:
        return {"activo": False, "mensaje": "Insuficientes datos"}

    hs = df["high"].tail(20).values

    # Lógica simplificada del patrón:
    # - Hombro izquierdo < Cabeza > Hombro derecho
    # - Hombro derecho < Cabeza y similar al hombro izquierdo
    if hs[0] < hs[5] and hs[10] > hs[5] and hs[10] > hs[15] and hs[19] < hs[15]:
        return {"activo": True, "mensaje": "Head and Shoulders detectado"}

    return {"activo": False, "mensaje": "Sin patrón Head and Shoulders"}
