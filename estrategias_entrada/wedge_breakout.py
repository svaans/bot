import pandas as pd

def wedge_breakout(df: pd.DataFrame) -> dict:
    """
    Detecta un patrón de cuña descendente (falling wedge) con ruptura al alza.
    """
    if len(df) < 30:
        return {"activo": False, "mensaje": "Insuficientes datos"}

    df = df.tail(20)
    altos = df["high"].values
    bajos = df["low"].values

    # Condiciones del wedge descendente:
    # - máximos descendentes
    # - mínimos descendentes
    # - última vela rompe el techo del canal bajista (alto actual > alto anterior)
    maximos_descendentes = altos[0] > altos[5] > altos[10] > altos[15]
    minimos_descendentes = bajos[0] > bajos[5] > bajos[10] > bajos[15]
    breakout = altos[-1] > altos[-2]

    if maximos_descendentes and minimos_descendentes and breakout:
        return {"activo": True, "mensaje": "Ruptura de cuña descendente detectada"}

    return {"activo": False, "mensaje": "Sin ruptura de cuña"}
