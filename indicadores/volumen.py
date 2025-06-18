import pandas as pd

def calcular_volumen_alto(df: pd.DataFrame, factor: float = 1.5, ventana: int = 20) -> bool:
    if "volume" not in df or len(df) < ventana:
        return False

    volumen_actual = df["volume"].iloc[-1]
    volumen_promedio = df["volume"].iloc[-ventana:-1].mean()

    return volumen_actual > volumen_promedio * factor


def verificar_volumen_suficiente(
    df: pd.DataFrame, factor: float = 0.6, ventana: int = 20
) -> bool:
    """Comprueba si el volumen actual es suficiente en relación al promedio.

    Retorna ``True`` cuando el volumen de la última vela es al menos ``factor``
    veces el volumen medio de las ``ventana`` velas previas.
    """
    if "volume" not in df or len(df) < ventana:
        return True

    volumen_actual = df["volume"].iloc[-1]
    volumen_promedio = df["volume"].iloc[-ventana:-1].mean()

    return volumen_actual >= volumen_promedio * factor