import pandas as pd

def calcular_volumen_alto(df: pd.DataFrame, factor: float = 1.5, ventana: int = 20) -> bool:
    if "volume" not in df or len(df) < ventana:
        return False

    volumen_actual = df["volume"].iloc[-1]
    volumen_promedio = df["volume"].iloc[-ventana:-1].mean()

    return volumen_actual > volumen_promedio * factor
