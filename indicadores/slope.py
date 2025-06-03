# slope.py
import pandas as pd

def calcular_slope(df: pd.DataFrame, periodo: int = 5) -> float:
    if "close" not in df or len(df) < periodo:
        return 0.0

    y = df["close"].tail(periodo)
    x = range(len(y))
    # Usamos regresión lineal simple: pendiente = covarianza(x, y) / varianza(x)
    x_mean = sum(x) / len(x)
    y_mean = y.mean()
    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    den = sum((xi - x_mean)**2 for xi in x)
    return num / den if den != 0 else 0.0
