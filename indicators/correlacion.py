import pandas as pd


def calcular_correlacion(df1: pd.DataFrame, df2: pd.DataFrame, columna: str
    ='close', ventana: int=30) ->(float | None):
    """Calcula la correlación de Pearson entre dos series de precios."""
    if columna not in df1 or columna not in df2:
        return None
    serie1 = df1[columna].tail(ventana)
    serie2 = df2[columna].tail(ventana)
    if len(serie1) < 2 or len(serie2) < 2:
        return None
    return float(serie1.corr(serie2))
