import pandas as pd


def calcular_momentum(df: pd.DataFrame, periodo: int = 10) -> float:
    """Calcula el momentum como cambio porcentual del cierre.

    Retorna el cambio relativo entre el último precio de cierre y el de
    ``periodo`` velas atrás. Si el DataFrame no contiene la columna ``close`` o
    no hay suficientes datos, devuelve ``None``.
    """
    if 'close' not in df or len(df) < periodo + 1:
        return None
    return df['close'].pct_change(periodo).iloc[-1]
