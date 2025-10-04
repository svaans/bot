import pandas as pd
import numpy as np


def calcular_momentum(df: pd.DataFrame, periodo: int = 10) -> float:
    """Calcula el *momentum* normalizado del cierre.

    - Usa únicamente velas cerradas para evitar *look-ahead*.
    - Clipa y normaliza el resultado a ``[-1, 1]``.
    - Retorna ``0.0`` ante datos insuficientes o valores no finitos.
    """
    if not isinstance(df, pd.DataFrame) or 'close' not in df:
        return 0.0

    df_filtrado = df[df['is_closed']] if 'is_closed' in df else df
    if len(df_filtrado) < periodo + 1:
        return 0.0

    momentum = df_filtrado['close'].pct_change(periodo).iloc[-1]
    if not np.isfinite(momentum):
        return 0.0
    return float(np.clip(momentum, -1.0, 1.0))
