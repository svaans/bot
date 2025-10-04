import pandas as pd
import numpy as np


def calcular_slope(df: pd.DataFrame, periodo: int = 5) -> float:
    """Calcula la pendiente normalizada del precio de cierre."""

    if not isinstance(df, pd.DataFrame) or 'close' not in df:
        return 0.0

    df_filtrado = df[df['is_closed']] if 'is_closed' in df else df
    if len(df_filtrado) < periodo:
        return 0.0
    y = df_filtrado['close'].tail(periodo).to_numpy(dtype=float)
    x = np.arange(len(y), dtype=float)
    if len(y) < 2:
        return 0.0
    slope, _ = np.polyfit(x, y, 1)
    price_range = y.max() - y.min()
    if price_range != 0:
        slope /= price_range
    if not np.isfinite(slope):
        return 0.0
    return float(np.clip(slope, -1.0, 1.0))
