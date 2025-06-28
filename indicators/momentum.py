import pandas as pd
import numpy as np
from core.utils.cache_indicadores import cached_indicator

@cached_indicator
def calcular_momentum(df: pd.DataFrame, periodo: int = 10) -> float:
    if "close" not in df or len(df) < periodo + 1:
        return None

    close = df["close"].to_numpy(dtype=float)
    return float(close[-1] - close[-periodo])

