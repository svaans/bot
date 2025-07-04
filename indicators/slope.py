import pandas as pd
import numpy as np


def calcular_slope(df: pd.DataFrame, periodo: int=5) ->float:
    if 'close' not in df or len(df) < periodo:
        return 0.0
    y = df['close'].tail(periodo).to_numpy()
    x = np.arange(len(y))
    if len(y) < 2:
        return 0.0
    slope, _ = np.polyfit(x, y, 1)
    return float(slope)
