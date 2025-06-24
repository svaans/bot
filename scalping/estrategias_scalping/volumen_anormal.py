"""Detección de volumen anómalo."""

import pandas as pd
from utils_scalping.indicadores import volumen_relativo


THRESHOLD = 3.0

def evaluar(df: pd.DataFrame) -> bool:
    if len(df) < 20:
        return False
    vr = volumen_relativo(df).iloc[-1]
    return vr > THRESHOLD