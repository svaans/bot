from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Literal

from indicators.helpers import get_atr


def calcular_atr_pct(
    df: pd.DataFrame, periodo: int = 14, max_pct: float | None = None
) -> float:
    """Devuelve el ATR como porcentaje del último precio de cierre.

    Se calcula ``ATR / close`` del período indicado. Si ``max_pct`` está
    definido, el resultado se limita a ese máximo.
    """

    if df is None or df.empty or "close" not in df.columns:
        return 0.0
    close = float(df["close"].iloc[-1])
    if close == 0:
        return 0.0
    atr = get_atr(df, periodo)
    if atr is None:
        return 0.0
    atr_pct = float(atr) / close
    if max_pct is not None:
        atr_pct = min(max_pct, atr_pct)
    return max(0.0, atr_pct)


def calcular_slope_pct(serie: pd.Series) -> float:
    """Calcula la pendiente relativa de ``serie`` respecto al último cierre."""

    if serie is None:
        return 0.0
    serie = serie.dropna()
    if len(serie) < 2:
        return 0.0
    y = serie.to_numpy(dtype=float)
    x = np.arange(len(y), dtype=float)
    slope, _ = np.polyfit(x, y, 1)
    precio_ref = y[-1]
    return float(slope / precio_ref) if precio_ref else 0.0


def clasificar_volatilidad(
    atr_pct: float, quieto: float = 0.008, volatil: float = 0.015
) -> Literal["quieto", "moderado", "volatil"]:
    """Clasifica ``atr_pct`` en niveles de volatilidad.

    - ``quieto`` si ``atr_pct`` <= ``quieto``.
    - ``volatil`` si ``atr_pct`` >= ``volatil``.
    - ``moderado`` en el rango intermedio.
    """

    if atr_pct <= quieto:
        return "quieto"
    if atr_pct >= volatil:
        return "volatil"
    return "moderado"
