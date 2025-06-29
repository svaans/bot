from __future__ import annotations

import pandas as pd


def factor_volumen_atipico(
    df: pd.DataFrame,
    ventana: int = 30,
    desviaciones: float = 3.0,
) -> float:
    """Calcula un factor de penalización para velas con volumen anómalo.

    Si el volumen de la última vela excede la media de ``ventana`` velas
    previas en más de ``desviaciones`` desviaciones estándar, se considera
    atípico y se devuelve un factor reductor entre ``0.5`` y ``1.0``.
    Cuando no hay datos suficientes se devuelve ``1.0``.
    """
    if (
        df is None
        or "volume" not in df.columns
        or len(df) < ventana + 1
    ):
        return 1.0

    historico = df["volume"].iloc[-ventana - 1 : -1]
    media = historico.mean()
    std = historico.std()

    if std <= 0 or pd.isna(std) or pd.isna(media):
        return 1.0

    limite = media + desviaciones * std
    vol_actual = float(df["volume"].iloc[-1])

    if vol_actual <= limite:
        return 1.0

    exceso = vol_actual / limite
    factor = 1.0 / exceso
    return round(max(0.5, min(1.0, factor)), 3)


__all__ = ["factor_volumen_atipico"]