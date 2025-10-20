from __future__ import annotations

from typing import Any

import pandas as pd
from indicadores.macd import calcular_macd


def _extraer_valor_histograma(valor: Any) -> float | None:
    """Normaliza distintos formatos de histograma al último valor numérico."""

    if valor is None:
        return None

    if isinstance(valor, pd.Series):
        if valor.empty:
            return None
        candidato = valor.iloc[-1]
    elif isinstance(valor, (list, tuple)):
        if not valor:
            return None
        candidato = valor[-1]
    elif hasattr(valor, "__len__") and hasattr(valor, "__getitem__"):
        try:
            if len(valor) == 0:  # type: ignore[arg-type]
                return None
            candidato = valor[-1]  # type: ignore[index]
        except Exception:
            candidato = valor
    else:
        candidato = valor

    try:
        return float(candidato)
    except (TypeError, ValueError):
        return None


def estrategia_macd_hist_inversion(df: pd.DataFrame) -> dict[str, Any]:
    """Detecta un cruce de histograma MACD de negativo a positivo."""
    if len(df) < 35:
        return {"activo": False, "mensaje": "Insuficientes datos"}

    _, _, hist_anterior_raw = calcular_macd(df.iloc[:-1])
    _, _, hist_actual_raw = calcular_macd(df)

    hist_anterior = _extraer_valor_histograma(hist_anterior_raw)
    hist_actual = _extraer_valor_histograma(hist_actual_raw)
    if hist_anterior is None or hist_actual is None:
        return {"activo": False, "mensaje": "Histograma MACD no disponible"}

    if hist_anterior < 0.0 and hist_actual > 0.0:
        return {
            "activo": True,
            "mensaje": "Inversión del histograma MACD detectada",
        }

    return {"activo": False, "mensaje": "Sin inversión MACD"}
