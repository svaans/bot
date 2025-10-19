"""Backtesting utilitario para calibrar ``core.market_regime.detectar_regimen``."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from itertools import product
from typing import Dict, Mapping, Sequence

import pandas as pd

from core.market_regime import RegimeThresholds, detectar_regimen, reset_regimen_cache


@dataclass(frozen=True)
class RegimeBacktestResult:
    """Resumen de la calibración para un símbolo concreto."""

    symbol: str
    thresholds: RegimeThresholds
    accuracy: float
    total: int
    correct: int
    confusion_matrix: Dict[str, Dict[str, int]]


def _evaluar_regimen(
    symbol: str,
    df: pd.DataFrame,
    thresholds: RegimeThresholds,
    etiqueta_col: str,
    window: int,
    step: int,
) -> tuple[float, int, int, Dict[str, Dict[str, int]]]:
    """Evalúa ``detectar_regimen`` recorriendo ventanas crecientes."""

    if window <= 0:
        raise ValueError("window debe ser positivo")
    if step <= 0:
        raise ValueError("step debe ser positivo")
    if etiqueta_col not in df.columns:
        raise ValueError(f"El DataFrame debe contener la columna '{etiqueta_col}'")

    trabajo = df.copy(deep=True)
    trabajo.attrs["symbol"] = symbol
    reset_regimen_cache(symbol)

    correct = 0
    total = 0
    confusion: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for end in range(window, len(trabajo) + 1, step):
        etiqueta = trabajo[etiqueta_col].iloc[end - 1]
        if not isinstance(etiqueta, str) or not etiqueta:
            continue
        ventana = trabajo.iloc[:end]
        ventana.attrs["symbol"] = symbol
        prediccion = detectar_regimen(ventana, thresholds=thresholds)
        confusion[etiqueta][prediccion] += 1
        total += 1
        if prediccion == etiqueta:
            correct += 1

    accuracy = correct / total if total else 0.0
    matriz = {k: dict(v) for k, v in confusion.items()}
    return accuracy, total, correct, matriz


def calibrar_umbrales_por_activo(
    datasets: Mapping[str, pd.DataFrame],
    *,
    etiqueta_col: str = "regimen_real",
    window: int = 120,
    step: int = 5,
    alta_vol_candidates: Sequence[float] | None = None,
    slope_upper_candidates: Sequence[float] | None = None,
    slope_lower_candidates: Sequence[float] | None = None,
    base_thresholds: RegimeThresholds | None = None,
) -> Dict[str, RegimeBacktestResult]:
    """Realiza una búsqueda de umbrales que maximizan la precisión por símbolo."""

    base = base_thresholds or RegimeThresholds()
    alta_vol_candidates = tuple(alta_vol_candidates or (base.alta_volatilidad,))
    slope_upper_candidates = tuple(slope_upper_candidates or (base.slope_upper,))
    slope_lower_candidates = tuple(slope_lower_candidates or (base.slope_lower,))

    resultados: Dict[str, RegimeBacktestResult] = {}

    for symbol, dataset in datasets.items():
        mejor_resultado: RegimeBacktestResult | None = None
        for alta, upper, lower in product(
            alta_vol_candidates, slope_upper_candidates, slope_lower_candidates
        ):
            if lower > upper:
                continue
            thresholds = replace(
                base,
                alta_volatilidad=float(alta),
                slope_upper=float(upper),
                slope_lower=float(lower),
            )
            accuracy, total, correct, confusion = _evaluar_regimen(
                symbol,
                dataset,
                thresholds,
                etiqueta_col=etiqueta_col,
                window=window,
                step=step,
            )
            if total == 0:
                continue
            if mejor_resultado is None or accuracy > mejor_resultado.accuracy:
                mejor_resultado = RegimeBacktestResult(
                    symbol=symbol,
                    thresholds=thresholds,
                    accuracy=accuracy,
                    total=total,
                    correct=correct,
                    confusion_matrix=confusion,
                )

        if mejor_resultado is None:
            mejor_resultado = RegimeBacktestResult(
                symbol=symbol,
                thresholds=base,
                accuracy=0.0,
                total=0,
                correct=0,
                confusion_matrix={},
            )
        resultados[symbol] = mejor_resultado

    return resultados