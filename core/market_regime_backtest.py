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
    train_accuracy: float = 0.0
    train_total: int = 0
    train_correct: int = 0
    validation_available: bool = True


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


def _calcular_tamano_validacion(
    total: int,
    *,
    window: int,
    step: int,
    validation_fraction: float,
    min_validation_windows: int,
) -> int:
    if not 0 < validation_fraction < 1:
        raise ValueError("validation_fraction debe estar en (0, 1)")
    if min_validation_windows < 1:
        raise ValueError("min_validation_windows debe ser >= 1")
    base_size = max(int(total * validation_fraction), 1)
    ventana_minima = window + step * (min_validation_windows - 1)
    return max(base_size, ventana_minima)


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
    validation_fraction: float = 0.3,
    min_validation_windows: int = 3,
) -> Dict[str, RegimeBacktestResult]:
    """Realiza una búsqueda de umbrales que maximizan la precisión por símbolo."""

    base = base_thresholds or RegimeThresholds()
    alta_vol_candidates = tuple(alta_vol_candidates or (base.alta_volatilidad,))
    slope_upper_candidates = tuple(slope_upper_candidates or (base.slope_upper,))
    slope_lower_candidates = tuple(slope_lower_candidates or (base.slope_lower,))

    resultados: Dict[str, RegimeBacktestResult] = {}

    for symbol, dataset in datasets.items():
        total_registros = len(dataset)
        try:
            val_size = _calcular_tamano_validacion(
                total_registros,
                window=window,
                step=step,
                validation_fraction=validation_fraction,
                min_validation_windows=min_validation_windows,
            )
        except ValueError:
            resultados[symbol] = RegimeBacktestResult(
                symbol=symbol,
                thresholds=base,
                accuracy=0.0,
                total=0,
                correct=0,
                confusion_matrix={},
                train_accuracy=0.0,
                train_total=0,
                train_correct=0,
                validation_available=False,
            )
            continue

        if total_registros <= val_size or total_registros - val_size < window:
            resultados[symbol] = RegimeBacktestResult(
                symbol=symbol,
                thresholds=base,
                accuracy=0.0,
                total=0,
                correct=0,
                confusion_matrix={},
                train_accuracy=0.0,
                train_total=0,
                train_correct=0,
                validation_available=False,
            )
            continue

        train_df = dataset.iloc[: total_registros - val_size]
        val_df = dataset.iloc[total_registros - val_size :]
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
            train_accuracy, train_total, train_correct, _ = _evaluar_regimen(
                symbol,
                train_df,
                thresholds,
                etiqueta_col=etiqueta_col,
                window=window,
                step=step,
            )
            accuracy, total, correct, confusion = _evaluar_regimen(
                symbol,
                val_df,
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
                    train_accuracy=train_accuracy,
                    train_total=train_total,
                    train_correct=train_correct,
                )
            elif accuracy == mejor_resultado.accuracy and train_accuracy > mejor_resultado.train_accuracy:
                mejor_resultado = RegimeBacktestResult(
                    symbol=symbol,
                    thresholds=thresholds,
                    accuracy=accuracy,
                    total=total,
                    correct=correct,
                    confusion_matrix=confusion,
                    train_accuracy=train_accuracy,
                    train_total=train_total,
                    train_correct=train_correct,
                )

        if mejor_resultado is None:
            mejor_resultado = RegimeBacktestResult(
                symbol=symbol,
                thresholds=base,
                accuracy=0.0,
                total=0,
                correct=0,
                confusion_matrix={},
                validation_available=False,
            )
        resultados[symbol] = mejor_resultado

    return resultados