"""Pruebas para la calibración de parámetros adaptativos."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from learning.calibrar_parametros_adaptativos import (
    AdaptiveWeightsCalibration,
    calibrar_pesos,
)


def _crear_dataset(n: int = 120, ruido: float = 0.01) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    pesos_verdaderos = np.array([0.4, 0.3, 0.2, 0.1])
    caracteristicas = rng.normal(loc=0.0, scale=1.0, size=(n, 4))
    objetivo = caracteristicas @ pesos_verdaderos + rng.normal(scale=ruido, size=n)
    df = pd.DataFrame(
        caracteristicas,
        columns=['volatilidad', 'rango', 'volumen', 'momentum'],
    )
    df['objetivo'] = objetivo
    return df


def test_calibrar_pesos_realiza_holdout(tmp_path: Path) -> None:
    df = _crear_dataset(n=160)
    dataset_path = tmp_path / 'dataset.csv'
    df.to_csv(dataset_path, index=False)
    config_path = tmp_path / 'config.json'

    resultado = calibrar_pesos(
        dataset_path,
        config_path,
        validation_fraction=0.2,
        l2_penalty=1e-4,
    )

    assert isinstance(resultado, AdaptiveWeightsCalibration)
    assert resultado.n_train + resultado.n_validation == len(df)
    assert resultado.n_train > resultado.n_validation
    assert resultado.n_validation >= 5
    assert resultado.train_mse >= 0
    assert resultado.validation_mse >= 0
    assert pytest.approx(1.0, abs=1e-3) == sum(resultado.weights.values())

    with open(config_path, 'r', encoding='utf-8') as fh:
        guardado = json.load(fh)

    assert guardado['pesos_contexto'] == resultado.weights
    metricas = guardado['metricas_pesos_contexto']
    assert metricas['n_validation'] == resultado.n_validation
    assert metricas['n_train'] == resultado.n_train
    assert metricas['validation_fraction'] == pytest.approx(0.2)


def test_calibrar_pesos_valida_columnas(tmp_path: Path) -> None:
    df = pd.DataFrame({'volatilidad': [0.1, 0.2], 'objetivo': [0.3, 0.4]})
    dataset_path = tmp_path / 'dataset.csv'
    df.to_csv(dataset_path, index=False)

    with pytest.raises(ValueError):
        calibrar_pesos(dataset_path, tmp_path / 'config.json')