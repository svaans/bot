import math
from decimal import Decimal, getcontext
from typing import Iterable

import numpy as np
import pandas as pd


def retornos_simples(precios: pd.Series) -> pd.Series:
    """Calcula los retornos simples de una serie de precios."""
    return precios.pct_change().dropna()


def retornos_log(precios: pd.Series) -> pd.Series:
    """Calcula los retornos logarítmicos de una serie de precios."""
    return np.log(precios / precios.shift(1)).dropna()


def verificar_consistencia(simple: pd.Series, log: pd.Series, tol: float = 1e-9) -> bool:
    """Comprueba que los retornos simples y logarítmicos sean consistentes.

    No deben mezclarse en el mismo pipeline.
    """
    if len(simple) != len(log):
        return False
    return np.allclose(np.log1p(simple.values), log.values, atol=tol, rtol=0)


class Welford:
    """Algoritmo online de Welford para varianza y volatilidad."""

    def __init__(self) -> None:
        self.n = 0
        self.mean = 0.0
        self.M2 = 0.0

    def add(self, x: float) -> None:
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.M2 += delta * delta2

    @property
    def variance(self) -> float:
        return self.M2 / (self.n - 1) if self.n > 1 else 0.0

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)


def varianza_welford(retornos: Iterable[float]) -> float:
    """Calcula la varianza de forma online usando Welford."""
    w = Welford()
    for r in retornos:
        w.add(float(r))
    return w.variance


def volatilidad_welford(retornos: Iterable[float]) -> float:
    """Calcula la volatilidad (desviación estándar) mediante Welford."""
    w = Welford()
    for r in retornos:
        w.add(float(r))
    return w.std


def varianza_alta_precision(retornos: Iterable[float]) -> float:
    """Referencia de varianza con alta precisión usando ``decimal``."""
    datos = [Decimal(str(r)) for r in retornos]
    n = len(datos)
    if n < 2:
        return 0.0
    getcontext().prec = 50
    media = sum(datos) / Decimal(n)
    var = sum((x - media) ** 2 for x in datos) / Decimal(n - 1)
    return float(var)


def annualizar_volatilidad(volatilidad: float, periodos_por_ano: int) -> float:
    """Annualiza la volatilidad según ``periodos_por_ano``."""
    return volatilidad * math.sqrt(periodos_por_ano)