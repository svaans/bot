"""Algoritmos utilitarios de backoff exponencial con jitter."""
from __future__ import annotations

import random

__all__ = ["calcular_backoff"]


def calcular_backoff(intento: int, *, base: float = 1.0, max_seg: float = 30.0) -> float:
    """Devuelve el tiempo de espera recomendado aplicando jitter exponencial."""
    intento = max(1, intento)
    expo = base * (2 ** (intento - 1))
    expo = min(expo, max_seg)
    jitter = random.uniform(0.8, 1.2)
    return round(expo * jitter, 3)