"""Utilidades para cálculo de backoff exponencial."""

from __future__ import annotations

import random


def calcular_backoff(
    intentos: int,
    base: float = 1.8,
    max_seg: float = 60.0,
    jitter: bool = True,
) -> float:
    """Calcula el tiempo de espera para ``intentos`` con backoff exponencial.

    El valor crece como ``base ** intentos`` limitado por ``max_seg``. Cuando
    ``jitter`` es verdadero se aplica un factor aleatorio para evitar
    sincronización entre múltiples productores.
    """
    t = min(max_seg, base ** intentos)
    if jitter:
        t = random.uniform(0.5 * t, 1.5 * t)
    return t