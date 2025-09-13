"""Herramientas de reconexión para el flujo de datos."""

from __future__ import annotations

import random


def calcular_backoff(intentos: int, base: float = 1.0, factor: float = 2.0, max_seg: float = 60.0) -> float:
    """Calcula el tiempo de espera antes de reintentar una conexión.

    Usa un crecimiento exponencial controlado por ``factor`` y añade ``jitter``
    aleatorio proporcional al ``delay`` calculado para evitar sincronización
    entre múltiples clientes.
    """
    delay = min(max_seg, base * (factor ** intentos))
    jitter = random.uniform(0, delay * 0.1)
    return delay + jitter