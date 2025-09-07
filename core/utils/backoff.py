"""Utilidades para cálculo de backoff exponencial."""

from __future__ import annotations


def calcular_backoff(actual: float, fallos: int, max_backoff: float = 60) -> float:
    """Calcula el siguiente delay de backoff exponencial.

    Duplica ``actual`` cuando los fallos consecutivos son menores a 3.
    A partir del tercer fallo se incrementa con un factor de 3 para
    espaciar reconexiones agresivas. El resultado se limita a ``max_backoff``.
    """
    factor = 2 if fallos < 3 else 3
    return min(actual * factor, max_backoff)