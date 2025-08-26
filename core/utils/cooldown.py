from __future__ import annotations


def calcular_cooldown(perdidas_consecutivas: int, cooldown_base: int) -> int:
    """Calcula un cooldown ajustado según pérdidas consecutivas.

    El cooldown crece linealmente con el número de pérdidas consecutivas
    hasta un máximo de 3 veces el cooldown base.
    """
    perdidas = max(1, perdidas_consecutivas)
    factor = min(3, perdidas)
    return cooldown_base * factor