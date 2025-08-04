"""Funciones de validación de señales."""
from __future__ import annotations
from typing import Dict
from core.estrategias import TENDENCIA_IDEAL


def hay_contradicciones(estrategias_activas: Dict[str, bool]) -> bool:
    """Retorna ``True`` si hay señales alcistas y bajistas simultáneas.

    La evaluación se realiza según la ``TENDENCIA_IDEAL`` de cada estrategia,
    evitando depender de palabras clave en sus nombres.
    """
    tendencias = {TENDENCIA_IDEAL.get(nombre) for nombre, activo in
                  estrategias_activas.items() if activo}
    return 'alcista' in tendencias and 'bajista' in tendencias
