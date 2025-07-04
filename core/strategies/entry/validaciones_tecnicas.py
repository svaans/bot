"""Funciones de validación de señales."""
from __future__ import annotations
from typing import Dict


def hay_contradicciones(estrategias_activas: Dict[str, bool]) ->bool:
    """Retorna ``True`` si existen señales alcistas y bajistas a la vez."""
    tiene_alcista = any('alcista' in nombre and activo for nombre, activo in
        estrategias_activas.items())
    tiene_bajista = any('bajista' in nombre and activo for nombre, activo in
        estrategias_activas.items())
    return bool(tiene_alcista and tiene_bajista)
