"""Funciones de validación de señales."""
from __future__ import annotations

from typing import Dict

from .loader import cargar_estrategias

_FUNCIONES = cargar_estrategias()

def _obtener_tipo(nombre: str) -> str | None:
    """Retorna ``True`` si existen señales alcistas y bajistas activas a la vez."""
    funcion = _FUNCIONES.get(nombre)
    return getattr(funcion, "tipo", None) if callable(funcion) else None


def hay_contradicciones(estrategias_activas: Dict[str, bool]) -> bool:
    """Retorna ``True`` si existen señales alcistas y bajistas a la vez."""
    tiene_alcista = any(
        _obtener_tipo(nombre) == "alcista" and activo
        for nombre, activo in estrategias_activas.items()
    )
    tiene_bajista = any(
        _obtener_tipo(nombre) == "bajista" and activo
        for nombre, activo in estrategias_activas.items()
    )
    return bool(tiene_alcista and tiene_bajista)