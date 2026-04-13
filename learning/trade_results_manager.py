"""Compatibilidad: registro de trades tras ejecución.

La implementación vive en :mod:`learning.gestor_aprendizaje` para evitar duplicar
lógica y rutas de persistencia.
"""

from __future__ import annotations

from .gestor_aprendizaje import registrar_resultado_trade

__all__ = ["registrar_resultado_trade"]
