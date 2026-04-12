"""Estrategias de entrada/salida y motor de evaluación.

Las importaciones pesadas (p. ej. :class:`StrategyEngine`) se resuelven de
forma perezosa para permitir usar el loader de entrada sin cargar todo el
árbol de indicadores en herramientas y tests ligeros.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "StrategyEngine",
    "gestor_pesos",
    "cargar_pesos_estrategias",
    "obtener_peso_salida",
    "ajustar_pesos_por_desempeno",
    "evaluar_estrategias",
    "evaluar_puntaje_tecnico",
    "cargar_pesos_tecnicos",
    "actualizar_pesos_tecnicos",
    "calcular_score_tecnico",
    "obtener_estrategias_por_tendencia",
    "filtrar_por_direccion",
    "ESTRATEGIAS_POR_TENDENCIA",
    "calcular_sinergia",
    "detectar_tendencia",
]


def __getattr__(name: str) -> Any:
    if name == "StrategyEngine":
        from .strategy_engine import StrategyEngine

        return StrategyEngine
    if name in ("gestor_pesos", "cargar_pesos_estrategias", "obtener_peso_salida"):
        from . import pesos as _pesos

        return getattr(_pesos, name)
    if name == "ajustar_pesos_por_desempeno":
        from .ajustador_pesos import ajustar_pesos_por_desempeno

        return ajustar_pesos_por_desempeno
    if name == "evaluar_estrategias":
        from core.evaluacion_tecnica import evaluar_estrategias

        return evaluar_estrategias
    if name in (
        "evaluar_puntaje_tecnico",
        "cargar_pesos_tecnicos",
        "actualizar_pesos_tecnicos",
    ):
        from . import evaluador_tecnico as _ev

        return getattr(_ev, name)
    if name == "calcular_score_tecnico":
        from core.scoring import calcular_score_tecnico

        return calcular_score_tecnico
    if name in (
        "obtener_estrategias_por_tendencia",
        "filtrar_por_direccion",
        "ESTRATEGIAS_POR_TENDENCIA",
        "calcular_sinergia",
    ):
        from core import estrategias as _est

        return getattr(_est, name)
    if name == "detectar_tendencia":
        from .tendencia import detectar_tendencia

        return detectar_tendencia
    if name.startswith("analisis"):
        from .entry import analisis_previo as _ap

        return getattr(_ap, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
