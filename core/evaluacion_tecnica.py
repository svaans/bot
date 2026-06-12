"""Módulo de compatibilidad — delegaba en gestor_entradas sin añadir lógica.

Los consumidores deben importar directamente:
    from core.strategies.entry.gestor_entradas import evaluar_estrategias
"""
from core.strategies.entry.gestor_entradas import evaluar_estrategias

__all__ = ["evaluar_estrategias"]
