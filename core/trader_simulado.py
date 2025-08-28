"""Implementación de un :class:`Trader` en modo simulado.

Este módulo expone :class:`TraderSimulado`, un contenedor ligero que fuerza el
modo no real del trader modular. De esta forma se reutiliza toda la lógica del
bot pero se evita el envío de órdenes reales, permitiendo operar con capital
ficticio para pruebas y demostraciones.
"""

from dataclasses import replace

from .trader_modular import Trader

class TraderSimulado(Trader):
    """Versión simulada del trader modular.

    Cualquier instancia recibirá una configuración con ``modo_real=False`` para
    asegurarse de que las órdenes nunca se envíen al exchange.
    """

    def __init__(self, config):
        super().__init__(replace(config, modo_real=False))

__all__ = ["TraderSimulado"]