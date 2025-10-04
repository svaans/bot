"""SDK offline minimalista para interactuar con Binance.

Este paquete provee implementaciones *stub* utilizadas en entornos de desarrollo
sin acceso a la API real de Binance. Se exponen las mismas firmas empleadas por
el bot para que el resto del código pueda ejecutarse (p. ej. en tests) sin
romperse por import errors. Las funciones retornan datos deterministas que
permiten simular balances, velas históricas y streams en memoria.
"""
from __future__ import annotations

__all__ = [
    "cliente",
    "websocket",
    "filters",
]