"""Paquete de indicadores en castellano.

Este módulo inicializa dinámicamente todos los submódulos que residen en
``bot/indicadores`` y los expone mediante ``__all__``. Además, no realiza
ninguna importación circular con ``indicators``; ese alias se crea en
``bot/indicators`` para mantener compatibilidad con el nombre en inglés.
"""

from __future__ import annotations

from importlib import import_module
import pkgutil
from types import ModuleType
from typing import List

__all__: List[str] = []


def _cargar_modulo(nombre: str) -> ModuleType:
    """Importa el submódulo solicitado y lo expone en el paquete."""

    modulo = import_module(f"{__name__}.{nombre}")
    globals()[nombre] = modulo
    __all__.append(nombre)
    return modulo


for _mod in pkgutil.iter_modules(__path__):
    _cargar_modulo(_mod.name)
