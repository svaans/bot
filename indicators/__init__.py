"""Alias en inglés para el paquete :mod:`indicadores`.

Provee compatibilidad con código legado que espera importar módulos como
``indicators.ema`` exponiendo directamente los módulos existentes en
``bot/indicadores``.
"""

from __future__ import annotations

from importlib import import_module
import pkgutil
import sys
from types import ModuleType
from typing import Dict, List

_ORIGEN = "indicadores"

_paquete_origen = import_module(_ORIGEN)

# Sincronizamos ``__path__`` para que Python reconozca este paquete como
# equivalente al original y permita importaciones relativas.
__path__ = list(getattr(_paquete_origen, "__path__", []))

_cache: Dict[str, ModuleType] = {}
__all__: List[str] = []


def _registrar(nombre: str) -> ModuleType:
    """Registra un submódulo del paquete ``indicadores`` bajo ``indicators``."""

    if nombre in _cache:
        return _cache[nombre]

    modulo = import_module(f"{_ORIGEN}.{nombre}")
    _cache[nombre] = modulo
    globals()[nombre] = modulo
    sys.modules[f"{__name__}.{nombre}"] = modulo
    if nombre not in __all__:
        __all__.append(nombre)
    return modulo


for _info in pkgutil.iter_modules(__path__):
    _registrar(_info.name)


def __getattr__(nombre: str) -> ModuleType:
    """Carga perezosamente módulos que no se hayan importado todavía."""

    if nombre.startswith("_"):
        raise AttributeError(nombre)
    try:
        return _registrar(nombre)
    except ModuleNotFoundError as exc:  # pragma: no cover - flujo excepcional
        raise AttributeError(nombre) from exc


def __dir__() -> List[str]:
    """Lista atributos disponibles, útil para autocompletado."""

    return sorted(set(__all__))