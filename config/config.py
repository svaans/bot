"""Configuración de alto nivel cargada desde ``ConfigManager``.

Acceso lazy: la carga del entorno se difiere al primer acceso de cualquier
atributo. Esto evita que el simple hecho de importar este módulo ejecute
``load_from_env()`` antes de que el entorno esté listo (efecto secundario
en import-time que rompía tests unitarios y dificultaba la modularidad).

Uso compatible con el código existente::

    from config.config import MODO_REAL       # lazy: carga al acceder
    from config.config import cfg             # objeto Config completo
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from config.config_manager import Config

_cfg: "Config | None" = None


def _load() -> "Config":
    global _cfg
    if _cfg is None:
        from config.config_manager import ConfigManager
        _cfg = ConfigManager.load_from_env()
    return _cfg


def __getattr__(name: str) -> Any:
    """Resuelve atributos de módulo de forma lazy sobre la Config cargada."""
    _DIRECT = {"_cfg", "_load", "__getattr__", "__all__", "__spec__", "__loader__"}
    if name in _DIRECT:
        raise AttributeError(name)
    # Alias conveniente: ``from config.config import cfg``
    if name == "cfg":
        return _load()
    config = _load()
    try:
        return getattr(config, name.lower())
    except AttributeError:
        pass
    raise AttributeError(f"module 'config.config' has no attribute {name!r}")
