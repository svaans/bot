"""Compatibilidad: reexporta validación de niveles desde un único módulo canónico.

La implementación vive en :mod:`core.risk.level_validators` para evitar duplicación
con ``order_manager`` y otros consumidores.
"""

from __future__ import annotations

from core.risk.level_validators import LevelValidationError, validate_levels

__all__ = ["LevelValidationError", "validate_levels"]
