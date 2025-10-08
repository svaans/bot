"""Definiciones auxiliares de tipos para el arranque."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from config.config_manager import Config
else:  # Compatibilidad con stubs de tests que omiten Config
    Config = Any  # type: ignore[assignment]

__all__ = ["Config"]
