"""Módulo de configuración del bot."""
from typing import Any, TYPE_CHECKING

from .development import DevelopmentConfig
from .production import ProductionConfig

try:
    from .config_manager import Config, ConfigManager
except (ImportError, AttributeError):  # pragma: no cover - compatibilidad tests
    if TYPE_CHECKING:  # pragma: no branch
        raise

    Config = Any  # type: ignore[assignment]

    class ConfigManager:  # type: ignore[no-redef]
        """Stub mínimo para entornos de test que inyectan config_manager."""

        def __init__(self) -> None:
            self.config = Any  # type: ignore[assignment]
__all__ = ['Config', 'ConfigManager', 'DevelopmentConfig', 'ProductionConfig']
