"""Configuración centralizada para el módulo de indicadores."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

try:
    from config.config_manager import ConfigManager
except Exception:  # pragma: no cover - fallback en tests
    ConfigManager = None  # type: ignore[assignment]


@dataclass(frozen=True)
class IndicatorSettings:
    """Valores de configuración utilizados por los indicadores."""

    sanitize_normalize_default: bool = True
    cache_max_entries: int = 128
    momentum_activation_threshold: float = 0.001
    momentum_threshold_overrides: dict[str, float] = field(default_factory=dict)


@lru_cache(maxsize=1)
def get_indicator_settings() -> IndicatorSettings:
    """Obtiene los ajustes de indicadores desde la configuración global."""

    if ConfigManager is None:
        return IndicatorSettings()

    cfg: Any = ConfigManager.load_from_env()
    normalize_default = getattr(cfg, 'indicadores_normalize_default', True)
    if isinstance(normalize_default, str):
        normalize_default = normalize_default.strip().lower() not in {
            '0',
            'false',
            'no',
            'off',
        }
    cache_max_entries = getattr(cfg, 'indicadores_cache_max_entries', 128)

    momentum_threshold = getattr(cfg, 'momentum_activation_threshold', 0.001)
    try:
        momentum_threshold = float(momentum_threshold)
    except (TypeError, ValueError):
        momentum_threshold = 0.001
    momentum_threshold = max(momentum_threshold, 0.0)

    overrides_raw = getattr(cfg, 'momentum_threshold_overrides', {})
    overrides: dict[str, float] = {}
    if isinstance(overrides_raw, dict):
        for key, value in overrides_raw.items():
            try:
                overrides[str(key).upper()] = max(float(value), 0.0)
            except (TypeError, ValueError):
                continue

    try:
        cache_max_entries = int(cache_max_entries)
    except (TypeError, ValueError):
        cache_max_entries = 128
    cache_max_entries = max(cache_max_entries, 0)
    return IndicatorSettings(
        sanitize_normalize_default=bool(normalize_default),
        cache_max_entries=int(cache_max_entries),
        momentum_activation_threshold=float(momentum_threshold),
        momentum_threshold_overrides=overrides,
    )


def _reset_indicator_settings_cache_for_tests() -> None:  # pragma: no cover - solo tests
    """Limpia la caché interna usada en pruebas."""

    get_indicator_settings.cache_clear()  # type: ignore[attr-defined]
