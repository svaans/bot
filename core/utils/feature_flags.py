"""Utility helpers for runtime feature flags based on environment variables."""
from __future__ import annotations

import os
from functools import lru_cache

_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}


def _normalize_name(name: str) -> str:
    """Map dotted flag names to environment-friendly keys."""

    return name.strip().upper().replace(".", "_").replace("-", "_")


@lru_cache(maxsize=64)
def is_flag_enabled(name: str, default: bool = False) -> bool:
    """Return the boolean state for ``name`` using environment overrides.

    The lookup is case-insensitive and accepts dotted names (``a.b``) by mapping
    them to environment variables with underscores (``A_B``). Values such as
    ``"1"``, ``"true"`` or ``"on"`` enable the flag while ``"0"`` or
    ``"false"`` disable it. When the environment variable is missing the
    ``default`` value is returned.
    """

    env_key = _normalize_name(name)
    raw = os.getenv(env_key)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    return default


def reset_flag_cache() -> None:
    """Clear cached flag values (used by unit tests)."""

    is_flag_enabled.cache_clear()