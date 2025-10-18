"""Herramientas para coordinar la persistencia de estado crítico."""

from __future__ import annotations

from .persistence import (
    CriticalState,
    PersistenceEntry,
    get_persisted_state,
    persist_critical_state,
    register_state,
    restore_critical_state,
    unregister_state,
)

__all__ = [
    "CriticalState",
    "PersistenceEntry",
    "get_persisted_state",
    "persist_critical_state",
    "register_state",
    "restore_critical_state",
    "unregister_state",
]