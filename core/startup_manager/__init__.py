"""Herramientas para gestionar el arranque del bot."""

from __future__ import annotations

from typing import Any

from .manager import StartupManager
from . import snapshot as _snapshot

SNAPSHOT_PATH = _snapshot.SNAPSHOT_PATH

__all__ = ["StartupManager", "SNAPSHOT_PATH"]


def __getattr__(name: str) -> Any:
    if name == "SNAPSHOT_PATH":
        return _snapshot.SNAPSHOT_PATH
    raise AttributeError(name)


def __setattr__(name: str, value: Any) -> None:
    if name == "SNAPSHOT_PATH":
        _snapshot.SNAPSHOT_PATH = value
    globals()[name] = value
