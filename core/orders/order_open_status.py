"""Estado de apertura de orden (módulo ligero para evitar imports circulares)."""

from __future__ import annotations

from enum import Enum


class OrderOpenStatus(Enum):
    """Estado resultante al intentar abrir una orden."""

    OPENED = "opened"
    PENDING_REGISTRATION = "pending_registration"
    FAILED = "failed"
    BLOCKED_BY_ORPHAN = "blocked_by_orphan"  # Símbolo bloqueado: orphan pendiente de reconciliar
