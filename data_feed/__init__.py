"""Compatibilidad para el módulo ``data_feed``.

Este paquete reexporta :class:`~core.data_feed.DataFeed` para mantener la
firma histórica (``from data_feed import DataFeed``) utilizada en distintas
partes del bot. También permite importar los submódulos existentes dentro del
paquete ``data_feed`` sin romper la detección de paquetes implícitos.
"""
from __future__ import annotations

from core.data_feed import DataFeed

__all__ = ["DataFeed"]