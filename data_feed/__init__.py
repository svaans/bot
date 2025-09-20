"""Compatibilidad para el paquete ``data_feed``.

Este paquete reexporta :class:`~data_feed.lite.DataFeed` para mantener la
firma histórica (``from data_feed import DataFeed``) utilizada en distintas
partes del bot. También permite importar los submódulos existentes dentro del
paquete ``data_feed`` sin romper la detección de paquetes implícitos.
"""
from __future__ import annotations

# Re-export DataFeed from the local lite module. The original implementation
# resided in ``core.data_feed``; it has been moved to ``data_feed/lite.py``.
from .lite import DataFeed

__all__ = ["DataFeed"]