"""Compatibilidad de importaciones para ``data_feed``.

Este paquete expone :class:`DataFeed` y :class:`DataFeedLite` apuntando a la
implementación robusta ubicada en :mod:`core.data_feed`. De esta forma se
mantienen operativas las rutas heredadas (``data_feed.lite`` / ``data_feed``)
sin duplicar código ni lógica.
"""
from __future__ import annotations

from .lite import DataFeed, DataFeedLite

__all__ = ["DataFeed", "DataFeedLite"]