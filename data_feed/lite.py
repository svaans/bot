"""Compatibilidad ligera para ``data_feed.lite``.

El proyecto original referenciaba un módulo ``data_feed_lite``. Actualmente la
implementación estable vive en :mod:`core.data_feed`. Este adaptador reexpone la
misma clase para evitar romper integraciones existentes y centralizar el código
fuente en un único lugar.
"""
from __future__ import annotations

from core.data_feed import DataFeed as _CoreDataFeed

__all__ = ["DataFeed", "DataFeedLite"]


class DataFeed(_CoreDataFeed):
    """Alias de :class:`core.data_feed.DataFeed`.

    Se define como subclase vacía para permitir especializaciones futuras sin
    alterar la API actual. Mantiene compatibilidad con instancias pickled o
    comprobaciones ``isinstance`` que esperen ``DataFeed`` desde este paquete.
    """


# Compatibilidad con importaciones antiguas
DataFeedLite = DataFeed