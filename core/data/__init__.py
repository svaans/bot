"""Inicialización perezosa del paquete ``core.data``."""

from __future__ import annotations

from typing import Any

__all__ = [
    "DataFeed",
    "calcular_persistencia_minima",
    "PersistenciaTecnica",
    "coincidencia_parcial",
    "hay_contradicciones",
    "ExternalFeeds",
    "normalizar_funding_rate",
    "normalizar_open_interest",
    "normalizar_noticia",
]


def __getattr__(name: str) -> Any:
    if name == "DataFeed":
        from ..data_feed import DataFeed as _DataFeed

        return _DataFeed
    if name == "calcular_persistencia_minima":
        from .adaptador_persistencia import calcular_persistencia_minima as _fn

        return _fn
    if name == "PersistenciaTecnica":
        from ..persistencia_tecnica import PersistenciaTecnica as _cls

        return _cls
    if name == "coincidencia_parcial":
        from ..persistencia_tecnica import coincidencia_parcial as _fn

        return _fn
    if name == "hay_contradicciones":
        from ..strategies.entry.validaciones_tecnicas import (
            hay_contradicciones as _fn,
        )

        return _fn
    if name == "ExternalFeeds":
        from .external_feeds import ExternalFeeds as _cls

        return _cls
    if name == "normalizar_funding_rate":
        from .external_feeds import normalizar_funding_rate as _fn

        return _fn
    if name == "normalizar_open_interest":
        from .external_feeds import normalizar_open_interest as _fn

        return _fn
    if name == "normalizar_noticia":
        from .external_feeds import normalizar_noticia as _fn

        return _fn
    raise AttributeError(name)
