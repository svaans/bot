"""Paquete que encapsula la lógica modular del trader."""

from ._utils import (
    EstadoSimbolo,
    _is_awaitable,
    _maybe_await,
    _normalize_timestamp,
    _reason_none,
    _silence_task_result,
    tf_seconds,
)
from .trader import Trader
from .trader_lite import (
    ComponentResolutionError,
    TraderComponentFactories,
    TraderLite,
)

__all__ = [
    "TraderLite",
    "Trader",
    "TraderComponentFactories",
    "ComponentResolutionError",
    "EstadoSimbolo",
    "tf_seconds",
    "_is_awaitable",
    "_maybe_await",
    "_normalize_timestamp",
    "_reason_none",
    "_silence_task_result",
]
