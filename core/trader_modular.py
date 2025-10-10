"""Compatibilidad histórica para el trader modular.

Este módulo mantiene las rutas de importación previas delegando en el paquete
:mod:`core.trader`, que ahora aloja la implementación dividida en componentes.
"""
from __future__ import annotations

try:
    from data_feed.lite import DataFeed
except ModuleNotFoundError:  # pragma: no cover
    from data_feed import DataFeed  # type: ignore

try:
    from core.supervisor import Supervisor
except ModuleNotFoundError:  # pragma: no cover
    from supervisor import Supervisor  # type: ignore

try:  # pragma: no cover
    from binance_api.cliente import crear_cliente
except Exception:  # pragma: no cover
    crear_cliente = None  # type: ignore

from core import trader as _trader

TraderLite = _trader.TraderLite
Trader = _trader.Trader
EstadoSimbolo = _trader.EstadoSimbolo
TraderComponentFactories = _trader.TraderComponentFactories
ComponentResolutionError = _trader.ComponentResolutionError

_is_awaitable = _trader._is_awaitable
_maybe_await = _trader._maybe_await
_silence_task_result = _trader._silence_task_result
_normalize_timestamp = _trader._normalize_timestamp
_reason_none = _trader._reason_none
tf_seconds = _trader.tf_seconds

__all__ = list(_trader.__all__)
