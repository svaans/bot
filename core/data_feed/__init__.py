from __future__ import annotations

"""Paquete público para la capa de ingesta de datos."""

import time as _time

from binance_api.cliente import fetch_ohlcv_async

from core.utils.utils import validar_integridad_velas

from .datafeed import COMBINED_STREAM_KEY, ConsumerState, DataFeed

__all__ = [
    "DataFeed",
    "ConsumerState",
    "COMBINED_STREAM_KEY",
    "fetch_ohlcv_async",
    "validar_integridad_velas",
]

time = _time

__all__.append("time")