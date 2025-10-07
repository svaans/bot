from __future__ import annotations

"""Paquete público para la capa de ingesta de datos."""

from typing import Any, Awaitable, Callable, Iterable, Mapping

import asyncio
import inspect

import time as _time

from binance_api import websocket as _ws
from binance_api.cliente import fetch_ohlcv_async

from core.utils.utils import validar_integridad_velas

SimpleHandler = Callable[[dict], Awaitable[None] | None]
SymbolHandler = Callable[[str, dict], Awaitable[None] | None]
HandlerType = SimpleHandler | SymbolHandler
CombinedHandlerType = Mapping[str, HandlerType] | SymbolHandler | SimpleHandler


def _requires_symbol(handler: HandlerType) -> bool:
    """Determina si el ``handler`` espera recibir ``symbol`` explícitamente."""

    try:
        signature = inspect.signature(handler)
    except (TypeError, ValueError):
        return False

    positional_params = [
        param
        for param in signature.parameters.values()
        if param.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]

    required_without_defaults = [
        param for param in positional_params if param.default is inspect._empty
    ]

    return len(required_without_defaults) >= 2


def _wrap_callable(handler: HandlerType, symbol: str | None = None) -> SimpleHandler:
    needs_symbol = _requires_symbol(handler)

    async def _inner(candle: dict) -> None:
        if needs_symbol and symbol is not None:
            result = handler(symbol, candle)  # type: ignore[misc]
        else:
            result = handler(candle)  # type: ignore[misc]
        if asyncio.iscoroutine(result):
            await result

    return _inner


async def escuchar_velas(
    symbol: str,
    intervalo: str,
    handler: HandlerType,
    tiempo_inactividad: float,
    cliente: Any | None,
    *,
    mensaje_timeout: float | None = None,
    backpressure: bool = True,
    ultimos: Mapping[str, Any] | None = None,
) -> None:
    """Wrapper compatible con ``core.data_feed.streaming``.

    Traducimos los parámetros históricos ``ultimos`` a los argumentos
    ``ultimo_timestamp`` y ``ultimo_cierre`` esperados por la implementación
    concreta en :mod:`binance_api.websocket`.
    """

    ultimo_timestamp = None
    ultimo_cierre = None
    if ultimos:
        ultimo_timestamp = ultimos.get("ultimo_timestamp")
        ultimo_cierre = ultimos.get("ultimo_cierre")

    dispatch = _wrap_callable(handler, symbol)

    await _ws.escuchar_velas(
        symbol,
        intervalo,
        dispatch,
        {},
        tiempo_inactividad,
        tiempo_inactividad,
        cliente=cliente,
        mensaje_timeout=mensaje_timeout,
        backpressure=backpressure,
        ultimo_timestamp=ultimo_timestamp,
        ultimo_cierre=ultimo_cierre,
    )


async def escuchar_velas_combinado(
    symbols: Iterable[str],
    intervalo: str,
    handler: CombinedHandlerType,
    opciones: Mapping[str, Any] | None,
    tiempo_inactividad: float,
    heartbeat: float,
    *,
    cliente: Any | None = None,
    mensaje_timeout: float | None = None,
    backpressure: bool = True,
    ultimos: Mapping[str, Mapping[str, Any]] | None = None,
) -> None:
    """Wrapper que preserva la API pública de streams combinados."""

    if isinstance(handler, Mapping):
        adapted_handler = {symbol: _wrap_callable(fn, symbol) for symbol, fn in handler.items()}
    else:
        needs_symbol = _requires_symbol(handler)

        async def _composed(symbol: str, candle: dict) -> None:
            if needs_symbol:
                result = handler(symbol, candle)  # type: ignore[misc]
            else:
                result = handler(candle)  # type: ignore[misc]
            if asyncio.iscoroutine(result):
                await result

        adapted_handler = _composed

    await _ws.escuchar_velas_combinado(
        symbols,
        intervalo,
        adapted_handler,
        dict(opciones or {}),
        tiempo_inactividad,
        heartbeat,
        cliente=cliente,
        mensaje_timeout=mensaje_timeout,
        backpressure=backpressure,
        ultimos=ultimos,
    )

from .datafeed import COMBINED_STREAM_KEY, ConsumerState, DataFeed

__all__ = [
    "DataFeed",
    "ConsumerState",
    "COMBINED_STREAM_KEY",
    "fetch_ohlcv_async",
    "validar_integridad_velas",
    "escuchar_velas",
    "escuchar_velas_combinado",
]

time = _time

__all__.append("time")