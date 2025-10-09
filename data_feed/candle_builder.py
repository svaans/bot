"""Utilidades asíncronas para completar ventanas de velas.

Este módulo provee una interfaz ligera usada por componentes históricos del
proyecto para lanzar *backfills* en segundo plano cuando el ``DataFrame``
recibido por las estrategias tiene menos velas de las esperadas. La
implementación actual delega la descarga a un *fetcher* configurable y aplica
una normalización consistente con el resto del ``DataFeed``.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Awaitable, Callable, Dict, List, Sequence

from core.utils.utils import configurar_logger, intervalo_a_segundos

try:  # pragma: no cover - dependencia opcional en entornos mínimos
    from core.data_feed import fetch_ohlcv_async as _fetch_ohlcv_async
except Exception:  # pragma: no cover - en pruebas unitarias se inyecta un mock
    _fetch_ohlcv_async = None  # type: ignore

log = configurar_logger("candle_builder", modo_silencioso=True)

FetchCallable = Callable[[Any, str, str, Any | None, int | None], Awaitable[Sequence[Sequence[float]]]]

_DEFAULT_INTERVAL = os.getenv("CANDLE_BUILDER_INTERVAL", os.getenv("INTERVALO_VELAS", "1m"))
_default_fetcher: FetchCallable | None = None
_default_client: Any | None = None
_symbol_locks: Dict[str, asyncio.Lock] = {}

__all__ = ["configure", "backfill"]


def configure(*, fetcher: FetchCallable | None = None, cliente: Any | None = None, intervalo: str | None = None) -> None:
    """Configura valores por defecto para las peticiones de backfill."""

    global _default_fetcher, _default_client, _DEFAULT_INTERVAL
    if fetcher is not None:
        _default_fetcher = fetcher
    if cliente is not None:
        _default_client = cliente
    if intervalo:
        _DEFAULT_INTERVAL = intervalo


def _get_lock(symbol: str) -> asyncio.Lock:
    lock = _symbol_locks.get(symbol)
    if lock is None:
        lock = asyncio.Lock()
        _symbol_locks[symbol] = lock
    return lock


def _resolve_fetcher(fetcher: FetchCallable | None) -> FetchCallable | None:
    if fetcher is not None:
        return fetcher
    if _default_fetcher is not None:
        return _default_fetcher
    return _fetch_ohlcv_async  # type: ignore[return-value]


async def _fetch(symbol: str, intervalo: str, faltantes: int, *, fetcher: FetchCallable, cliente: Any | None) -> List[dict]:
    limit = max(1, min(1000, faltantes))
    try:
        raw = await fetcher(cliente, symbol, intervalo, since=None, limit=limit)
    except Exception as exc:  # pragma: no cover - logging defensivo
        log.warning("[%s] backfill falló al obtener datos: %s", symbol, exc)
        return []

    base_ms = intervalo_a_segundos(intervalo) * 1000
    normalizados: Dict[int, dict] = {}
    for vela in raw or []:
        if not vela:
            continue
        try:
            timestamp = int(vela[0])
            if timestamp % base_ms != 0:
                continue
            normalizados[timestamp] = {
                "symbol": symbol,
                "timestamp": timestamp,
                "open": float(vela[1]),
                "high": float(vela[2]),
                "low": float(vela[3]),
                "close": float(vela[4]),
                "volume": float(vela[5]) if len(vela) > 5 else 0.0,
                "is_closed": True,
            }
        except (TypeError, ValueError, IndexError):  # pragma: no cover - datos corruptos
            log.debug("[%s] vela ignorada por formato inesperado: %r", symbol, vela)
            continue
    ordenados = [normalizados[k] for k in sorted(normalizados)]
    return ordenados[-faltantes:]


async def backfill(
    symbol: str,
    faltantes: int,
    *,
    intervalo: str | None = None,
    cliente: Any | None = None,
    fetcher: FetchCallable | None = None,
) -> List[dict]:
    """Obtiene velas adicionales para ``symbol``.

    La función es *best effort*: si no hay fetcher o cliente configurado la
    llamada se resuelve sin error pero retorna una lista vacía.
    """

    if faltantes <= 0:
        return []

    resolved_intervalo = (intervalo or _DEFAULT_INTERVAL or "1m").lower()
    resolved_cliente = cliente if cliente is not None else _default_client
    resolved_fetcher = _resolve_fetcher(fetcher)

    if resolved_fetcher is None or resolved_cliente is None:
        log.debug(
            "[%s] backfill omitido; fetcher=%s cliente=%s",
            symbol,
            bool(resolved_fetcher),
            bool(resolved_cliente),
        )
        return []

    lock = _get_lock(symbol)
    async with lock:
        candles = await _fetch(symbol, resolved_intervalo, faltantes, fetcher=resolved_fetcher, cliente=resolved_cliente)
        if candles:
            log.info("[%s] backfill completado con %d velas", symbol, len(candles))
        else:
            log.debug("[%s] backfill sin resultados", symbol)
        return candles
