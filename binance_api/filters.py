"""Helper para obtener filtros de mercado de Binance con caché.

Provee una función `get_symbol_filters` que devuelve los valores de
``tick_size``, ``step_size``, ``min_qty`` y ``min_notional`` para un
símbolo determinado. Los resultados se almacenan en caché durante un
intervalo configurado para evitar llamadas repetidas al exchange.
"""
from __future__ import annotations

from typing import Dict, Any
import time

from .cliente import crear_cliente

_CACHE_TTL = 60  # segundos
_CACHE: Dict[str, tuple[Dict[str, float], float]] = {}


def _parse_filters(info: Dict[str, Any]) -> Dict[str, float]:
    filters = {f["filterType"]: f for f in info.get("info", {}).get("filters", [])}
    price = filters.get("PRICE_FILTER", {})
    lot = filters.get("LOT_SIZE", {})
    min_notional = filters.get("MIN_NOTIONAL", {})
    return {
        "tick_size": float(price.get("tickSize") or 0) or 0.0,
        "step_size": float(lot.get("stepSize") or 0) or 0.0,
        "min_qty": float(lot.get("minQty") or 0) or 0.0,
        "min_notional": float(min_notional.get("minNotional") or info.get("limits", {}).get("cost", {}).get("min") or 0)
        or 0.0,
    }


def get_symbol_filters(symbol: str, cliente=None) -> Dict[str, float]:
    """Obtiene filtros de un ``symbol`` en Binance con caché.

    Si no se proporciona ``cliente`` se crea uno nuevo. El caché se
    invalida automáticamente tras ``_CACHE_TTL`` segundos.
    """
    ahora = time.time()
    entry = _CACHE.get(symbol)
    if entry and ahora - entry[1] < _CACHE_TTL:
        return entry[0]
    if cliente is None:
        cliente = crear_cliente()
    markets = cliente.load_markets()
    info = markets.get(symbol) or markets.get(symbol.replace("/", ""), {})
    parsed = _parse_filters(info)
    _CACHE[symbol] = (parsed, ahora)
    return parsed


def invalidate_cache(symbol: str | None = None) -> None:
    """Invalida el caché para ``symbol`` o completo si es ``None``."""
    if symbol:
        _CACHE.pop(symbol, None)
    else:
        _CACHE.clear()