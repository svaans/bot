from __future__ import annotations

"""Validación de órdenes basada en filtros de Binance.

Obtiene ``PRICE_FILTER`` y ``PERCENT_PRICE`` vía ``exchangeInfo`` para
calcular una distancia mínima permitida entre el precio de entrada y el
``stop loss``. También redondea los precios de ``stop loss`` y
``take profit`` al múltiplo de ``tick_size`` correspondiente.
"""
from typing import Tuple

from binance_api.cliente import crear_cliente
from core.orders.order_model import ajustar_tick_size


def _obtener_filtros(symbol: str) -> Tuple[float, float]:
    """Recupera ``tick_size`` y ``percent_price`` para ``symbol``.

    Parameters
    ----------
    symbol: str
        Símbolo de trading (por ejemplo ``"BTC/USDT"``).

    Returns
    -------
    tuple
        ``(tick_size, percent_price)`` en valores absolutos. ``percent_price``
        se expresa como fracción (``0.1`` equivale a 10 %).
    """
    cliente = crear_cliente()
    try:
        info = cliente.public_get_exchangeinfo({"symbol": symbol.replace("/", "")})
    except Exception:
        # Fallback silencioso en caso de error de red u otro
        return 0.0, 0.0
    symbols = info.get("symbols") or []
    if not symbols:
        return 0.0, 0.0
    filters = {f.get("filterType"): f for f in symbols[0].get("filters", [])}
    price = filters.get("PRICE_FILTER", {})
    percent = filters.get("PERCENT_PRICE", {})
    tick_size = float(price.get("tickSize", 0) or 0)
    percent_price = max(
        float(percent.get("multiplierUp", 0) or 0) - 1,
        1 - float(percent.get("multiplierDown", 1) or 1),
    )
    return tick_size, percent_price


def validar_orden(
    symbol: str,
    entry: float,
    sl: float,
    tp: float,
    *,
    min_ticks: int = 2,
) -> Tuple[float, float]:
    """Valida y ajusta ``sl`` y ``tp`` para ``symbol``.

    Parameters
    ----------
    symbol:
        Símbolo de trading.
    entry:
        Precio de entrada propuesto.
    sl:
        ``Stop loss`` propuesto.
    tp:
        ``Take profit`` propuesto.
    min_ticks:
        Mínimo número de *ticks* permitidos entre ``entry`` y ``sl``.

    Returns
    -------
    tuple[float, float]
        ``(sl_ajustado, tp_ajustado)``.

    Raises
    ------
    ValueError
        Si la distancia entre ``entry`` y ``sl`` no cumple ``min_distance``.
    """
    tick_size, percent_price = _obtener_filtros(symbol)
    min_distance = max(tick_size * min_ticks, percent_price * entry)
    if abs(entry - sl) < min_distance:
        raise ValueError(
            f"SL demasiado cercano al precio de entrada. Distancia mínima "
            f"requerida: {min_distance:.8f}"
        )

    # Redondeo a múltiplos de tick_size
    if tick_size > 0:
        direccion_sl = "long" if sl < entry else "short"
        direccion_tp = "short" if tp > entry else "long"
        sl = ajustar_tick_size(sl, tick_size, direccion_sl)
        tp = ajustar_tick_size(tp, tick_size, direccion_tp)
    return sl, tp