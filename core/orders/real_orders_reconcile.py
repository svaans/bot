# core/orders/real_orders_reconcile.py — reconciliación con exchange (órdenes abiertas / trades)
from __future__ import annotations

import copy
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

import pandas as pd

from binance_api.ccxt_client import obtener_ccxt as obtener_cliente
from binance_api.filters import get_symbol_filters
from config import config as app_config
from config.config_manager import Config
from core.adaptador_dinamico import calcular_tp_sl_adaptativos
from core.orders.order_helpers import extract_ccxt_operation_id
from core.orders.order_model import Order
from core.orders.real_orders_async import resolve_maybe_awaitable as _resolve_maybe_awaitable
from core.orders.real_orders_parse import coerce_open_orders as _coerce_open_orders
from core.utils.log_utils import format_exception_for_log
from core.utils.utils import configurar_logger
from config.exit_defaults import load_exit_config

log = configurar_logger("ordenes")

_RECONCILE_QTY_TOLERANCE = float(
    os.getenv("ORDERS_RECONCILE_QTY_TOLERANCE", "0.01") or 0.01
)

_ULTIMO_OPEN_ORDERS: dict[str, list] = {}
_ULTIMO_OPEN_TS: dict[str, float] = {}
_ULTIMO_OPEN_ORDERS_LOCK = threading.Lock()
_ULTIMO_OPEN_TS_LOCK = threading.Lock()


def reconciliar_ordenes(simbolos: list[str] | None = None) -> dict[str, Order]:
    import core.orders.real_orders as _ro

    # Copia profunda: la reconciliación muta órdenes y hace I/O larga; no debe
    # tocar las instancias en caché ni bloquear el lock durante fetch a Binance.
    #
    # Si ``fetch_open_orders`` falla, se devuelve una copia del estado local **sin**
    # aplicar cierres ni altas desde el exchange (evita borrar posiciones locales
    # por error transitorio de red).
    local = copy.deepcopy(_ro.cargar_ordenes())
    cfg = getattr(app_config, "cfg", None)
    if simbolos is None:
        simbolos = list(getattr(cfg, "symbols", None) or ())
    try:
        cliente = obtener_cliente()
        ordenes_api: list[dict] = []
        if simbolos:
            for s in simbolos:
                fetched = _coerce_open_orders(
                    _resolve_maybe_awaitable(cliente.fetch_open_orders(s))
                )
                if fetched:
                    ordenes_api.extend(fetched)
        else:
            ordenes_api = _coerce_open_orders(
                _resolve_maybe_awaitable(cliente.fetch_open_orders())
            )
    except Exception as e:
        log.error(
            "❌ Error consultando órdenes abiertas: %s",
            format_exception_for_log(e),
        )
        return local
    exchange: dict[str, dict] = {}
    for o in ordenes_api:
        symbol = o.get("symbol")
        if not symbol:
            continue
        filtros = get_symbol_filters(symbol, cliente)
        min_amount = filtros.get("min_qty", 0.0)
        min_cost = filtros.get("min_notional", 0.0)
        price = float(o.get("price") or o.get("average") or 0.0)
        amount = float(o.get("amount") or o.get("remaining") or 0.0)
        if amount < min_amount or price * amount < min_cost:
            continue
        exchange[symbol] = {
            "price": price,
            "amount": amount,
            "side": o.get("side", "buy").lower(),
            "operation_id": extract_ccxt_operation_id(o),
        }
    local_symbols = set(local.keys())
    exchange_symbols = set(exchange.keys())
    local_only = sorted(local_symbols - exchange_symbols)
    exchange_only = sorted(exchange_symbols - local_symbols)
    both = sorted(local_symbols & exchange_symbols)
    log.debug(f"local_only: {local_only}", extra={"symbol": None, "timeframe": None})
    log.debug(f"exchange_only: {exchange_only}", extra={"symbol": None, "timeframe": None})
    log.debug(f"both: {both}", extra={"symbol": None, "timeframe": None})
    for sym in local_only:
        ord_ = local.get(sym)
        if not ord_:
            continue
        ord_.fecha_cierre = datetime.now(timezone.utc).isoformat()
        ord_.motivo_cierre = "closed_by_reconciliation"
        try:
            _ro.registrar_operacion(ord_)
        except Exception as e:
            log.warning(
                "⚠️ No se pudo registrar cierre para %s: %s",
                sym,
                format_exception_for_log(e),
            )
        try:
            _ro.eliminar_orden(sym)
        except Exception as e:
            log.warning(
                "⚠️ No se pudo eliminar orden local %s: %s",
                sym,
                format_exception_for_log(e),
            )
        local.pop(sym, None)
    for sym in exchange_only:
        # ``operation_id`` queda en el objeto :class:`Order` en memoria; el esquema
        # SQLite actual no persiste esa columna (se pierde al recargar desde disco).
        info = exchange[sym]
        side = info["side"]
        direccion = "long" if side == "buy" else "short"
        sl = tp = 0.0
        try:
            ohlcv = cliente.fetch_ohlcv(sym, timeframe="1h", limit=120)
            if ohlcv:
                df = pd.DataFrame(
                    ohlcv, columns=["ts", "open", "high", "low", "close", "volume"]
                )
                cfg_exit = load_exit_config(sym)
                sl_calc, tp_calc = calcular_tp_sl_adaptativos(sym, df, cfg_exit)
                if direccion == "long":
                    sl, tp = sl_calc, tp_calc
                else:
                    sl, tp = tp_calc, sl_calc
        except Exception as e:
            log.warning(
                "⚠️ Error calculando SL/TP para %s: %s",
                sym,
                format_exception_for_log(e),
            )
        try:
            _ro.registrar_orden(
                sym,
                info["price"],
                info["amount"],
                sl,
                tp,
                {},
                "",
                direccion,
                info.get("operation_id"),
            )
        except Exception as e:
            log.warning(
                "⚠️ No se pudo registrar orden reconciliada para %s: %s",
                sym,
                format_exception_for_log(e),
            )
    return _ro.cargar_ordenes()


def sincronizar_ordenes_binance(
    simbolos: list[str] | None = None,
    config: Config | None = None,
    modo_real: bool | None = None,
) -> dict[str, Order]:
    """Consulta órdenes abiertas directamente desde Binance y las registra.

    Esto permite reconstruir el estado de las posiciones cuando el bot se
    reinicia y la base de datos local no contiene todas las operaciones
    abiertas. Devuelve el diccionario de órdenes resultante.
    """
    import core.orders.real_orders as _ro

    config = config or getattr(app_config, "cfg", None)
    if modo_real is None:
        modo_real = getattr(config, "modo_real", True)
    if not modo_real:
        return _ro.cargar_ordenes()
    if simbolos is None and config is not None:
        simbolos = list(getattr(config, "symbols", None) or ())
    try:
        cliente = obtener_cliente(config)
        ordenes_api = []
        if simbolos:
            for s in simbolos:
                fetched = _coerce_open_orders(
                    _resolve_maybe_awaitable(cliente.fetch_open_orders(s))
                )
                if fetched:
                    ordenes_api.extend(fetched)
        else:
            ordenes_api = _coerce_open_orders(
                _resolve_maybe_awaitable(cliente.fetch_open_orders())
            )
    except Exception as e:
        log.error(
            "❌ Error consultando órdenes abiertas: %s",
            format_exception_for_log(e),
        )
        return _ro.cargar_ordenes()
    for o in ordenes_api:
        symbol = o.get("symbol")
        if not symbol:
            continue
        filtros = get_symbol_filters(symbol, cliente)
        min_amount = filtros.get("min_qty", 0.0)
        min_cost = filtros.get("min_notional", 0.0)
        price = float(o.get("price") or o.get("average") or 0)
        amount = float(o.get("amount") or o.get("remaining") or 0)
        if amount < min_amount or price * amount < min_cost:
            continue
        side = o.get("side", "buy").lower()
        direccion = "long" if side == "buy" else "short"
        sl = 0.0
        tp = 0.0
        try:
            ohlcv = cliente.fetch_ohlcv(symbol, timeframe="1h", limit=120)
            if ohlcv:
                df = pd.DataFrame(
                    ohlcv, columns=["ts", "open", "high", "low", "close", "volume"]
                )
                cfg_exit = load_exit_config(symbol)
                sl_calc, tp_calc = calcular_tp_sl_adaptativos(symbol, df, cfg_exit)
                if direccion == "long":
                    sl, tp = sl_calc, tp_calc
                else:
                    sl, tp = tp_calc, sl_calc
        except Exception as e:
            log.warning(
                "⚠️ Error calculando SL/TP para %s: %s",
                symbol,
                format_exception_for_log(e),
            )
        op_id = extract_ccxt_operation_id(o)
        _ro.registrar_orden(symbol, price, amount, sl, tp, {}, "", direccion, op_id)
    return _ro.cargar_ordenes()


def consultar_ordenes_abiertas(symbol: str) -> list[dict]:
    """Consulta órdenes abiertas actuales para `symbol` con reintentos y registro detallado."""
    from ccxt.base.errors import AuthenticationError, NetworkError

    config = getattr(app_config, "cfg", None)
    modo_real = getattr(config, "modo_real", True) if config else True
    if not modo_real:
        log.info(f"🔍 Modo simulado: sin órdenes reales para {symbol}")
        return []

    cliente = obtener_cliente(config)
    now = time.time()
    with _ULTIMO_OPEN_TS_LOCK:
        last_ts = _ULTIMO_OPEN_TS.get(symbol)
        if last_ts and (now - last_ts) < 0.5:
            with _ULTIMO_OPEN_ORDERS_LOCK:
                return list(_ULTIMO_OPEN_ORDERS.get(symbol, []))
        _ULTIMO_OPEN_TS[symbol] = now

    ordenes_api: list[dict] = []
    for intento in range(1, 4):
        try:
            ordenes_api = _coerce_open_orders(
                _resolve_maybe_awaitable(cliente.fetch_open_orders(symbol))
            )
            break
        except AuthenticationError as e:
            log.error(
                "❌ Error de autenticación al consultar órdenes: %s",
                format_exception_for_log(e),
            )
            return []
        except NetworkError as e:
            log.warning(
                "⚠️ Fallo de red consultando órdenes (intento %s/3): %s",
                intento,
                format_exception_for_log(e),
            )
            time.sleep(0.1 * intento)
            continue
        except Exception as e:
            log.error(
                "❌ Error inesperado consultando órdenes: %s",
                format_exception_for_log(e),
            )
            return []

    if not ordenes_api:
        log.info(f"⚠️ No hay órdenes abiertas para {symbol}")
        with _ULTIMO_OPEN_ORDERS_LOCK:
            _ULTIMO_OPEN_ORDERS[symbol] = []
        return []

    filtros = get_symbol_filters(symbol, cliente)
    min_qty = filtros.get("min_qty", 0.0)
    min_cost = filtros.get("min_notional", 0.0)
    step = filtros.get("step_size", None)
    tick_sz = filtros.get("tick_size", None)
    log.info(
        f"🔎 Filtros {symbol}: min_qty={min_qty}, min_notional={min_cost}, stepSize={step}, tickSize={tick_sz}"
    )

    ordenes_validas: list[dict] = []
    for o in ordenes_api:
        price = float(o.get("price") or o.get("average") or 0.0)
        amount = float(o.get("amount") or o.get("remaining") or 0.0)
        if amount < min_qty or price * amount < min_cost:
            log.info(
                f"⚠️ Orden abierta omitida {symbol}: cantidad {amount}, notional {price * amount:.2f} (< mínimos)"
            )
            continue
        ordenes_validas.append(o)

    with _ULTIMO_OPEN_ORDERS_LOCK:
        _ULTIMO_OPEN_ORDERS[symbol] = ordenes_validas
    log.info(
        f"🔍 Órdenes abiertas encontradas para {symbol}: {len(ordenes_validas)}"
    )
    return ordenes_validas


def reconciliar_trades_binance(
    simbolos: list[str] | None = None,
    limit: int = 50,
    apply_changes: bool = False,
    reporter: Callable[[list[dict[str, Any]]], None] | None = None,
) -> list[dict[str, Any]]:
    """Analiza trades recientes y detecta divergencias con el estado local.

    Parameters
    ----------
    simbolos:
        Lista de símbolos a reconciliar. Si se omite se utilizan todos los
        mercados disponibles en el cliente.
    limit:
        Número máximo de trades recientes a consultar por símbolo.
    apply_changes:
        Cuando es ``True`` intenta persistir automáticamente las divergencias.
        Por defecto sólo se reportan para revisión manual.
    reporter:
        Callback opcional que recibe la lista de divergencias detectadas.

    Returns
    -------
    list[dict[str, Any]]
        Detalles de cada divergencia detectada.
    """
    import core.orders.real_orders as _ro

    divergencias: list[dict[str, Any]] = []
    try:
        cliente = _ro.obtener_cliente()
        if simbolos is None:
            simbolos = list(cliente.load_markets().keys())
        for s in simbolos:
            filtros = _ro.get_symbol_filters(s, cliente)
            min_amount = filtros.get("min_qty", 0.0)
            min_cost = filtros.get("min_notional", 0.0)
            try:
                trades = cliente.fetch_my_trades(s, limit=limit)
            except Exception as exc:
                log.warning(
                    "⚠️ No se pudieron obtener trades para %s: %s",
                    s,
                    exc,
                )
                continue

            orden_local = _ro.obtener_orden(s)
            local_qty = 0.0
            if orden_local is not None:
                local_qty = float(
                    getattr(orden_local, "cantidad_abierta", None)
                    or getattr(orden_local, "cantidad", 0.0)
                )

            for t in trades:
                try:
                    price = float(t.get("price") or 0)
                    amount = float(t.get("amount") or t.get("qty") or 0)
                except (TypeError, ValueError):
                    continue
                cost = price * amount
                if amount < min_amount or cost < min_cost:
                    continue
                side = t.get("side", "buy").lower()
                timestamp_value = datetime.fromtimestamp(
                    t.get("timestamp", 0) / 1000,
                    timezone.utc,
                ).isoformat()

                reason: str | None = None
                if side == "buy":
                    if local_qty <= 0:
                        reason = "missing_local_order"
                    else:
                        diff = abs(local_qty - amount)
                        threshold = max(local_qty, amount) * _RECONCILE_QTY_TOLERANCE
                        if diff > threshold:
                            reason = "quantity_mismatch"
                elif side == "sell" and local_qty > 0:
                    reason = "unexpected_sell_trade_with_open_order"

                if not reason:
                    continue

                entry = {
                    "symbol": s,
                    "side": side,
                    "price": price,
                    "amount": amount,
                    "timestamp": timestamp_value,
                    "local_amount": local_qty,
                    "reason": reason,
                }
                divergencias.append(entry)

                if not apply_changes:
                    continue

                try:
                    sl = tp = 0.0
                    if side == "buy":
                        ohlcv = cliente.fetch_ohlcv(s, timeframe="1h", limit=100)
                        if ohlcv:
                            df = pd.DataFrame(
                                ohlcv,
                                columns=[
                                    "timestamp",
                                    "open",
                                    "high",
                                    "low",
                                    "close",
                                    "volume",
                                ],
                            )
                            cfg_exit = load_exit_config(s)
                            sl, tp = calcular_tp_sl_adaptativos(
                                s,
                                df,
                                cfg_exit,
                                precio_actual=price,
                            )
                    data = {
                        "symbol": s,
                        "precio_entrada": price,
                        "cantidad": amount,
                        "timestamp": timestamp_value,
                        "stop_loss": sl,
                        "take_profit": tp,
                        "estrategias_activas": {},
                        "tendencia": "",
                        "max_price": price,
                        "direccion": "long" if side == "buy" else "short",
                    }
                    _ro.guardar_orden_real(s, data)
                    if side == "buy" and amount > 0 and not _ro.obtener_orden(s):
                        try:
                            _ro.registrar_orden(s, price, amount, sl, tp, {}, "", "long", None)
                        except Exception as reg_exc:
                            log.warning(
                                "⚠️ No se pudo registrar orden reconciliada para %s: %s",
                                s,
                                reg_exc,
                            )
                except Exception as persist_exc:
                    log.error(
                        "❌ Error aplicando reconciliación para %s: %s",
                        s,
                        persist_exc,
                    )
    except Exception as e:
        log.error(
            "❌ Error al reconciliar trades: %s",
            format_exception_for_log(e),
        )

    if reporter:
        reporter(divergencias)
    else:
        if divergencias:
            log.warning(
                "orders.reconcile_trades.divergences_detected",
                extra={"count": len(divergencias)},
            )
        else:
            log.info("orders.reconcile_trades.sin_divergencias")
    return divergencias
