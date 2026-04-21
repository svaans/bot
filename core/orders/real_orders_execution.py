# core/orders/real_orders_execution.py — órdenes mercado / límite (CCXT)
from __future__ import annotations

import math
import os
import time
from datetime import datetime, timezone
from typing import Any

from binance_api.ccxt_client import binance_spot_client_order_id, obtener_ccxt as obtener_cliente
from binance_api.filters import get_symbol_filters
from core.diag.execution_quality_log import append_ejecucion_mercado
from core.orders import real_orders_audit
from core.orders import real_orders_metrics
from core.orders.order_model import normalizar_precio_cantidad
from core.utils.logger import log_decision
from core.utils.log_utils import format_exception_for_log
from core.utils.utils import configurar_logger, guardar_orden_real

try:
    from ccxt.base.errors import InsufficientFunds
except ImportError:
    class InsufficientFunds(Exception):
        pass

log = configurar_logger("ordenes")
UTC = timezone.utc

MAX_SLIPPAGE_PCT = float(os.getenv("MAX_SLIPPAGE_PCT", "0.05") or 0.05)
LIMIT_TIMEOUT = float(os.getenv("LIMIT_ORDER_TIMEOUT", "10") or 10)
OFFSET_REPRICE = float(os.getenv("OFFSET_REPRICE", "0.001") or 0.001)
LIMIT_MAX_RETRY = int(os.getenv("LIMIT_ORDER_MAX_RETRY", "3") or 3)


def _ro():
    import core.orders.real_orders as m

    return m


def ejecutar_orden_market(
    symbol: str,
    cantidad: float,
    operation_id: str | None = None,
    *,
    order_attempt: int = 1,
    precio_senal_bot: float | None = None,
) -> dict:
    """Ejecuta una compra de mercado y devuelve detalles de la ejecución.

    ``precio_senal_bot`` es el precio que usó el bot al decidir (p. ej. cierre de vela);
    permite medir slippage respecto al ticker del exchange al enviar la orden.
    """
    ro = _ro()
    entrada = {'symbol': symbol, 'cantidad': cantidad}
    if cantidad <= 0:
        log.warning(f'⚠️ Cantidad inválida para compra en {symbol}: {cantidad}')
        log_decision(log, 'ejecutar_orden_market', operation_id, entrada, {'cantidad_valida': False}, 'reject', {'ejecutado': 0})
        return {'ejecutado': 0.0, 'restante': 0.0, 'status': 'FILLED', 'min_qty': 0.0, 'fee': 0.0, 'pnl': 0.0}
    cliente = None
    precio = 0.0
    try:
        cliente = obtener_cliente()
        filtros = get_symbol_filters(symbol, cliente)
        step_size = filtros.get('step_size', 0.0)
        min_cost = filtros.get('min_notional', 0.0)
        ticker = cliente.fetch_ticker(symbol)
        precio = float(ticker.get('last') or ticker.get('close') or 0)
        precio, cantidad = normalizar_precio_cantidad(filtros, precio, cantidad, 'compra')
        if cantidad <= 0:
            log.error(f'⛔ Cantidad ajustada inválida para {symbol}: {cantidad}')
            log_decision(log, 'ejecutar_orden_market', operation_id, entrada, {'ajuste_valido': False}, 'reject', {'ejecutado': 0})
            return {'ejecutado': 0.0, 'restante': 0.0, 'status': 'FILLED', 'min_qty': 0.0, 'fee': 0.0, 'pnl': 0.0}
        min_amount = filtros.get('min_qty', 0.0)
        quote = symbol.split('/')[1]
        balance = cliente.fetch_balance()
        disponible_quote = balance.get('free', {}).get(quote, 0)
        if precio:
            costo = cantidad * precio
            if costo > disponible_quote:
                cantidad_ajustada = math.floor((disponible_quote / precio) / step_size) * step_size
                cantidad_ajustada = normalizar_precio_cantidad(filtros, precio, cantidad_ajustada, 'compra')[1]
                if cantidad_ajustada <= 0:
                    log.error(
                        f'⛔ Compra cancelada por saldo insuficiente en {symbol}. Requerido: {costo:.2f} {quote}, disponible: {disponible_quote:.2f}'
                    )
                    try:
                        ro.notificador.enviar(
                            f'Compra cancelada por saldo insuficiente en {symbol}',
                            'CRITICAL',
                        )
                    except Exception:
                        pass
                    log_decision(log, 'ejecutar_orden_market', operation_id, entrada, {'saldo_suficiente': False}, 'reject', {'ejecutado': 0})
                    return {'ejecutado': 0.0, 'restante': 0.0, 'status': 'FILLED', 'min_qty': min_amount, 'fee': 0.0, 'pnl': 0.0}
                log.warning(
                    f'⚠️ Cantidad ajustada por saldo insuficiente en {symbol}. Requerido: {costo:.2f} {quote}, disponible: {disponible_quote:.2f}, nueva cantidad: {cantidad_ajustada}'
                )
                cantidad = cantidad_ajustada
        if cantidad < min_amount or precio and cantidad * precio < min_cost:
            log.error(
                f'⛔ Compra inválida para {symbol}. Cantidad: {cantidad}, Precio: {precio}, Mínimos → amount: {min_amount}, notional: {min_cost}'
                )
            try:
                ro.notificador.enviar(
                    f'Compra inválida para {symbol}', 'WARNING'
                )
            except Exception:
                pass
            log_decision(log, 'ejecutar_orden_market', operation_id, entrada, {'minimos': False}, 'reject', {'ejecutado': 0})
            return {'ejecutado': 0.0, 'restante': 0.0, 'status': 'FILLED', 'min_qty': min_amount, 'fee': 0.0, 'pnl': 0.0}
        log.debug(
            f'📤 Enviando orden de compra para {symbol} | Cantidad: {cantidad} | Precio estimado: {precio:.4f}',
            extra={'symbol': symbol, 'timeframe': None},
        )
        params = {}
        cid = binance_spot_client_order_id(operation_id, attempt=order_attempt)
        if cid:
            params['newClientOrderId'] = cid
        response = cliente.create_market_buy_order(symbol, cantidad, params)
        ejecutado = float(response.get('amount') or response.get('filled') or 0)
        if ejecutado <= 0:
            log.warning(
                '⚠️ Compra %s: exchange devolvió filled=0; no se asume ejecución completa.',
                symbol,
                extra={'symbol': symbol, 'timeframe': None},
            )
            restante = cantidad
            status = 'PARTIAL'
        else:
            restante = max(cantidad - ejecutado, 0.0)
            status = 'FILLED'
            if restante > 1e-8:
                status = 'PARTIAL'
        metricas = {'fee': 0.0, 'pnl': 0.0}
        if operation_id:
            metricas = real_orders_metrics.acumular_metricas(operation_id, response)
        precio_fill = float(response.get('price') or response.get('average') or precio)
        slippage = abs(precio_fill - precio) / precio if precio else 0.0
        slippage_vs_senal: float | None = None
        if precio_senal_bot is not None and precio_senal_bot > 0:
            slippage_vs_senal = abs(precio_fill - precio_senal_bot) / precio_senal_bot
        if slippage > MAX_SLIPPAGE_PCT:
            log.warning(
                f'⚠️ Slippage alto en {symbol}: {slippage:.2%} (máx {MAX_SLIPPAGE_PCT:.2%})'
            )
            try:
                ro.notificador.enviar(
                    f'Slippage alto en {symbol}: {slippage:.2%}', 'WARNING'
                )
            except Exception:
                pass
        if ejecutado > 0:
            log.info(
                f'🟢 Order real ejecutada: {symbol}, cantidad: {ejecutado}',
                extra={'symbol': symbol, 'timeframe': None},
            )
        else:
            log.warning(
                '⚠️ Compra %s sin fills reportados en la respuesta.',
                symbol,
                extra={'symbol': symbol, 'timeframe': None},
            )
        salida = {
            'ejecutado': ejecutado,
            'restante': restante,
            'status': status,
            'min_qty': min_amount,
            'fee': metricas['fee'],
            'pnl': metricas['pnl'],
            'slippage': slippage,
            'precio_referencia': precio,
            'precio_fill': precio_fill,
        }
        if slippage_vs_senal is not None:
            salida['slippage_vs_senal'] = slippage_vs_senal
        if ejecutado > 0:
            append_ejecucion_mercado(
                {
                    'ts': time.time(),
                    'event': 'market_buy',
                    'symbol': symbol,
                    'side': 'buy',
                    'operation_id': operation_id,
                    'order_attempt': order_attempt,
                    'precio_referencia': precio,
                    'precio_fill': precio_fill,
                    'slippage_vs_ticker': slippage,
                    'precio_senal': precio_senal_bot,
                    'slippage_vs_senal': slippage_vs_senal,
                    'ejecutado': ejecutado,
                    'fee': metricas['fee'],
                }
            )
        log_decision(log, 'ejecutar_orden_market', operation_id, entrada, {'minimos': True}, 'accept', salida)
        return salida
    except Exception as e:
        err_msg = format_exception_for_log(e)
        log.error(
            '❌ Error en Binance al ejecutar compra en %s: %s',
            symbol,
            err_msg,
        )
        contexto_decision: dict[str, Any] = {'reason': err_msg}
        ejecucion_confirmada = False

        if cliente is not None:
            try:
                trades = cliente.fetch_my_trades(symbol, limit=1)
                trade = trades[-1] if trades else None
            except Exception as ver_err:
                ver_msg = format_exception_for_log(ver_err)
                log.error(
                    '❌ Error verificando trades tras fallo: %s',
                    ver_msg,
                )
                contexto_decision['trade_history_error'] = ver_msg
                trade = None
            else:
                ejecutado = (
                    float(trade.get('amount') or trade.get('qty') or 0) if trade else 0.0
                )
                if ejecutado > 0:
                    ejecucion_confirmada = True
                    contexto_decision.update(
                        {
                            'fallback': 'trade_history',
                            'ejecutado': ejecutado,
                        }
                    )
                    precio_trade = (
                        float(trade.get('price') or 0) if trade else precio
                    )
                    log.warning(
                        f'⚠️ Operación detectada tras error vía trades: {ejecutado} {symbol}'
                    )
                    try:
                        ro.notificador.enviar(
                            f'Orden ejecutada tras error en {symbol}', 'WARNING'
                        )
                    except Exception:
                        pass
                    try:
                        guardar_orden_real(
                            symbol,
                            {
                                'symbol': symbol,
                                'precio_entrada': precio_trade,
                                'cantidad': ejecutado,
                                'timestamp': datetime.now(UTC).isoformat(),
                                'stop_loss': 0.0,
                                'take_profit': 0.0,
                                'estrategias_activas': {},
                                'tendencia': '',
                                'max_price': precio_trade,
                                'direccion': 'long',
                            },
                        )
                        ro.registrar_orden(
                            symbol,
                            precio_trade,
                            ejecutado,
                            0.0,
                            0.0,
                            {},
                            '',
                            'long',
                            operation_id,
                        )
                    except Exception as e_reg:
                        reg_msg = format_exception_for_log(e_reg)
                        log.error(
                            '❌ No se pudo registrar orden tras error: %s',
                            reg_msg,
                        )
                        contexto_decision['registro_error'] = reg_msg

        auditoria_info: dict[str, Any] | None = None
        if not ejecucion_confirmada and cliente is not None:
            try:
                auditoria_info = real_orders_audit.auditar_operacion_post_error(
                    cliente,
                    symbol,
                    operation_id,
                    cantidad,
                )
            except Exception as audit_err:
                aud_msg = format_exception_for_log(audit_err)
                log.error(
                    '❌ Auditoría cruzada falló para %s: %s',
                    symbol,
                    aud_msg,
                )
                contexto_decision['audit_error'] = aud_msg
            else:
                if auditoria_info:
                    contexto_decision['auditoria'] = auditoria_info.get('source')
                    contexto_decision['ejecutado'] = auditoria_info.get('ejecutado', 0.0)
                    contexto_decision['restante'] = auditoria_info.get('restante')
                    if auditoria_info.get('ejecutado', 0.0) > 0:
                        ejecucion_confirmada = True
                        precio_confirmado = auditoria_info.get('precio') or precio
                        log.warning(
                            '⚠️ Auditoría confirmó ejecución en %s (%s): %.6f',
                            symbol,
                            auditoria_info.get('source'),
                            auditoria_info.get('ejecutado', 0.0),
                        )
                        try:
                            ro.notificador.enviar(
                                f'Orden confirmada por auditoría en {symbol}',
                                'WARNING',
                            )
                        except Exception:
                            pass
                        try:
                            guardar_orden_real(
                                symbol,
                                {
                                    'symbol': symbol,
                                    'precio_entrada': precio_confirmado,
                                    'cantidad': float(
                                        auditoria_info.get('ejecutado', 0.0)
                                    ),
                                    'timestamp': datetime.now(UTC).isoformat(),
                                    'stop_loss': 0.0,
                                    'take_profit': 0.0,
                                    'estrategias_activas': {},
                                    'tendencia': '',
                                    'max_price': precio_confirmado,
                                    'direccion': 'long',
                                },
                            )
                            ro.registrar_orden(
                                symbol,
                                precio_confirmado,
                                float(auditoria_info.get('ejecutado', 0.0)),
                                0.0,
                                0.0,
                                {},
                                '',
                                'long',
                                operation_id,
                            )
                        except Exception as audit_reg_err:
                            log.error(
                                f'❌ No se pudo registrar orden confirmada por auditoría: {audit_reg_err}'
                            )
                            contexto_decision['registro_error'] = str(audit_reg_err)
                    elif auditoria_info.get('source') == 'open_orders_pending':
                        log.warning(
                            '⚠️ Auditoría detectó orden pendiente para %s tras el fallo',
                            symbol,
                        )

        if cliente is None:
            contexto_decision['cliente'] = 'no_disponible'

        log_decision(
            log,
            'ejecutar_orden_market',
            operation_id,
            entrada,
            {},
            'error',
            contexto_decision,
        )
        raise


def ejecutar_orden_market_sell(
    symbol: str,
    cantidad: float,
    operation_id: str | None = None,
    *,
    order_attempt: int = 1,
    precio_senal_bot: float | None = None,
) -> dict:
    """Ejecuta una venta de mercado validando saldo y devuelve detalles."""
    ro = _ro()
    entrada = {'symbol': symbol, 'cantidad': cantidad}

    if ro.venta_fallida(symbol):
        log.warning(f'⏭️ Venta omitida para {symbol} por intento previo fallido de saldo.')
        log_decision(
            log,
            'ejecutar_orden_market_sell',
            operation_id,
            entrada,
            {'venta_omitida': True},
            'reject',
            {'ejecutado': 0},
        )
        return {
            'ejecutado': 0.0,
            'restante': cantidad,
            'status': 'FILLED',
            'min_qty': 0.0,
            'fee': 0.0,
            'pnl': 0.0,
        }

    try:
        cliente = obtener_cliente()
        balance = cliente.fetch_balance()
        base = symbol.split('/')[0]
        disponible = balance.get('free', {}).get(base, 0)

        if disponible <= 0:
            log_decision(log, 'ejecutar_orden_market_sell', operation_id, entrada, {'saldo': 0}, 'reject', {'ejecutado': 0})
            raise InsufficientFunds(f'Saldo 0 disponible para {symbol}')

        filtros = get_symbol_filters(symbol, cliente)
        step_size = filtros.get('step_size', 0.0)
        cantidad_vender = math.floor(disponible / step_size) * step_size
        cantidad_vender = min(cantidad, cantidad_vender)

        if cantidad_vender <= 0:
            log_decision(log, 'ejecutar_orden_market_sell', operation_id, entrada, {'cantidad_valida': False}, 'reject', {'ejecutado': 0})
            raise ValueError(f'Cantidad inválida ({cantidad_vender}) para vender en {symbol}')

        min_amount = filtros.get('min_qty', 0.0)
        min_cost = filtros.get('min_notional', 0.0)
        ticker = cliente.fetch_ticker(symbol)
        precio = float(ticker.get('last') or ticker.get('close') or 0)
        precio, cantidad_vender = normalizar_precio_cantidad(filtros, precio, cantidad_vender, 'venta')

        if cantidad_vender < min_amount or (precio and cantidad_vender * precio < min_cost):
            log.error(
                f'⛔ Venta rechazada por mínimos: {symbol} → cantidad: {cantidad_vender:.8f}, '
                f'mínimos: amount={min_amount}, notional={min_cost}'
            )
            ro.registrar_venta_fallida(symbol)
            try:
                ro.notificador.enviar(f'Venta rechazada por mínimos en {symbol}', 'WARNING')
            except Exception:
                pass
            log_decision(log, 'ejecutar_orden_market_sell', operation_id, entrada, {'minimos': False}, 'reject', {'ejecutado': 0})
            return {
                'ejecutado': 0.0,
                'restante': cantidad_vender,
                'status': 'FILLED',
                'min_qty': min_amount,
                'fee': 0.0,
                'pnl': 0.0,
            }

        log.info(
            f'💱 Ejecutando venta real en {symbol}: {cantidad_vender:.8f} unidades (precio estimado: {precio:.2f})',
            extra={'symbol': symbol, 'timeframe': None},
        )

        params = {}
        cid = binance_spot_client_order_id(operation_id, attempt=order_attempt)
        if cid:
            params['newClientOrderId'] = cid

        response = cliente.create_market_sell_order(symbol, cantidad_vender, params)
        ejecutado = float(response.get('amount') or response.get('filled') or 0)
        if ejecutado <= 0:
            log.warning(
                '⚠️ Venta %s: exchange devolvió filled=0; no se asume ejecución completa.',
                symbol,
                extra={'symbol': symbol, 'timeframe': None},
            )
            ejecutado = 0.0

        restante = max(cantidad_vender - ejecutado, 0.0)
        status = 'FILLED' if restante <= 1e-8 else 'PARTIAL'

        metricas = {'fee': 0.0, 'pnl': 0.0}
        if operation_id:
            metricas = real_orders_metrics.acumular_metricas(operation_id, response)

        precio_fill = float(response.get('price') or response.get('average') or precio)
        slippage = abs(precio_fill - precio) / precio if precio else 0.0
        slippage_vs_senal: float | None = None
        if precio_senal_bot is not None and precio_senal_bot > 0:
            slippage_vs_senal = abs(precio_fill - precio_senal_bot) / precio_senal_bot
        if slippage > MAX_SLIPPAGE_PCT:
            log.warning(f'⚠️ Slippage alto en {symbol}: {slippage:.2%} (máx {MAX_SLIPPAGE_PCT:.2%})')
            try:
                ro.notificador.enviar(f'Slippage alto en {symbol}: {slippage:.2%}', 'WARNING')
            except Exception:
                pass

        log.info(
            f'🔴 Orden de venta ejecutada: {symbol}, cantidad: {ejecutado:.8f}',
            extra={'symbol': symbol, 'timeframe': None},
        )

        ro.limpiar_venta_fallida(symbol)

        salida = {
            'ejecutado': ejecutado,
            'restante': restante,
            'status': status,
            'min_qty': min_amount,
            'fee': metricas['fee'],
            'pnl': metricas['pnl'],
            'slippage': slippage,
            'precio_referencia': precio,
            'precio_fill': precio_fill,
        }
        if slippage_vs_senal is not None:
            salida['slippage_vs_senal'] = slippage_vs_senal
        if ejecutado > 0:
            append_ejecucion_mercado(
                {
                    'ts': time.time(),
                    'event': 'market_sell',
                    'symbol': symbol,
                    'side': 'sell',
                    'operation_id': operation_id,
                    'order_attempt': order_attempt,
                    'precio_referencia': precio,
                    'precio_fill': precio_fill,
                    'slippage_vs_ticker': slippage,
                    'precio_senal': precio_senal_bot,
                    'slippage_vs_senal': slippage_vs_senal,
                    'ejecutado': ejecutado,
                    'fee': metricas['fee'],
                }
            )
        log_decision(log, 'ejecutar_orden_market_sell', operation_id, entrada, {'minimos': True}, 'accept', salida)
        return salida

    except InsufficientFunds as e:
        log.error(
            '❌ Venta rechazada por saldo insuficiente en %s: %s',
            symbol,
            format_exception_for_log(e),
        )
        ro.registrar_venta_fallida(symbol)
        try:
            ro.notificador.enviar(f'Venta rechazada por saldo insuficiente en {symbol}', 'CRITICAL')
        except Exception:
            pass
        log_decision(
            log,
            'ejecutar_orden_market_sell',
            operation_id,
            entrada,
            {'saldo_insuficiente': True},
            'reject',
            {'ejecutado': 0},
        )
        return {
            'ejecutado': 0.0,
            'restante': cantidad,
            'status': 'FILLED',
            'min_qty': 0.0,
            'fee': 0.0,
            'pnl': 0.0,
        }

    except Exception as e:
        sell_err = format_exception_for_log(e)
        log.error(
            '❌ Error en intercambio al vender %s: %s',
            symbol,
            sell_err,
        )
        log_decision(
            log,
            'ejecutar_orden_market_sell',
            operation_id,
            entrada,
            {},
            'error',
            {'reason': sell_err},
        )
        return {
            'ejecutado': 0.0,
            'restante': cantidad,
            'status': 'ERROR',
            'min_qty': 0.0,
            'fee': 0.0,
            'pnl': 0.0,
        }



def _market_sell_retry(
    symbol: str,
    cantidad: float,
    operation_id: str | None = None,
    order_attempt_start: int = 1,
    *,
    precio_senal_bot: float | None = None,
) -> dict:
    """Envía ventas de mercado reintentando en caso de fills parciales.

    ``order_attempt_start`` alinea ``newClientOrderId`` con reintentos externos
    (p. ej. :class:`MarketRetryExecutor`): cada llamada a Binance debe usar un
    id de cliente distinto aunque este bucle se reinicie tras un error.
    """
    ro = _ro()
    restante = cantidad
    total = total_fee = total_pnl = 0.0
    vwap_num = 0.0
    vwap_den = 0.0
    min_qty = 0.0
    attempt = 0
    last_order_attempt = max(0, int(order_attempt_start) - 1)
    while restante > 0:
        attempt += 1
        oid_att = int(order_attempt_start) + attempt - 1
        last_order_attempt = oid_att
        resp = ro.ejecutar_orden_market_sell(
            symbol,
            restante,
            operation_id,
            order_attempt=oid_att,
            precio_senal_bot=precio_senal_bot,
        )
        if str(resp.get("status") or "").upper() == "ERROR":
            fill_avg = (vwap_num / vwap_den) if vwap_den > 0 else None
            return {
                "ejecutado": total,
                "restante": restante,
                "status": "ERROR",
                "min_qty": float(resp.get("min_qty", 0.0)),
                "fee": total_fee,
                "pnl": total_pnl,
                "last_order_attempt": last_order_attempt,
                "precio_fill_promedio": fill_avg,
            }
        ejecutado = float(resp.get('ejecutado', 0.0))
        restante = float(resp.get('restante', 0.0))
        min_qty = float(resp.get('min_qty', 0.0))
        total += ejecutado
        total_fee += float(resp.get('fee', 0.0))
        total_pnl += float(resp.get('pnl', 0.0))
        if ejecutado > 0:
            pf = resp.get("precio_fill")
            if pf is not None:
                try:
                    pff = float(pf)
                    if pff > 0:
                        vwap_num += pff * ejecutado
                        vwap_den += ejecutado
                except (TypeError, ValueError):
                    pass
        if ejecutado <= 0:
            break
        if resp.get('status') != 'PARTIAL' or restante < min_qty:
            break
    estado = 'FILLED' if restante < 1e-8 else 'PARTIAL'
    fill_avg = (vwap_num / vwap_den) if vwap_den > 0 else None
    return {
        'ejecutado': total,
        'restante': restante,
        'status': estado,
        'min_qty': min_qty,
        'fee': total_fee,
        'pnl': total_pnl,
        'last_order_attempt': last_order_attempt,
        'precio_fill_promedio': fill_avg,
    }


def ejecutar_orden_limit(
    symbol: str,
    side: str,
    precio: float,
    cantidad: float,
    operation_id: str | None = None,
    timeout: float | None = None,
    offset_reprice: float | None = None,
    max_reintentos: int | None = None,
) -> dict:
    """Ejecuta una orden LIMIT con re-precio y fallback a MARKET.

    Si la orden no se completa dentro de `timeout` segundos se cancela y se
    re-precia usando `offset_reprice`. Tras `max_reintentos` intentos fallidos,
    envía una orden de mercado.
    """
    ro = _ro()
    if side not in {"buy", "sell"}:
        raise ValueError('side debe ser "buy" o "sell"')

    timeout = timeout or LIMIT_TIMEOUT
    offset_reprice = offset_reprice or OFFSET_REPRICE
    max_reintentos = max_reintentos or LIMIT_MAX_RETRY

    cliente = obtener_cliente()
    cliente.load_markets()  # asegura markets en ccxt

    params_base: dict[str, Any] = {}

    filtros = get_symbol_filters(symbol, cliente)
    min_qty = filtros.get("min_qty", 0.0)

    restante = cantidad
    ejecutado_total = 0.0
    precio_actual = precio
    metricas = {"fee": 0.0, "pnl": 0.0}
    slippage = 0.0
    vwap_num = 0.0
    vwap_den = 0.0

    for intento in range(1, max_reintentos + 1):
        # Normaliza precio/cantidad para cumplir filtros del exchange
        precio_actual, cantidad_norm = normalizar_precio_cantidad(
            filtros, precio_actual, restante, "compra" if side == "buy" else "venta"
        )

        params = params_base.copy()
        cid = binance_spot_client_order_id(operation_id, attempt=intento)
        if cid:
            params["newClientOrderId"] = cid

        if side == "buy":
            orden = cliente.create_limit_buy_order(symbol, cantidad_norm, precio_actual, params)
        else:
            orden = cliente.create_limit_sell_order(symbol, cantidad_norm, precio_actual, params)

        order_id = orden.get("id")
        inicio = time.time()
        estado: dict[str, Any] = orden

        # Espera activa hasta timeout comprobando fills
        while time.time() - inicio < timeout:
            estado = cliente.fetch_order(order_id, symbol)
            filled_tmp = float(estado.get("filled") or 0.0)
            if estado.get("status") in {"closed", "canceled"} or filled_tmp >= cantidad_norm:
                break
            time.sleep(1)
        else:
            # timeout -> cancelar y obtener estado final
            try:
                estado = cliente.cancel_order(order_id, symbol)
            except Exception as e:
                log.warning(
                    "⚠️ No se pudo cancelar orden %s: %s",
                    order_id,
                    format_exception_for_log(e),
                )
                estado = {}
            if float(estado.get("filled") or estado.get("executedQty") or 0.0) == 0.0:
                try:
                    estado = cliente.fetch_order(order_id, symbol)
                except Exception:
                    estado = {}

        ejecutado = float(estado.get("filled") or estado.get("executedQty") or 0.0)
        ejecutado_total += ejecutado
        restante = max(restante - ejecutado, 0.0)

        if operation_id:
            metricas = real_orders_metrics.acumular_metricas(operation_id, estado)

        precio_fill = float(estado.get("price") or estado.get("average") or precio_actual)
        slippage = abs(precio_fill - precio_actual) / precio_actual if precio_actual else 0.0
        if slippage > MAX_SLIPPAGE_PCT:
            log.warning(f"⚠️ Slippage alto en {symbol}: {slippage:.2%} (máx {MAX_SLIPPAGE_PCT:.2%})")
            try:
                ro.notificador.enviar(f"Slippage alto en {symbol}: {slippage:.2%}", "WARNING")
            except Exception:
                pass

        if ejecutado > 0 and precio_fill > 0:
            vwap_num += precio_fill * ejecutado
            vwap_den += ejecutado

        if restante < min_qty or restante <= 0:
            break
        if intento >= max_reintentos:
            break

        # Ajustar precio y continuar con la cantidad restante
        if side == "buy":
            precio_actual *= 1 + offset_reprice
        else:
            precio_actual *= 1 - offset_reprice

    if restante >= min_qty:
        log.warning(
            f"⚠️ Fallback a orden de mercado en {symbol} tras {max_reintentos} intentos limit fallidos"
        )
        m_attempt = max_reintentos + 1
        if side == "buy":
            res_market = ro.ejecutar_orden_market(
                symbol,
                restante,
                operation_id,
                order_attempt=m_attempt,
                precio_senal_bot=float(precio) if precio else None,
            )
        else:
            res_market = ro.ejecutar_orden_market_sell(
                symbol,
                restante,
                operation_id,
                order_attempt=m_attempt,
                precio_senal_bot=float(precio) if precio else None,
            )
        ej_m = float(res_market.get("ejecutado", 0.0) or 0.0)
        ejecutado_total += ej_m
        restante = res_market.get("restante", 0.0)
        slippage = res_market.get("slippage", slippage)
        if operation_id:
            metricas["fee"] = res_market.get("fee", metricas.get("fee", 0.0))
            metricas["pnl"] = res_market.get("pnl", metricas.get("pnl", 0.0))
        if ej_m > 0:
            pf_m = res_market.get("precio_fill")
            if pf_m is not None:
                try:
                    pfm = float(pf_m)
                    if pfm > 0:
                        vwap_num += pfm * ej_m
                        vwap_den += ej_m
                except (TypeError, ValueError):
                    pass

    status = "FILLED" if restante <= 1e-8 else "PARTIAL"
    fill_avg = (vwap_num / vwap_den) if vwap_den > 0 else None
    return {
        "ejecutado": ejecutado_total,
        "restante": restante,
        "status": status,
        "min_qty": min_qty,
        "fee": metricas.get("fee", 0.0),
        "pnl": metricas.get("pnl", 0.0),
        "slippage": slippage,
        "precio_fill_promedio": fill_avg,
    }