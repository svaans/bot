# core/orders/order_manager_abrir.py — flujo de apertura (`OrderManager.abrir_async`)
from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Dict

from binance_api.ccxt_client import obtener_ccxt as obtener_cliente
from core.metrics import (
    limpiar_registro_pendiente,
    registrar_buy_rejected_insufficient_funds,
    registrar_orden,
    registrar_registro_error,
    registrar_registro_pendiente,
)
from core.orders import real_orders
from core.orders.order_model import Order
from core.orders.order_open_status import OrderOpenStatus
from core.orders.order_manager_helpers import (
    exchange_side_open_position as _exchange_side_open_position,
    fetch_balance_non_blocking as _fetch_balance_non_blocking,
    fmt_exchange_err as _fmt_exchange_err,
    is_short_direction as _is_short_direction,
    sim_aplicar_slippage_entrada_salida as _sim_aplicar_slippage_entrada_salida,
)
from core.orders.validators_binance import remainder_executable
from core.risk.level_validators import LevelValidationError, validate_levels
from core.utils.logger import configurar_logger, log_decision
from core.utils.log_utils import safe_extra
from core.utils.utils import is_valid_number

log = configurar_logger('orders', modo_silencioso=True)
UTC = timezone.utc

async def abrir_async(
    manager: Any,
    symbol: str,
    precio: float,
    sl: float,
    tp: float,
    estrategias: Dict,
    tendencia: str,
    direccion: str = 'long',
    cantidad: float = 0.0,
    puntaje: float = 0.0,
    umbral: float = 0.0,
    score_tecnico: float = 0.0,
    objetivo: float | None = None,
    fracciones: int = 1,
    detalles_tecnicos: dict | None = None,
    tick_size: float | None = None,
    step_size: float | None = None,
    min_dist_pct: float | None = None,
    *,
    candle_close_ts: int | None = None,   # reservado (no usado aquí)
    strategy_version: str | None = None,  # reservado (no usado aquí)
) -> OrderOpenStatus:
    operation_id = manager._generar_operation_id(symbol)
    tick_size_value = float(tick_size or 0.0)
    step_size_value = float(step_size or 0.0)
    try:
        min_dist_pct_value = float(min_dist_pct) if min_dist_pct is not None else 0.0
    except (TypeError, ValueError):
        min_dist_pct_value = 0.0

    try:
        precio, sl, tp = validate_levels(
            direccion,
            precio,
            sl,
            tp,
            min_dist_pct_value,
            tick_size_value,
            step_size_value,
        )
    except LevelValidationError as exc:
        contexto = dict(exc.context)
        contexto.update({'symbol': symbol, 'reason': exc.reason})
        entrada_log = {
            'symbol': symbol,
            'precio': contexto.get('entry', precio),
            'sl': contexto.get('sl', sl),
            'tp': contexto.get('tp', tp),
            'cantidad': cantidad,
        }
        log.warning(
            f'⚠️ {symbol}: validación SL/TP fallida ({exc.reason})',
            extra=contexto,
        )
        log_decision(
            log,
            'abrir',
            operation_id,
            entrada_log,
            {'niveles_validos': False},
            'reject',
            {'reason': 'invalid_levels', 'contexto': contexto},
        )
        registrar_orden('rejected')
        return OrderOpenStatus.FAILED
    
    entrada_log = {
        'symbol': symbol,
        'precio': precio,
        'sl': sl,
        'tp': tp,
        'cantidad': cantidad,
    }
    lock = manager._locks.setdefault(symbol, asyncio.Lock())
    async with lock:
        # La asyncio.Lock() se mantiene durante TODA la ejecución (incluyendo awaits),
        # garantizando que dos eventos concurrentes para el mismo símbolo nunca puedan
        # ejecutar en paralelo. La segunda llamada esperará en esta línea hasta que
        # la primera libere el lock en el bloque finally, en el que ya habrá registrado
        # el símbolo en manager.ordenes (éxito) o dejado abriendo vacío (fallo/retry).
        # Evitar duplicados si ya se está abriendo o existe localmente
        if symbol in manager.abriendo or symbol in manager.ordenes:
            if symbol not in manager._dup_warned:
                log.warning(f'⚠️ Orden duplicada evitada para {symbol}')
                manager._dup_warned.add(symbol)
            log_decision(
                log,
                'abrir',
                operation_id,
                entrada_log,
                {'duplicada': True},
                'reject',
                {'reason': 'duplicate'},
            )
            return OrderOpenStatus.FAILED
        if symbol in manager._registro_pendiente_paused:
            log.warning(
                '🚫 Apertura bloqueada para %s por registro pendiente persistente',
                symbol,
            )
            if manager.bus:
                await manager.bus.publish(
                    'notify',
                    {
                        'mensaje': f'🚫 Apertura bloqueada en {symbol} por registro pendiente',
                        'tipo': 'WARNING',
                        'operation_id': operation_id,
                    },
                )
            registrar_orden('rejected')
            log_decision(
                log,
                'abrir',
                operation_id,
                entrada_log,
                {'registro': 'bloqueado'},
                'reject',
                {'reason': 'registro_pendiente_bloqueado'},
            )
            return OrderOpenStatus.FAILED
        ordenes_api = {}
        if manager.modo_real:
            reintentos_sync = 3
            ultimo_error_sync: Exception | None = None
            for intento in range(1, reintentos_sync + 1):
                try:
                    ordenes_api = await asyncio.to_thread(
                        real_orders.sincronizar_ordenes_binance,
                        [symbol],
                        modo_real=manager.modo_real,
                    )
                    ultimo_error_sync = None
                    break
                except Exception as e:
                    ultimo_error_sync = e
                    log.warning(
                        '⚠️ Error verificando órdenes abiertas (intento %s/%s) para %s: %s',
                        intento,
                        reintentos_sync,
                        symbol,
                        _fmt_exchange_err(e, limit=400),
                    )
                    if intento < reintentos_sync:
                        await asyncio.sleep(0.25 * intento)
            if ultimo_error_sync is not None:
                log.error(
                    "❌ Error verificando órdenes abiertas tras reintentos: %s",
                    _fmt_exchange_err(ultimo_error_sync),
                )
                if manager.bus:
                    await manager.bus.publish(
                        'notify',
                        {
                            'mensaje': f'⚠️ No se pudo verificar órdenes abiertas en {symbol}',
                            'tipo': 'WARNING',
                            'operation_id': operation_id,
                        },
                    )
                log_decision(
                    log,
                    'abrir',
                    operation_id,
                    entrada_log,
                    {'verificacion': 'error'},
                    'reject',
                    {'reason': 'sync_error', 'intentos': reintentos_sync},
                )
                return OrderOpenStatus.FAILED
            
        if symbol in ordenes_api:
            manager.ordenes[symbol] = ordenes_api[symbol]
            if symbol not in manager._dup_warned:
                log.warning(f'⚠️ Orden duplicada evitada para {symbol}')
                manager._dup_warned.add(symbol)
            if manager.bus:
                await manager.bus.publish(
                    'notify',
                    {'mensaje': f'⚠️ Orden ya abierta en Binance para {symbol}', 'operation_id': operation_id},
                )
            log_decision(
                log,
                'abrir',
                operation_id,
                entrada_log,
                {'duplicada': True},
                'reject',
                {'reason': 'already_open'},
            )
            return OrderOpenStatus.FAILED
        
        if manager.modo_real:
            try:
                cliente = obtener_cliente()
                balance = await _fetch_balance_non_blocking(cliente)
                quote = symbol.split('/')[1]
                free_balances = balance.get('free', {}) if isinstance(balance, Mapping) else {}
                if not isinstance(free_balances, Mapping):
                    free_balances = {}
                disponible = float(free_balances.get(quote, 0.0))
            except Exception as e:
                log.error("❌ Error obteniendo balance: %s", _fmt_exchange_err(e))
                disponible = 0.0
            notional = precio * cantidad
            if not remainder_executable(symbol, precio, cantidad) or notional > disponible:
                if manager.bus:
                    await manager.bus.publish(
                        'notify', {'mensaje': 'insuficiente', 'tipo': 'WARNING', 'operation_id': operation_id}
                    )
                registrar_buy_rejected_insufficient_funds()
                registrar_orden('rejected')
                log_decision(
                    log,
                    'abrir',
                    operation_id,
                    entrada_log,
                    {
                        'saldo_disponible': disponible,
                        'notional': notional,
                    },
                    'reject',
                    {'reason': 'insufficient_funds'},
                )
                return OrderOpenStatus.FAILED

        manager.abriendo.add(symbol)
        log.info(
            "order.create.start",
            extra=safe_extra(
                {"symbol": symbol, "operation_id": operation_id, "modo_real": manager.modo_real}
            ),
        )
        try:
            precio_senal = float(precio)
            if not manager.modo_real:
                precio = _sim_aplicar_slippage_entrada_salida(
                    precio, direccion, es_entrada=True
                )
            objetivo = objetivo if objetivo is not None else cantidad
            orden = Order(
                symbol=symbol,
                precio_entrada=precio,
                cantidad=objetivo,
                cantidad_abierta=cantidad,
                stop_loss=sl,
                take_profit=tp,
                estrategias_activas=estrategias,
                tendencia=tendencia,
                timestamp=datetime.now(UTC).isoformat(),
                max_price=precio,
                direccion=direccion,
                entradas=[{'precio': precio, 'cantidad': cantidad}],
                fracciones_totales=fracciones,
                fracciones_restantes=max(fracciones - 1, 0),
                precio_ultima_piramide=precio,
                puntaje_entrada=puntaje,
                umbral_entrada=umbral,
					score_tecnico=score_tecnico,
                detalles_tecnicos=detalles_tecnicos,
                break_even_activado=False,
                duracion_en_velas=0,
                registro_pendiente=True,
                operation_id=operation_id,
            )
            manager.ordenes[symbol] = orden
            precio_para_registro = float(precio)

            if manager.bus:
                estrategias_txt = ', '.join(estrategias.keys())
                accion = "Venta" if _is_short_direction(direccion) else "Compra"
                msg_pendiente = (
                    f"""📝 {accion} creada (pendiente de registro) {symbol}
                    Precio: {precio:.2f} Cantidad: {cantidad}
                    SL: {sl:.2f} TP: {tp:.2f}
                    Estrategias: {estrategias_txt}"""
                )
                await manager.bus.publish('notify', {'mensaje': msg_pendiente, 'operation_id': operation_id})

            try:
                if manager.modo_real and is_valid_number(cantidad) and cantidad > 0:
                    side_open = _exchange_side_open_position(direccion)
                    execution = await manager._execute_real_order(
                        side_open,
                        symbol,
                        cantidad,
                        operation_id,
                        {
                            'side': side_open,
                            'symbol': symbol,
                            'cantidad': cantidad,
                            'precio_senal': precio_senal,
                        },
							precio=precio_senal,
                    )
                    # actualiza con lo realmente ejecutado
                    cantidad = float(execution.executed)
                    orden.fee_total = getattr(orden, 'fee_total', 0.0) + execution.fee
                    manager._apply_realized_pnl_delta(symbol, orden, execution.pnl)
                    precio_registro = precio_senal
                    fill_px = execution.precio_fill_promedio
                    if (
                        fill_px is not None
                        and is_valid_number(fill_px)
                        and float(fill_px) > 0
                    ):
                        precio_registro = float(fill_px)
                        orden.precio_entrada = precio_registro
                        prev_ext = float(
                            getattr(orden, "max_price", precio_registro) or precio_registro
                        )
                        if _is_short_direction(direccion):
                            orden.max_price = min(precio_registro, prev_ext)
                        else:
                            orden.max_price = max(precio_registro, prev_ext)
                        if orden.entradas and isinstance(orden.entradas[0], dict):
                            orden.entradas[0]["precio"] = precio_registro
                        orden.precio_ultima_piramide = precio_registro
                    if cantidad <= 0:
                        if manager.bus:
                            await manager.bus.publish(
                                'notify',
                                {
                                    'mensaje': f'❌ Orden real no ejecutada en {symbol}',
                                    'tipo': 'CRITICAL',
                                    'operation_id': operation_id,
                                },
                            )
                        registrar_orden('rejected')
                        log_decision(
                            log,
                            'abrir',
                            operation_id,
                            entrada_log,
                            {'ejecucion': 'sin_fills'},
                            'reject',
                            {'reason': 'no_fills'},
                        )
                        return OrderOpenStatus.FAILED
                    if execution.status == 'PARTIAL' and execution.remaining > 0:
                        log.warning(
                            'orders.execution.partial',
                            extra=safe_extra(
                                {
                                    'symbol': symbol,
                                    'side': side_open,
                                    'remaining': execution.remaining,
                                    'operation_id': operation_id,
                                }
                            ),
                        )
                    precio_para_registro = precio_registro
                else:
                    # Simulado: el “coste” inicial lo cargamos como PnL negativo hasta el cierre
                    manager._apply_realized_pnl_delta(symbol, orden, -(precio * cantidad))

                if cantidad > 0:
                    if manager.modo_real:
                        registrado = False
                        last_error: Exception | None = None
                        for _ in range(3):
                            try:
                                await asyncio.to_thread(
                                    real_orders.registrar_orden,
                                    symbol,
                                    precio_para_registro,
                                    cantidad,
                                    sl,
                                    tp,
                                    estrategias,
                                    tendencia,
                                    direccion,
                                    operation_id,
                                )
                                registrado = True
                                break
                            except Exception as e:
                                last_error = e
                                registrar_registro_error()
                                log.error(
                                    "Error registrando orden",
                                    extra=safe_extra(
                                        {
                                            "symbol": symbol,
                                            "error": _fmt_exchange_err(e),
                                        }
                                    ),
                                )
                        if not registrado:
                            msg = (
                                _fmt_exchange_err(last_error)
                                if last_error is not None
                                else "Error desconocido"
                            )
                            log.error(
                                "Error registrando orden",
                                extra=safe_extra({"symbol": symbol, "error": msg}),
                            )
                            if manager.bus:
                                await manager.bus.publish(
                                    "notify",
                                    {
                                        "mensaje": f"❌ Error registrando orden {symbol}: {msg}",
                                        "tipo": "CRITICAL",
                                        "operation_id": operation_id,
                                    },
                                )
                            if manager._should_schedule_persistence_retry(last_error):
                                reason_label = type(last_error).__name__ if last_error else 'unknown'
                                manager._schedule_registro_retry(symbol, reason=reason_label)
                        await asyncio.sleep(1)

                        if registrado:
                            orden.registro_pendiente = False
                            limpiar_registro_pendiente(symbol)
                            manager._registro_pendiente_paused.discard(symbol)
                        else:
                            registrar_orden('failed')  # mantenemos etiqueta por compatibilidad
                            if manager.bus:
                                await manager.bus.publish(
                                    'notify',
                                    {
                                        'mensaje': (
                                            f'⚠️ Orden {symbol} ejecutada pero registro pendiente; '
                                            'nuevas entradas pausadas hasta sincronización'
                                        ),
											'tipo': 'WARNING',
											'operation_id': operation_id,
                                    },
                                )
                            registrar_registro_pendiente(symbol)
                            manager._registro_pendiente_paused.add(symbol)

                        orden.cantidad_abierta = cantidad
                        orden.entradas[0]['cantidad'] = cantidad
                    
                    else:
                        if manager.bus:
                            await manager.bus.publish(
                                'orden_simulada_creada',
                                {
                                    'symbol': symbol,
                                    'precio': precio,
                                    'cantidad': cantidad,
                                    'sl': sl,
                                    'tp': tp,
                                    'estrategias': estrategias,
                                    'direccion': direccion,
                                    'operation_id': operation_id,
                                    'orden': orden.to_parquet_record(),
                                },
                            )
                        orden.registro_pendiente = False
                        limpiar_registro_pendiente(symbol)
                        manager._registro_pendiente_paused.discard(symbol)

            except Exception as e:
                err_t = _fmt_exchange_err(e)
                log.error(
                    "order.create.error",
                    extra=safe_extra(
                        {
                            "symbol": symbol,
                            "operation_id": operation_id,
                            "error": err_t,
                        }
                    ),
                )
                if manager.bus:
                    await manager.bus.publish(
                        "notify",
                        {
                            "mensaje": f"❌ Error al abrir orden en {symbol}: {err_t}",
                            "tipo": "CRITICAL",
                        },
                    )
                manager.ordenes.pop(symbol, None)
                limpiar_registro_pendiente(symbol)
                manager._registro_pendiente_paused.discard(symbol)
                registrar_orden('failed')
                return OrderOpenStatus.FAILED

        except asyncio.CancelledError:
            log.warning(
                "order.create.cancelled",
                extra=safe_extra({"symbol": symbol, "operation_id": operation_id}),
            )
            manager.ordenes.pop(symbol, None)
            limpiar_registro_pendiente(symbol)
            manager._registro_pendiente_paused.discard(symbol)
            manager.schedule_sync_after_open_cancel(symbol)
            raise
        finally:
            manager.abriendo.discard(symbol)
            manager._dup_warned.discard(symbol)

        if orden.registro_pendiente:
            log.warning(f'⚠️ Orden {symbol} pendiente de registro')
            log_decision(
                log,
                'abrir',
                operation_id,
                entrada_log,
                {'registro': 'pendiente'},
                'reject',
                {'reason': 'registro_pendiente'},
            )
            return OrderOpenStatus.PENDING_REGISTRATION

        registrar_orden('opened')
        log.info(f'🟢 Orden abierta para {symbol} @ {precio:.2f}')
        if manager.bus and manager.modo_real:
            estrategias_txt = ', '.join(estrategias.keys())
            etiq_ok = "Venta" if _is_short_direction(direccion) else "Compra"
            mensaje = (
                f"""🟢 {etiq_ok} {symbol}\nPrecio: {precio:.2f} Cantidad: {cantidad}\nSL: {sl:.2f} TP: {tp:.2f}\nEstrategias: {estrategias_txt}"""
            )
            await manager.bus.publish('notify', {'mensaje': mensaje, 'operation_id': operation_id})

        log_decision(
            log,
            'abrir',
            operation_id,
            entrada_log,
            {'validaciones': 'ok'},
            'accept',
            {'cantidad': cantidad},
        )
        manager._actualizar_capital_disponible(symbol, orden)
        log.info(
            "order.create.end",
            extra=safe_extra({"symbol": symbol, "operation_id": operation_id}),
        )
        return OrderOpenStatus.OPENED
