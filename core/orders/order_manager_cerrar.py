# core/orders/order_manager_cerrar.py — cierre total/parcial y agregar posición
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from core.metrics import (
    limpiar_registro_pendiente,
    registrar_orden,
    registrar_partial_close_collision,
)
from core.orders import real_orders
from core.orders.market_retry_executor import ExecutionResult
from core.orders.order_manager_helpers import (
    exchange_side_open_position as _exchange_side_open_position,
    exchange_side_reduce_position as _exchange_side_reduce_position,
    fmt_exchange_err as _fmt_exchange_err,
    is_short_direction as _is_short_direction,
    sim_aplicar_slippage_entrada_salida as _sim_aplicar_slippage_entrada_salida,
)
from core.orders.validators_binance import remainder_executable
from core.utils.logger import configurar_logger, log_decision
from core.utils.log_utils import safe_extra
from core.utils.utils import is_valid_number

log = configurar_logger("orders", modo_silencioso=True)
UTC = timezone.utc


def _get_retry_delay(manager, symbol: str) -> float:
    """M-03: per-symbol retry delay, fallback al global ORDERS_PARTIAL_CLOSE_RETRY_DELAY.

    Lee el JSON crudo (sin merge con CONFIG_BASE) para que el valor solo
    sobreescriba si esta EXPLICITAMENTE definido para el simbolo.  Asi los
    tests que hacen manager._partial_close_retry_delay = 0.0 no son afectados
    por los defaults del CONFIG_BASE (que tambien contienen partial_close_retry_delay).
    """
    try:
        import json as _json
        from pathlib import Path as _Path
        from config.configuracion import RUTA_CONFIG_SIMBOLOS
        raw = _json.loads(_Path(RUTA_CONFIG_SIMBOLOS).read_text(encoding="utf-8"))
        sym_data = raw.get(symbol) or raw.get(symbol.replace("/", ""))
        if isinstance(sym_data, dict) and "partial_close_retry_delay" in sym_data:
            return max(0.0, float(sym_data["partial_close_retry_delay"]))
    except Exception:
        pass
    return manager._partial_close_retry_delay




def requeue_partial_close(
    manager,
    symbol: str,
    cantidad: float,
    precio: float,
    motivo: str,
    *,
    operation_id: str,
) -> bool:
    if not manager.bus:
        return False
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return False

    payload = {
        "symbol": symbol,
        "cantidad": cantidad,
        "precio": precio,
        "motivo": motivo,
    }

    async def _retry() -> None:
        try:
            delay = _get_retry_delay(manager, symbol)
            if delay > 0:
                await asyncio.sleep(delay)
            await manager.bus.publish("cerrar_parcial", dict(payload))
        except Exception:
            log.warning(
                "orders.partial_close.retry_failed",
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "operation_id": operation_id,
                    }
                ),
                exc_info=True,
            )

    task = loop.create_task(
        _retry(),
        name=f"orders.retry_partial_close.{symbol.replace('/', '')}",
    )
    # H-11: register the task so aclose_background_tasks can cancel it on shutdown.
    if hasattr(manager, "_registro_retry_tasks"):
        manager._registro_retry_tasks[f"__requeue_partial__{symbol}"] = task
    log.info(
        "orders.partial_close.reenqueued",
        extra=safe_extra(
            {
                "symbol": symbol,
                "cantidad": cantidad,
                "precio": precio,
                "operation_id": operation_id,
                "delay": _get_retry_delay(manager, symbol),
            }
        ),
    )
    return True

def requeue_full_close(
    manager,
    symbol: str,
    precio: float,
    motivo: str,
    *,
    operation_id: str,
) -> bool:
    if not manager.bus:
        return False
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return False

    payload = {"symbol": symbol, "precio": precio, "motivo": motivo}

    async def _retry() -> None:
        try:
            delay = _get_retry_delay(manager, symbol)
            if delay > 0:
                await asyncio.sleep(delay)
            await manager.bus.publish("cerrar_orden", dict(payload))
        except Exception:
            log.warning(
                "orders.full_close.retry_failed",
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "operation_id": operation_id,
                    }
                ),
                exc_info=True,
            )
        finally:
            # Lower the guard regardless of publish outcome so cerrar_async
            # can accept a new close signal if this retry fails permanently.
            manager._pending_close_requeue.discard(symbol)

    task = loop.create_task(
        _retry(),
        name=f"orders.retry_full_close.{symbol.replace('/', '')}",
    )
    # H-11: register the task so aclose_background_tasks can cancel it on shutdown.
    if hasattr(manager, "_registro_retry_tasks"):
        manager._registro_retry_tasks[f"__requeue_full__{symbol}"] = task
    log.info(
        "orders.full_close.reenqueued",
        extra=safe_extra(
            {
                "symbol": symbol,
                "precio": precio,
                "motivo": motivo,
                "operation_id": operation_id,
                "delay": _get_retry_delay(manager, symbol),
            }
        ),
    )
    return True
async def agregar_parcial_async(manager, symbol: str, precio: float, cantidad: float) -> bool:
    lock = manager._locks.setdefault(symbol, asyncio.Lock())
    async with lock:
        """Aumenta la posición abierta (long: compra adicional; short: venta adicional)."""
        orden = manager.ordenes.get(symbol)
        if not orden:
            return False
        
        operation_id = manager._generar_operation_id(symbol)
        execution: ExecutionResult | None = None
        side_add = _exchange_side_open_position(getattr(orden, "direccion", None))

        if manager.modo_real:
            try:
                if cantidad > 0:
                    precio_senal_py = float(precio)
                    execution = await manager._execute_real_order(
                        side_add,
                        symbol,
                        cantidad,
                        operation_id,
                        {
                            'side': side_add,
                            'symbol': symbol,
                            'cantidad': cantidad,
                            'precio_senal': precio_senal_py,
                        },
							precio=precio_senal_py,
                    )
                    cantidad = execution.executed
                    orden.fee_total = getattr(orden, 'fee_total', 0.0) + execution.fee
                    manager._apply_realized_pnl_delta(symbol, orden, execution.pnl)
            except Exception as e:
                err_t = _fmt_exchange_err(e)
                log.error("❌ No se pudo agregar posición real para %s: %s", symbol, err_t)
                if manager.bus:
                    await manager.bus.publish(
                        "notify",
                        {
                            "mensaje": f"❌ Error al agregar posición en {symbol}: {err_t}",
                            "tipo": "CRITICAL",
                            "operation_id": operation_id,
                        },
                    )
                return False
        else:
            px_sim = _sim_aplicar_slippage_entrada_salida(
                precio, orden.direccion, es_entrada=True
            )
            manager._apply_realized_pnl_delta(symbol, orden, -(px_sim * cantidad))

        total_prev = orden.cantidad_abierta
        orden.cantidad_abierta += cantidad
        orden.cantidad += cantidad

        if manager.modo_real and cantidad > 0 and execution is not None:
            fill_px = execution.precio_fill_promedio
            px_nueva = (
                float(fill_px)
                if (
                    fill_px is not None
                    and is_valid_number(fill_px)
                    and float(fill_px) > 0
                )
                else float(precio)
            )
        elif not manager.modo_real:
            px_nueva = _sim_aplicar_slippage_entrada_salida(
                precio, orden.direccion, es_entrada=True
            )
        else:
            px_nueva = float(precio)

        if orden.cantidad > 0:
            orden.precio_entrada = (
                (orden.precio_entrada * total_prev) + (px_nueva * cantidad)
            ) / orden.cantidad
        else:
            orden.precio_entrada = px_nueva  # fallback defensivo

        prev_ext = float(getattr(orden, "max_price", px_nueva) or px_nueva)
        if _is_short_direction(getattr(orden, "direccion", None)):
            orden.max_price = min(prev_ext, float(px_nueva))
        else:
            orden.max_price = max(prev_ext, float(px_nueva))
        if not getattr(orden, 'entradas', None):
            orden.entradas = []
        orden.entradas.append({'precio': px_nueva, 'cantidad': cantidad})
        orden.precio_ultima_piramide = px_nueva
        manager._actualizar_capital_disponible(symbol, orden)
        # H-05/M-10: persist updated position (cantidad_abierta, precio_entrada)
        # to SQLite so a crash between add-partial and next event doesn't lose state.
        if manager.modo_real:
            try:
                await asyncio.to_thread(real_orders.actualizar_orden, symbol, orden)
            except Exception as e:
                log.error(
                    "Error persistiendo agregar_parcial de %s en SQLite: %s",
                    symbol,
                    _fmt_exchange_err(e),
                )
        return True

async def _finalizar_cierre_completo_async(
    manager,
    symbol: str,
    orden: Any,
    precio_cierre: float,
    motivo: str,
    operation_id: str,
    *,
    log_verb: str = "cerrar",
    entrada_log: dict | None = None,
) -> None:
    """Pasos finales compartidos por cerrar_async y cerrar_parcial_async (M-07).

    Anota el cierre en la orden, calcula el retorno, emite PnL, persiste el
    historial, publica notificaciones en el bus, elimina de SQLite (modo_real)
    y limpia el registro activo en memoria.

    Fix M-07-BUG: cerrar_parcial_async usaba "Venta" hardcoded en el mensaje
    de notificación incluso para posiciones short (cuyo cierre es una compra).
    Esta función usa _is_short_direction para determinar la acción correcta.
    """
    orden.precio_cierre = precio_cierre
    orden.fecha_cierre = datetime.now(UTC).isoformat()
    orden.motivo_cierre = motivo
    manager._set_latent_pnl(symbol, orden, 0.0, emit=False)

    base = orden.precio_entrada * orden.cantidad if orden.cantidad else 0.0
    retorno = (orden.pnl_realizado / base) if base else 0.0
    orden.retorno_total = retorno
    manager._emit_pnl_update(
        symbol,
        orden,
        extra={"precio_mark": precio_cierre, "motivo": motivo, "retorno": retorno},
    )

    manager.historial.setdefault(symbol, []).append(orden.to_dict())
    if len(manager.historial[symbol]) > manager.max_historial:
        manager.historial[symbol] = manager.historial[symbol][-manager.max_historial:]

    if manager.bus:
        if retorno < 0:
            await manager._publish_registrar_perdida(symbol, retorno, orden)
        else:
            await manager.bus.publish("risk.win_streak_reset", {})

    log.info("📤 Orden cerrada para %s @ %.2f | %s", symbol, precio_cierre, motivo)

    if manager.bus:
        if manager.modo_real:
            accion_cierre = (
                "Compra"
                if _is_short_direction(getattr(orden, "direccion", None))
                else "Venta"
            )
            mensaje = (
                f"📤 {accion_cierre} {symbol}\n"
                f"Entrada: {orden.precio_entrada:.2f} Salida: {precio_cierre:.2f}\n"
                f"Retorno: {retorno * 100:.2f}%\nMotivo: {motivo}"
            )
            await manager.bus.publish(
                "notify", {"mensaje": mensaje, "operation_id": operation_id}
            )
        else:
            await manager.bus.publish(
                "orden_simulada_cerrada",
                {
                    "symbol": symbol,
                    "precio_cierre": precio_cierre,
                    "retorno": retorno,
                    "motivo": motivo,
                    "operation_id": operation_id,
                },
            )

    registrar_orden("closed")

    # Eliminar de SQLite en modo real. cerrar_async no lo hacía (M-07), dejando
    # registros huérfanos hasta el siguiente ciclo de sincronización.
    if manager.modo_real:
        try:
            await asyncio.to_thread(real_orders.eliminar_orden, symbol)
        except Exception as e:
            log.error(
                "Error eliminando orden %s de SQLite tras cierre completo: %s",
                symbol,
                _fmt_exchange_err(e),
            )

    manager.ordenes.pop(symbol, None)
    limpiar_registro_pendiente(symbol)
    manager._registro_pendiente_paused.discard(symbol)
    manager._actualizar_capital_disponible(symbol)

    log_decision(
        log,
        log_verb,
        operation_id,
        entrada_log or {"symbol": symbol},
        {"venta_exitosa": True},
        "accept",
        {"retorno": retorno},
    )


async def cerrar_async(manager, symbol: str, precio: float | None, motivo: str) -> bool:
    lock = manager._locks.setdefault(symbol, asyncio.Lock())
    async with lock:
        """Cierra la orden indicada completamente."""
        orden = manager.ordenes.get(symbol)
        if not orden:
            log.warning(f'⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}')
            return False
        if getattr(orden, 'cerrando', False) or symbol in getattr(manager, '_pending_close_requeue', set()):
            if symbol not in manager._dup_warned:
                log.warning(
                    f'⚠️ Orden duplicada evitada para {symbol}'
                    + (' (requeue pendiente)' if symbol in getattr(manager, '_pending_close_requeue', set()) else '')
                )
                manager._dup_warned.add(symbol)
            return False

        orden.cerrando = True
        operation_id = manager._generar_operation_id(symbol)
        entrada_log = {'symbol': symbol, 'precio': precio, 'cantidad': orden.cantidad}
        precio_referencia = (
            float(precio)
            if precio is not None
            else float(
                getattr(orden, 'max_price', 0.0) or getattr(orden, 'precio_entrada', 0.0) or 0.0
            )
        )
        precio_cierre_mtm = float(precio) if precio is not None else float(precio_referencia)
        execution: ExecutionResult | None = None
        try:
            venta_exitosa = True

            if manager.modo_real:
                venta_exitosa = False
                abierta = getattr(orden, "cantidad_abierta", 0.0) or 0.0
                if is_valid_number(abierta) and abierta > 1e-08:
                    cantidad = abierta
                else:
                    cantidad = orden.cantidad if is_valid_number(orden.cantidad) else 0.0

                if cantidad > 1e-08:
                    try:
                        side_close = _exchange_side_reduce_position(getattr(orden, "direccion", None))
                        execution = await manager._execute_real_order(
                            side_close,
                            symbol,
                            cantidad,
                            operation_id,
                            {
                                'side': side_close,
                                'symbol': symbol,
                                'cantidad': cantidad,
                                'precio_senal': precio_referencia,
                            },
								precio=precio,
                        )
                        restante = max(cantidad - execution.executed, 0.0)

                        if execution.executed > 0:
                            orden.fee_total = getattr(orden, 'fee_total', 0.0) + execution.fee
                            manager._apply_realized_pnl_delta(
                                symbol,
                                orden,
                                execution.pnl,
                                emit=False,
                            )

                        if restante > 0 and not remainder_executable(symbol, precio_referencia, restante):
                            log.info(f'♻️ Resto no ejecutable para {symbol}: {restante}')
                            venta_exitosa = True
                            motivo += '|non_executable_remainder'
                        elif execution.executed > 0 and restante <= 1e-08:
                            venta_exitosa = True
                        elif execution.executed > 0 and restante > 0:
                            # M-02: elevado a error — operativamente equivalente a
                            # "sin fills" y a excepcion de exchange; debe disparar
                            # las mismas alertas en sistemas de monitoreo.
                            log.error(
                                "orders.full_close.partial_fill_executable_remainder",
                                extra=safe_extra(
                                    {
                                        "symbol": symbol,
                                        "operation_id": operation_id,
                                        "executed": execution.executed,
                                        "restante": restante,
                                    }
                                ),
                            )
                            orden.cantidad_abierta = restante
                            # Raise the guard BEFORE scheduling the task so that
                            # cerrar_async rejects any concurrent close arriving
                            # while the retry is sleeping.
                            manager._pending_close_requeue.add(symbol)
                            requeued = requeue_full_close(manager,
                                symbol,
                                precio if precio is not None else precio_referencia,
                                motivo,
                                operation_id=operation_id,
                            )
                            if not requeued:
                                # Task could not be scheduled; lower guard immediately.
                                manager._pending_close_requeue.discard(symbol)
                                real_orders.registrar_venta_fallida(symbol)
                        else:
                            log.error(f'❌ Venta no ejecutada para {symbol} (sin fills)')
                            real_orders.registrar_venta_fallida(symbol)

                        if manager.bus and not venta_exitosa:
                            await manager.bus.publish('notify', {'mensaje': f'❌ Venta fallida en {symbol}', 'operation_id': operation_id})
                    except Exception as e:
                        err_t = _fmt_exchange_err(e)
                        log.error("❌ Error al cerrar orden real en %s: %s", symbol, err_t)
                        if manager.bus:
                            await manager.bus.publish(
                                "notify",
                                {
                                    "mensaje": f"❌ Venta fallida en {symbol}: {err_t}",
                                    "operation_id": operation_id,
                                },
                            )
            else:
                # Modo simulado: calcula PnL por diferencia (slippage de salida opcional)
                precio_cierre_mtm = _sim_aplicar_slippage_entrada_salida(
                    precio_referencia, orden.direccion, es_entrada=False
                )
                diff = (precio_cierre_mtm - orden.precio_entrada) * orden.cantidad
                if orden.direccion in ('short', 'venta'):
                    diff = -diff
                manager._apply_realized_pnl_delta(
                    symbol,
                    orden,
                    diff,
                    emit=False,
                )

            # Si la venta no fue exitosa, no alteramos estado de cierre ni borramos la orden
            if not venta_exitosa:
                if manager.bus and manager.modo_real:
                    await manager.bus.publish(
                        'notify',
                        {
                            'mensaje': f'⚠️ Venta no realizada, se reintentará en {symbol}',
                            'operation_id': operation_id,
                        },
                    )
                log_decision(log, 'cerrar', operation_id, entrada_log, {'venta_exitosa': False}, 'reject', {'reason': 'venta_no_realizada'})
                return False

            # Venta exitosa: cerrar y registrar
            if manager.modo_real:
                # M-09: priorizar siempre el precio de fill real sobre el precio
                # de senal. Si fill_salida no esta disponible se cae al precio de
                # senal (lo que puede distorsionar PnL hasta la siguiente reconciliacion).
                fill_salida = (
                    execution.precio_fill_promedio
                    if execution is not None
                    else None
                )
                if (
                    fill_salida is not None
                    and is_valid_number(fill_salida)
                    and float(fill_salida) > 0
                ):
                    precio_cierre_registro = float(fill_salida)
                else:
                    # Fallback al precio de senal — PnL puede estar distorsionado.
                    precio_cierre_registro = precio if precio is not None else precio_referencia
                    log.warning(
                        "orders.close.precio_fill_no_disponible",
                        extra={
                            "symbol": symbol,
                            "operation_id": operation_id,
                            "precio_senal": precio_cierre_registro,
                            "nota": "pnl_basado_en_senal_no_en_fill_real",
                        },
                    )
            else:
                precio_cierre_registro = precio_cierre_mtm
            await _finalizar_cierre_completo_async(
                manager,
                symbol,
                orden,
                precio_cierre_registro,
                motivo,
                operation_id,
                log_verb="cerrar",
                entrada_log=entrada_log,
            )
            return True

        finally:
            # Only lower cerrando if no retry task is keeping the position
            # locked. The task's own finally will discard the symbol and allow
            # future close signals once the retry resolves.
            if symbol not in getattr(manager, '_pending_close_requeue', set()):
                orden.cerrando = False
            manager._dup_warned.discard(symbol)

async def cerrar_parcial_async(manager, symbol: str, cantidad: float, precio: float, motivo: str) -> bool:
    """Cierra parcialmente la orden activa."""
    lock = manager._locks.setdefault(symbol, asyncio.Lock())
    if lock.locked():
        registrar_partial_close_collision(symbol)
        log.warning(
            'Cierre parcial concurrente; se encola segundo intento',
            extra={'symbol': symbol},
        )
    async with lock:
        orden = manager.ordenes.get(symbol)
        order_id = getattr(orden, 'operation_id', 'N/A') if orden else 'N/A'
        log.debug(
            'Enter cerrar_parcial lock',
            extra={'symbol': symbol, 'order_id': order_id},
        )
        try:
            if not orden or orden.cantidad_abierta <= 0:
                log.warning(
                    'Se intentó cierre parcial sin orden activa',
                    extra={'symbol': symbol},
                )
                return False

            cantidad = min(cantidad, orden.cantidad_abierta)

            entrada_log = {
                "symbol": symbol,
                "cantidad": cantidad,
                "precio": precio,
                "motivo": motivo,
            }
            if cantidad < 1e-08:
                log.warning(
                    'Cantidad demasiado pequeña para vender',
                    extra={'symbol': symbol, 'cantidad': cantidad},
                )
                return False

            operation_id = manager._generar_operation_id(symbol)
            precio_mtm = float(precio)
            execution: ExecutionResult | None = None

            if manager.modo_real:
                try:
                    side_pc = _exchange_side_reduce_position(getattr(orden, "direccion", None))
                    execution = await manager._execute_real_order(
                        side_pc,
                        symbol,
                        cantidad,
                        operation_id,
                        {
                            'side': side_pc,
                            'symbol': symbol,
                            'cantidad': cantidad,
                            'precio_senal': precio,
                        },
							precio=precio,
                    )
                    executed_qty = float(execution.executed or 0.0)
                    if executed_qty <= 0.0:
                        log.warning(
                            "orders.partial_close.no_fill",
                            extra=safe_extra(
                                {
                                    "symbol": symbol,
                                    "operation_id": operation_id,
                                    "requested_qty": cantidad,
                                    "status": execution.status,
                                }
                            ),
                        )
                        requeued = requeue_partial_close(manager,
                            symbol,
                            cantidad,
                            precio,
                            motivo,
                            operation_id=operation_id,
                        )
                        log_decision(
                            log,
                            'cerrar_parcial',
                            operation_id,
                            entrada_log,
                            {},
                            'reject',
                            {
                                'reason': 'no_fill',
                                'requeued': requeued,
                                'status': execution.status,
                            },
                        )
                        return False
                    cantidad = executed_qty
                    manager._apply_realized_pnl_delta(
                        symbol,
                        orden,
                        execution.pnl,
                        emit=False,
                    )
                    orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) + execution.pnl
                    fill_px = execution.precio_fill_promedio
                    if (
                        fill_px is not None
                        and is_valid_number(fill_px)
                        and float(fill_px) > 0
                    ):
                        precio_mtm = float(fill_px)
                except Exception as e:
                    err_t = _fmt_exchange_err(e)
                    log.error(
                        "Error en venta parcial",
                        extra=safe_extra({"symbol": symbol, "error": err_t}),
                    )
                    if manager.bus:
                        await manager.bus.publish(
                            "notify",
                            {
                                "mensaje": f"❌ Venta parcial fallida en {symbol}: {err_t}",
                                "operation_id": operation_id,
                            },
                        )
                    log_decision(
                        log,
                        "cerrar_parcial",
                        operation_id,
                        entrada_log,
                        {},
                        "reject",
                        {"reason": err_t},
                    )
                    return False
            else:
                precio_mtm = _sim_aplicar_slippage_entrada_salida(
                    float(precio), orden.direccion, es_entrada=False
                )
                diff = (precio_mtm - orden.precio_entrada) * cantidad
                if orden.direccion in ('short', 'venta'):
                    diff = -diff
                manager._apply_realized_pnl_delta(
                    symbol,
                    orden,
                    diff,
                    emit=False,
                )

            orden.cantidad_abierta -= cantidad
            manager.actualizar_mark_to_market(symbol, precio_mtm)

            log.info(f'📤 Cierre parcial de {symbol}: {cantidad} @ {precio_mtm:.2f} | {motivo}')
            if manager.bus:
                mensaje = f"""📤 Venta parcial {symbol}\nCantidad: {cantidad}\nPrecio: {precio_mtm:.2f}\nMotivo: {motivo}"""
                await manager.bus.publish('notify', {'mensaje': mensaje, 'operation_id': operation_id})

            if orden.cantidad_abierta <= 0:
                await _finalizar_cierre_completo_async(
                    manager,
                    symbol,
                    orden,
                    precio_mtm,
                    motivo,
                    operation_id,
                    log_verb="cerrar_parcial",
                    entrada_log={"symbol": symbol, "cantidad": cantidad},
                )
            else:
                registrar_orden('partial')
                log_decision(log, 'cerrar_parcial', operation_id, {'symbol': symbol, 'cantidad': cantidad}, {}, 'accept', {'parcial': True})
                manager._actualizar_capital_disponible(symbol, orden)
                # H-05: persist cantidad_abierta to SQLite after each partial fill
                # so that a crash between partial and total close doesn't lose state.
                if manager.modo_real:
                    try:
                        await asyncio.to_thread(real_orders.actualizar_orden, symbol, orden)
                    except Exception as e:
                        log.error(
                            "Error persistiendo cierre parcial de %s en SQLite: %s",
                            symbol,
                            _fmt_exchange_err(e),
                        )
            return True
        finally:
            log.debug(f'Exit cerrar_parcial lock {symbol} id={order_id}')
