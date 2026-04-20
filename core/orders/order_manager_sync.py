# core/orders/order_manager_sync.py — reconciliación periódica con el exchange (modo real)
from __future__ import annotations

import asyncio
import random
from typing import Any, Dict

from core.metrics import (
    limpiar_registro_pendiente,
    registrar_orden,
    registrar_orders_sync_failure,
    registrar_orders_sync_success,
    registrar_registro_pendiente,
)
from core.orders import real_orders
from core.orders.order_model import Order
from core.orders.order_manager_helpers import fmt_exchange_err, is_short_direction
from core.registro_metrico import registro_metrico
from core.utils.logger import configurar_logger
from core.utils.log_utils import safe_extra

log = configurar_logger("orders", modo_silencioso=True)


async def run_sync_once(manager: Any) -> bool:
    actuales = set(manager.ordenes.keys())
    try:
        ordenes_reconciliadas: Dict[str, Order] = await asyncio.to_thread(
            real_orders.reconciliar_ordenes
        )
    except Exception as e:
        log.error("❌ Error sincronizando órdenes: %s", fmt_exchange_err(e))
        registrar_orders_sync_failure(type(e).__name__)
        return False

    reconciliadas = set(ordenes_reconciliadas.keys())
    local_only = actuales - reconciliadas
    exchange_only = reconciliadas - actuales

    if local_only or exchange_only:
        for sym in local_only:
            log.warning(f"⚠️ Orden local {sym} ausente en exchange; cerrada.")
            if manager.bus:
                await manager.bus.publish(
                    "notify",
                    {
                        "mensaje": f"⚠️ Orden local {sym} cerrada por reconciliación",
                        "tipo": "WARNING",
                    },
                )
        for sym in exchange_only:
            log.warning(f"⚠️ Orden {sym} encontrada en exchange y añadida.")
            if manager.bus:
                await manager.bus.publish(
                    "notify",
                    {
                        "mensaje": f"🔄 Orden sincronizada desde Binance: {sym}",
                        "tipo": "WARNING",
                    },
                )
        registro_metrico.registrar(
            "discrepancia_ordenes",
            {"local": len(local_only), "exchange": len(exchange_only)},
        )

    merged: Dict[str, Order] = {}

    for sym, remoto in ordenes_reconciliadas.items():
        local = manager.ordenes.get(sym)
        if local:
            setattr(
                remoto,
                "registro_pendiente",
                getattr(local, "registro_pendiente", False)
                or getattr(remoto, "registro_pendiente", False),
            )
        merged[sym] = remoto

    manager.ordenes = merged

    errores_registro = False
    for sym, ord_ in list(manager.ordenes.items()):
        if getattr(ord_, "registro_pendiente", False):
            manager._registro_pendiente_paused.add(sym)
            registrar_registro_pendiente(sym)
            try:
                await asyncio.to_thread(
                    real_orders.registrar_orden,
                    sym,
                    ord_.precio_entrada,
                    ord_.cantidad_abierta or ord_.cantidad,
                    ord_.stop_loss,
                    ord_.take_profit,
                    ord_.estrategias_activas,
                    ord_.tendencia,
                    ord_.direccion,
                    ord_.operation_id,
                )
                ord_.registro_pendiente = False
                limpiar_registro_pendiente(sym)
                manager._registro_pendiente_paused.discard(sym)
                registrar_orden("opened")
                log.info(f"🟢 Orden registrada tras reintento para {sym}")
                if manager.bus:
                    estrategias_txt = ", ".join(ord_.estrategias_activas.keys())
                    etiq = "Venta" if is_short_direction(getattr(ord_, "direccion", None)) else "Compra"
                    mensaje = (
                        f"🟢 {etiq} {sym}\nPrecio: {ord_.precio_entrada:.2f} "
                        f"Cantidad: {ord_.cantidad_abierta or ord_.cantidad}\n"
                        f"SL: {ord_.stop_loss:.2f} TP: {ord_.take_profit:.2f}\n"
                        f"Estrategias: {estrategias_txt}"
                    )
                    await manager.bus.publish(
                        "notify",
                        {
                            "mensaje": mensaje,
                            "operation_id": ord_.operation_id,
                        },
                    )
            except Exception as e:
                err_t = fmt_exchange_err(e, limit=400)
                log.error("❌ Error registrando orden pendiente %s: %s", sym, err_t)
                if manager.bus:
                    await manager.bus.publish(
                        "notify",
                        {
                            "mensaje": f"❌ Error registrando orden pendiente {sym}: {err_t}",
                            "tipo": "CRITICAL",
                            "operation_id": ord_.operation_id,
                        },
                    )
                registrar_orders_sync_failure(type(e).__name__)
                errores_registro = True

    if errores_registro:
        return False

    registrar_orders_sync_success()
    return True


def compute_next_sync_delay(manager: Any, success: bool) -> float:
    if success:
        manager._sync_failures = 0
        manager._sync_interval = manager._sync_base_interval
    else:
        manager._sync_failures += 1
        next_interval = manager._sync_base_interval * (
            manager._sync_backoff_factor ** manager._sync_failures
        )
        manager._sync_interval = min(manager._sync_max_interval, next_interval)
    delay = max(manager._sync_min_interval, manager._sync_interval)
    if manager._sync_jitter > 0:
        jitter = random.uniform(-manager._sync_jitter, manager._sync_jitter)
        delay *= 1 + jitter
    return max(manager._sync_min_interval, min(delay, manager._sync_max_interval))


async def run_sync_loop(manager: Any) -> None:
    while True:
        try:
            success = await run_sync_once(manager)
        except asyncio.CancelledError:
            raise
        delay = compute_next_sync_delay(manager, success)
        log.info(
            "orders.background_sync_idle",
            extra=safe_extra(
                {
                    "sync_ok": success,
                    "next_sync_in_sec": round(delay, 1),
                    "ordenes_en_memoria": len(manager.ordenes),
                }
            ),
        )
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise
