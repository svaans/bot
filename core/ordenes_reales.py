"""Utilidades para registrar y mantener las órdenes reales.

El archivo Parquet se protege de escrituras concurrentes entre procesos
mediante :class:`filelock.FileLock` cuando se persisten los datos.
"""

import os
import json
import pandas as pd
import threading
from datetime import datetime
from filelock import FileLock
from binance_api.cliente import obtener_cliente
from core.logger import configurar_logger
from core.ordenes_model import Orden

log = configurar_logger("ordenes")

_lock_archivo = threading.Lock()

RUTA_ORDENES = os.path.join("ordenes_reales", "ordenes_reales.parquet")
# Bloqueo entre procesos para evitar que varios procesos escriban simultáneamente
_lock_proceso = FileLock(RUTA_ORDENES + ".lock")
_CACHE_ORDENES: dict[str, Orden] | None = None


def cargar_ordenes() -> dict[str, Orden]:
    """Carga las órdenes almacenadas desde el archivo Parquet."""
    global _CACHE_ORDENES
    if _CACHE_ORDENES is not None:
        return _CACHE_ORDENES

    if os.path.exists(RUTA_ORDENES):
        try:
            with _lock_archivo:
                df = pd.read_parquet(RUTA_ORDENES)
            ordenes = {}
            for _, row in df.iterrows():
                data = row.to_dict()
                orden = Orden.from_dict(data)
                ordenes[orden.symbol] = orden
            _CACHE_ORDENES = ordenes
            return _CACHE_ORDENES
        except Exception as e:
            log.warning(f"⚠️ Error al leer archivo de órdenes: {e}. Se usará uno limpio.")

    _CACHE_ORDENES = {}
    return _CACHE_ORDENES

def guardar_ordenes(ordenes: dict[str, Orden]) -> None:
    """Guarda las órdenes en disco en formato Parquet."""
    global _CACHE_ORDENES
    try:
        os.makedirs(os.path.dirname(RUTA_ORDENES), exist_ok=True)
        temp = RUTA_ORDENES + ".tmp"
        with _lock_proceso:
            with _lock_archivo:
                registros = [o.to_parquet_record() for o in ordenes.values()]
                df = pd.DataFrame(registros)
                df.to_parquet(temp, index=False)
                os.replace(temp, RUTA_ORDENES)
        _CACHE_ORDENES = ordenes
        log.info("💾 Órdenes guardadas correctamente.")
    except Exception as e:
        log.error(f"❌ Error al guardar órdenes: {e}")

def obtener_orden(symbol: str) -> Orden | None:
    return cargar_ordenes().get(symbol)

def obtener_todas_las_ordenes():
    return cargar_ordenes()

def actualizar_orden(symbol, data):
    ordenes = cargar_ordenes()
    ordenes[symbol] = data
    guardar_ordenes(ordenes)
    log.info(f"📌 Orden actualizada para {symbol}.")

def eliminar_orden(symbol):
    ordenes = cargar_ordenes()
    if symbol in ordenes:
        del ordenes[symbol]
        guardar_ordenes(ordenes)
        log.info(f"🗑️ Orden eliminada para {symbol}.")
    else:
        log.warning(f"⚠️ Se intentó eliminar una orden inexistente: {symbol}.")

def registrar_orden(symbol: str, precio: float, cantidad: float, sl: float, tp: float, estrategias, tendencia) -> None:
    orden = Orden(
        symbol=symbol,
        precio_entrada=precio,
        cantidad=cantidad,
        stop_loss=sl,
        take_profit=tp,
        timestamp=datetime.utcnow().isoformat(),
        estrategias_activas=estrategias,
        tendencia=tendencia,
        max_price=precio,
    )
    actualizar_orden(symbol, orden)

def ejecutar_orden_market(symbol, cantidad):
    try:
        cliente = obtener_cliente()
        response = cliente.create_market_buy_order(symbol.replace("/", ""), cantidad)
        log.info(f"🟢 Orden real ejecutada: {symbol}, cantidad: {cantidad}")
        return response
    except Exception as e:
        log.error(f"❌ Error ejecutando orden real para {symbol}: {e}")
        return None
