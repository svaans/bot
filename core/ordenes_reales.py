"""Utilidades para registrar y mantener las órdenes reales.

El archivo Parquet se protege de escrituras concurrentes entre procesos
Las órdenes se almacenan en una pequeña base SQLite para facilitar la
persistencia entre reinicios del bot.
"""

import os
import json
import sqlite3
from datetime import datetime
from binance_api.cliente import obtener_cliente
from core.logger import configurar_logger
from core.ordenes_model import Orden

log = configurar_logger("ordenes")

RUTA_DB = os.path.join("ordenes_reales", "ordenes.db")


_CACHE_ORDENES: dict[str, Orden] | None = None

def _init_db() -> None:
    """Crea la tabla de órdenes si no existe."""
    os.makedirs(os.path.dirname(RUTA_DB), exist_ok=True)
    with sqlite3.connect(RUTA_DB) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ordenes (
                symbol TEXT PRIMARY KEY,
                precio_entrada REAL,
                cantidad REAL,
                stop_loss REAL,
                take_profit REAL,
                timestamp TEXT,
                estrategias_activas TEXT,
                tendencia TEXT,
                max_price REAL,
                direccion TEXT,
                precio_cierre REAL,
                fecha_cierre TEXT,
                motivo_cierre TEXT,
                retorno_total REAL
            )
            """
        )


def cargar_ordenes() -> dict[str, Orden]:
    """Carga las órdenes almacenadas desde la base de datos."""
    global _CACHE_ORDENES
    if _CACHE_ORDENES is not None:
        return _CACHE_ORDENES

    _init_db()
    ordenes: dict[str, Orden] = {}
    try:
        with sqlite3.connect(RUTA_DB) as conn:
            conn.row_factory = sqlite3.Row
            for row in conn.execute("SELECT * FROM ordenes"):
                data = dict(row)
                orden = Orden.from_dict(data)
                ordenes[orden.symbol] = orden
    except Exception as e:
        log.warning(f"⚠️ Error al leer órdenes desde la base de datos: {e}")
    _CACHE_ORDENES = ordenes
    return _CACHE_ORDENES

def guardar_ordenes(ordenes: dict[str, Orden]) -> None:
    """Guarda las órdenes en la base de datos solo si hay cambios."""
    global _CACHE_ORDENES

    current_hash = json.dumps(
        {k: o.to_dict() for k, o in ordenes.items()}, sort_keys=True
    )
    cache_hash = None
    if _CACHE_ORDENES is not None:
        cache_hash = json.dumps(
            {k: o.to_dict() for k, o in _CACHE_ORDENES.items()}, sort_keys=True
        )
    if cache_hash == current_hash:
        return

    _init_db()
    try:
        with sqlite3.connect(RUTA_DB) as conn:
            conn.execute("DELETE FROM ordenes")
            for orden in ordenes.values():
                data = orden.to_dict() if isinstance(orden, Orden) else orden
                if isinstance(data.get("estrategias_activas"), dict):
                    data["estrategias_activas"] = json.dumps(
                        data["estrategias_activas"]
                    )
                conn.execute(
                    """
                    INSERT INTO ordenes (
                        symbol, precio_entrada, cantidad, stop_loss, take_profit,
                        timestamp, estrategias_activas, tendencia, max_price,
                        direccion, precio_cierre, fecha_cierre, motivo_cierre,
                        retorno_total
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data.get("symbol"),
                        data.get("precio_entrada"),
                        data.get("cantidad"),
                        data.get("stop_loss"),
                        data.get("take_profit"),
                        data.get("timestamp"),
                        data.get("estrategias_activas"),
                        data.get("tendencia"),
                        data.get("max_price"),
                        data.get("direccion"),
                        data.get("precio_cierre"),
                        data.get("fecha_cierre"),
                        data.get("motivo_cierre"),
                        data.get("retorno_total"),
                    ),
                )
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
    if ordenes.get(symbol) == data:
        return
    
    d = data.to_dict() if isinstance(data, Orden) else data
    if isinstance(d.get("estrategias_activas"), dict):
        d["estrategias_activas"] = json.dumps(d["estrategias_activas"])

    _init_db()
    try:
        with sqlite3.connect(RUTA_DB) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO ordenes (
                    symbol, precio_entrada, cantidad, stop_loss, take_profit,
                    timestamp, estrategias_activas, tendencia, max_price,
                    direccion, precio_cierre, fecha_cierre, motivo_cierre,
                    retorno_total
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    d.get("symbol"),
                    d.get("precio_entrada"),
                    d.get("cantidad"),
                    d.get("stop_loss"),
                    d.get("take_profit"),
                    d.get("timestamp"),
                    d.get("estrategias_activas"),
                    d.get("tendencia"),
                    d.get("max_price"),
                    d.get("direccion"),
                    d.get("precio_cierre"),
                    d.get("fecha_cierre"),
                    d.get("motivo_cierre"),
                    d.get("retorno_total"),
                ),
            )
        ordenes[symbol] = data if isinstance(data, Orden) else Orden.from_dict(d)
        _CACHE_ORDENES = ordenes
        log.info(f"📌 Orden actualizada para {symbol}.")
    except Exception as e:
        log.error(f"❌ Error actualizando la orden en la base de datos: {e}")


def eliminar_orden(symbol):
    ordenes = cargar_ordenes()
    if symbol in ordenes:
        try:
            with sqlite3.connect(RUTA_DB) as conn:
                conn.execute("DELETE FROM ordenes WHERE symbol = ?", (symbol,))
            del ordenes[symbol]
            _CACHE_ORDENES = ordenes
            log.info(f"🗑️ Orden eliminada para {symbol}.")
        except Exception as e:
            log.error(f"❌ Error eliminando orden de la base de datos: {e}")
    else:
        log.warning(f"⚠️ Se intentó eliminar una orden inexistente: {symbol}.")

def registrar_orden(
    symbol: str,
    precio: float,
    cantidad: float,
    sl: float,
    tp: float,
    estrategias,
    tendencia,
) -> None:
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
