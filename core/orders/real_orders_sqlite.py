# core/orders/real_orders_sqlite.py — conexión y esquema SQLite para órdenes reales
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from core.orders.order_model import Order
from core.utils.log_utils import format_exception_for_log
from core.utils.utils import configurar_logger

log = configurar_logger("ordenes")


def connect_db(ruta_db: str) -> sqlite3.Connection:
    """Abre la conexión SQLite aplicando configuraciones de rendimiento."""
    conn = sqlite3.connect(ruta_db)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except sqlite3.Error as e:
        log.warning(
            "⚠️ No se pudieron aplicar PRAGMA en SQLite: %s",
            format_exception_for_log(e),
        )
    return conn


def validar_datos_orden(data: Any) -> dict:
    """Valida campos mínimos requeridos para persistir una orden."""
    d = data.to_dict() if isinstance(data, Order) else dict(data)
    symbol = d.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("Orden inválida: 'symbol' es obligatorio")
    for campo in ("precio_entrada", "cantidad"):
        try:
            d[campo] = float(d[campo])
        except (TypeError, ValueError, KeyError):
            raise ValueError(f"Orden inválida: '{campo}' debe ser numérico")
    return d


def init_db(ruta_db: str) -> None:
    """Crea la tabla de órdenes y operaciones si no existen."""
    os.makedirs(os.path.dirname(ruta_db), exist_ok=True)
    schema_base = """
        symbol TEXT,
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
    """
    try:
        with connect_db(ruta_db) as conn:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS ordenes ({schema_base}, PRIMARY KEY(symbol))"
            )
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS operaciones (id INTEGER PRIMARY KEY AUTOINCREMENT, {schema_base})"
            )
        log.info(
            "🗃️ Tablas de órdenes y operaciones verificadas/creadas.",
            extra={"symbol": None, "timeframe": None},
        )
    except sqlite3.Error as e:
        log.error(
            "❌ Error al crear las tablas en SQLite: %s",
            format_exception_for_log(e),
        )
        raise


def load_open_orders(ruta_db: str) -> dict[str, Order]:
    """Lee órdenes abiertas desde SQLite (sin caché en memoria)."""
    init_db(ruta_db)
    ordenes: dict[str, Order] = {}
    try:
        with connect_db(ruta_db) as conn:
            conn.row_factory = sqlite3.Row
            filas = conn.execute(
                "SELECT * FROM ordenes WHERE fecha_cierre IS NULL OR fecha_cierre = ''"
            ).fetchall()
            for row in filas:
                data = dict(row)
                orden = Order.from_dict(data)
                ordenes[orden.symbol] = orden
        log.info(
            f"📥 {len(ordenes)} órdenes abiertas cargadas desde la base de datos.",
            extra={"symbol": None, "timeframe": None},
        )
    except sqlite3.Error as e:
        log.error(
            "❌ Error al cargar órdenes desde SQLite: %s",
            format_exception_for_log(e),
        )
        raise
    return ordenes


def orders_stable_json(ordenes: dict[str, Order]) -> str:
    """Serialización estable para detectar cambios respecto al caché."""
    return json.dumps({k: o.to_dict() for k, o in ordenes.items()}, sort_keys=True)


def persist_orders(ruta_db: str, ordenes: dict[str, Order]) -> None:
    """Persiste el dict completo de órdenes (INSERT OR REPLACE por fila)."""
    init_db(ruta_db)
    try:
        with connect_db(ruta_db) as conn:
            with conn:
                for orden in ordenes.values():
                    data = validar_datos_orden(orden)
                    if isinstance(data.get("estrategias_activas"), dict):
                        data["estrategias_activas"] = json.dumps(data["estrategias_activas"])
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
        log.info(
            f"💾 {len(ordenes)} órdenes guardadas correctamente en la base de datos.",
            extra={"symbol": None, "timeframe": None},
        )
    except (sqlite3.Error, ValueError) as e:
        log.error(
            "❌ Error al guardar órdenes en SQLite: %s",
            format_exception_for_log(e),
        )
        raise
