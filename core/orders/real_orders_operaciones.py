# core/orders/real_orders_operaciones.py — buffer y persistencia de operaciones (SQLite + Parquet)
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from typing import Any

from core.orders import real_orders_sqlite
from core.orders.order_model import Order
from core.utils.log_utils import format_exception_for_log
from core.utils.utils import configurar_logger, guardar_orden_real

log = configurar_logger("ordenes")

_BUFFER_LOCK = threading.RLock()
_BUFFER_OPERACIONES: list[dict[str, Any]] = []
_MAX_BUFFER = int(os.getenv("MAX_BUFFER_OPERATIONS", "10") or 10)
_FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "300") or 300)
_ULTIMO_FLUSH = time.time()
_SLOW_FLUSHES = 0
_SLOW_FLUSH_THRESHOLD = 30
_SLOW_FLUSH_LIMIT = 3
_USE_PROCESS_POOL = os.getenv("USE_PROCESS_POOL", "0").lower() in {"1", "true", "yes"}
# Número máximo de operaciones a persistir por lote.
_FLUSH_BATCH_SIZE = int(os.getenv("FLUSH_BATCH_SIZE", "100") or 100)


def _chunked(seq: list[Any], size: int):
    """Divide ``seq`` en lotes del ``size`` indicado."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def persist_operaciones_batch(
    operaciones: list[dict[str, Any]], ruta_db: str
) -> tuple[int, int, list[dict[str, Any]]]:
    """Persiste operaciones en SQLite y Parquet devolviendo conteos de errores y pendientes."""
    real_orders_sqlite.init_db(ruta_db)
    errores_sqlite = 0
    errores_parquet = 0
    pendientes: list[dict[str, Any]] = []
    try:
        with real_orders_sqlite.connect_db(ruta_db) as conn:
            for op in operaciones:
                data = op.copy()
                if isinstance(data.get("estrategias_activas"), dict):
                    data["estrategias_activas"] = json.dumps(data["estrategias_activas"])
                try:
                    conn.execute(
                        """
                        INSERT INTO operaciones (
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
                except sqlite3.Error as e:
                    log.error(
                        "❌ Error SQLite al insertar operación para %s: %s",
                        data.get("symbol"),
                        format_exception_for_log(e),
                    )
                    errores_sqlite += 1
                    pendientes.append(op)
    except sqlite3.Error as e:
        log.error(
            "❌ Error global al guardar operaciones en SQLite: %s",
            format_exception_for_log(e),
        )
        errores_sqlite += 1
        pendientes.extend(operaciones)
    for op in operaciones:
        data = op.copy()
        symbol = data.get("symbol")
        if isinstance(data.get("estrategias_activas"), dict):
            data["estrategias_activas"] = json.dumps(data["estrategias_activas"])
        if symbol:
            try:
                guardar_orden_real(symbol, data)
            except Exception as e:
                log.error(
                    "❌ Error guardando operación en Parquet para %s: %s",
                    symbol,
                    format_exception_for_log(e),
                )
                errores_parquet += 1
                if op not in pendientes:
                    pendientes.append(op)
    return errores_sqlite, errores_parquet, pendientes


def flush_buffer_operaciones(ruta_db: str) -> None:
    """Guarda en disco todas las operaciones acumuladas en el buffer global del módulo."""
    global _SLOW_FLUSHES, _USE_PROCESS_POOL, _ULTIMO_FLUSH, _FLUSH_BATCH_SIZE
    with _BUFFER_LOCK:
        operaciones = list(_BUFFER_OPERACIONES)
        _BUFFER_OPERACIONES.clear()
    total_ops = len(operaciones)
    if not total_ops:
        return
    log.info(
        f"📝 Iniciando flush_operaciones con {total_ops} operaciones.",
        extra={"symbol": None, "timeframe": None},
    )
    inicio = time.time()
    errores_sqlite = errores_parquet = 0
    pendientes: list[dict[str, Any]] = []
    batches = list(_chunked(operaciones, _FLUSH_BATCH_SIZE))
    worker = partial(persist_operaciones_batch, ruta_db=ruta_db)
    if _USE_PROCESS_POOL:
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(worker, b) for b in batches]
            for f in as_completed(futures):
                e_sql, e_parq, pend = f.result()
                errores_sqlite += e_sql
                errores_parquet += e_parq
                pendientes.extend(pend)
    else:
        for b in batches:
            e_sql, e_parq, pend = worker(b)
            errores_sqlite += e_sql
            errores_parquet += e_parq
            pendientes.extend(pend)
    duracion = time.time() - inicio
    mensaje_timeout = int(os.getenv("MENSAJE_TIMEOUT", "0") or 0)
    inactivity_intervals = int(os.getenv("INACTIVITY_INTERVALS", "0") or 0)
    log.info(
        f"🏁 flush_operaciones finalizado en {duracion:.2f}s para {total_ops} operaciones.",
        extra={"symbol": None, "timeframe": None},
    )
    if mensaje_timeout and duracion > mensaje_timeout:
        log.warning(
            f"⏱️ Duración {duracion:.2f}s supera mensaje_timeout {mensaje_timeout}s"
        )
    if inactivity_intervals and duracion > inactivity_intervals:
        log.warning(
            f"⏱️ Duración {duracion:.2f}s supera inactivity_intervals {inactivity_intervals}s"
        )
    if duracion > _SLOW_FLUSH_THRESHOLD:
        _SLOW_FLUSHES += 1
        log.warning(
            f"⚠️ flush_operaciones tardó {duracion:.2f}s; considerar reducir "
            "FLUSH_BATCH_SIZE o activar USE_PROCESS_POOL"
        )
        if _FLUSH_BATCH_SIZE > 10:
            nuevo_tam = max(_FLUSH_BATCH_SIZE // 2, 10)
            if nuevo_tam < _FLUSH_BATCH_SIZE:
                _FLUSH_BATCH_SIZE = nuevo_tam
                log.info(
                    f"ℹ️ FLUSH_BATCH_SIZE reducido a {_FLUSH_BATCH_SIZE} por flush lento",
                    extra={"symbol": None, "timeframe": None},
                )
    else:
        _SLOW_FLUSHES = 0
    if _SLOW_FLUSHES >= _SLOW_FLUSH_LIMIT and not _USE_PROCESS_POOL:
        log.warning(
            "⚠️ flush_operaciones excede 30s de forma recurrente; activando ProcessPoolExecutor"
        )
        _USE_PROCESS_POOL = True

    if pendientes:
        with _BUFFER_LOCK:
            _BUFFER_OPERACIONES[:0] = pendientes

    _ULTIMO_FLUSH = time.time()

    if errores_sqlite == 0 and errores_parquet == 0:
        log.info(
            f"✅ {total_ops} operaciones guardadas correctamente.",
            extra={"symbol": None, "timeframe": None},
        )
    else:
        log.warning(
            f"⚠️ Guardadas {total_ops} operaciones con errores — SQLite: {errores_sqlite}, Parquet: {errores_parquet}"
        )


def registrar_operacion_en_buffer(data: dict | Order, ruta_db: str) -> None:
    """Agrega una operación ejecutada al buffer; puede disparar flush."""
    global _ULTIMO_FLUSH
    registro = data.to_dict() if isinstance(data, Order) else data
    symbol = registro.get("symbol")
    if not symbol:
        log.warning(
            "⚠️ Registro sin símbolo recibido en registrar_operacion(), ignorado."
        )
        return
    with _BUFFER_LOCK:
        _BUFFER_OPERACIONES.append(registro)
        log.debug(
            f"📥 Operación registrada en buffer para {symbol}",
            extra={"symbol": symbol, "timeframe": None},
        )
        ahora = time.time()
        if (
            len(_BUFFER_OPERACIONES) >= _MAX_BUFFER
            or ahora - _ULTIMO_FLUSH >= _FLUSH_INTERVAL
        ):
            log.debug(
                "🔁 Buffer de operaciones lleno o expirado, iniciando flush...",
                extra={"symbol": symbol, "timeframe": None},
            )
            flush_buffer_operaciones(ruta_db)
            _ULTIMO_FLUSH = ahora
