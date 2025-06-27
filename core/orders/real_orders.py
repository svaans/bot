"""Utilidades para registrar y mantener las órdenes reales.

El archivo Parquet se protege de escrituras concurrentes entre procesos
Las órdenes se almacenan en una pequeña base SQLite para facilitar la
persistencia entre reinicios del bot.
"""

import os
import sys
import json
import sqlite3
import time
import atexit
import signal
import threading
import asyncio
from datetime import datetime
from binance_api.cliente import obtener_cliente
from .order_model import Order
from core.utils.utils import configurar_logger
from core.async_utils import log_exceptions_async
from . import real_orders
from core.utils.utils import guardar_orden_real

import math

log = configurar_logger("ordenes")

# Base absoluto del proyecto para almacenar la base de datos siempre en la
# misma ubicación independientemente de desde dónde se ejecute el script.
# Usamos la raíz del proyecto para garantizar que la base de datos siempre
# se ubique en ``ordenes_reales/`` independientemente de dónde se ejecute.
BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
RUTA_DB = os.path.join(BASE_DIR, "ordenes_reales", "ordenes.db")

_CACHE_ORDENES: dict[str, Order] | None = None
_VENTAS_FALLIDAS: set[str] = set()
_BUFFER_OPERACIONES: list[dict] = []
_BUFFER_LOCK = threading.Lock()
_MAX_BUFFER = 10
_FLUSH_INTERVAL = 300
_ULTIMO_FLUSH = time.time()

def esperar_balance(
    cliente,
    symbol: str,
    cantidad_esperada: float,
    max_intentos: int = 10,
    delay: float = 0.3,
) -> float:
    """Espera hasta que el balance disponible alcance la cantidad esperada."""
    try:
        base = symbol.split("/")[0]
    except Exception as e:
        log.error(f"❌ Error al interpretar símbolo {symbol}: {e}")
        return 0.0

    for intento in range(max_intentos):
        try:
            balance = cliente.fetch_balance()
            disponible = balance.get("free", {}).get(base, 0.0)
            if disponible >= cantidad_esperada:
                return disponible
        except Exception as e:
            log.warning(f"⚠️ Error al obtener balance en intento {intento + 1}/{max_intentos}: {e}")
        time.sleep(delay)

    log.warning(
        f"⏱️ Tiempo de espera agotado para obtener balance suficiente en {symbol}. "
        f"Disponible: {disponible}, requerido: {cantidad_esperada}"
    )
    return disponible


async def esperar_balance_async(
    cliente,
    symbol: str,
    cantidad_esperada: float,
    max_intentos: int = 10,
    delay: float = 0.3,
) -> float:
    """Versión asíncrona de :func:`esperar_balance`."""
    try:
        base = symbol.split("/")[0]
    except Exception as e:
        log.error(f"❌ Error al interpretar símbolo {symbol}: {e}")
        return 0.0

    loop = asyncio.get_running_loop()
    for intento in range(max_intentos):
        try:
            balance = await loop.run_in_executor(None, cliente.fetch_balance)
            disponible = balance.get("free", {}).get(base, 0.0)
            if disponible >= cantidad_esperada:
                return disponible
        except Exception as e:
            log.warning(
                f"⚠️ Error al obtener balance en intento {intento + 1}/{max_intentos}: {e}"
            )
        await asyncio.sleep(delay)
    
    log.warning(
        f"⏱️ Tiempo de espera agotado para obtener balance suficiente en {symbol}. "
        f"Disponible: {disponible}, requerido: {cantidad_esperada}"
    )
    return disponible

def _init_db() -> None:
    """Crea la tabla de órdenes y operaciones si no existen."""
    os.makedirs(os.path.dirname(RUTA_DB), exist_ok=True)

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
        with sqlite3.connect(RUTA_DB) as conn:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS ordenes ({schema_base}, PRIMARY KEY(symbol))"
            )
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS operaciones (id INTEGER PRIMARY KEY AUTOINCREMENT, {schema_base})"
            )
        log.info("🗃️ Tablas de órdenes y operaciones verificadas/creadas.")
    except sqlite3.Error as e:
        log.error(f"❌ Error al crear las tablas en SQLite: {e}")
        raise


def cargar_ordenes() -> dict[str, Order]:
    """Carga las órdenes almacenadas desde la base de datos SQLite."""
    global _CACHE_ORDENES
    if _CACHE_ORDENES is not None:
        return _CACHE_ORDENES

    _init_db()
    ordenes: dict[str, Order] = {}
    try:
        with sqlite3.connect(RUTA_DB) as conn:
            conn.row_factory = sqlite3.Row
            filas = conn.execute("SELECT * FROM ordenes").fetchall()
            for row in filas:
                data = dict(row)
                orden = Order.from_dict(data)
                ordenes[orden.symbol] = orden
        log.info(f"📥 {len(ordenes)} órdenes cargadas desde la base de datos.")
    except sqlite3.Error as e:
        log.error(f"❌ Error al cargar órdenes desde SQLite: {e}")
        return {}  # Devuelve vacío en caso de fallo en vez de lanzar
    _CACHE_ORDENES = ordenes
    return _CACHE_ORDENES

def guardar_ordenes(ordenes: dict[str, Order]) -> None:
    """Guarda las órdenes en la base de datos si han cambiado respecto al caché."""
    global _CACHE_ORDENES

    def ordenar_dict(d):
        return json.dumps({k: o.to_dict() for k, o in d.items()}, sort_keys=True)

    if _CACHE_ORDENES and ordenar_dict(_CACHE_ORDENES) == ordenar_dict(ordenes):
        return  # Sin cambios

    _init_db()
    try:
        with sqlite3.connect(RUTA_DB) as conn:
            for orden in ordenes.values():
                data = orden.to_dict() if isinstance(orden, Order) else orden
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
            conn.commit()
        _CACHE_ORDENES = ordenes
        log.info(f"💾 {len(ordenes)} órdenes guardadas correctamente en la base de datos.")
    except sqlite3.Error as e:
        log.error(f"❌ Error al guardar órdenes en SQLite: {e}")
        raise


def obtener_orden(symbol: str) -> Order | None:
    try:
        return cargar_ordenes().get(symbol)
    except Exception as e:
        log.error(f"❌ Error al obtener orden de {symbol}: {e}")
        return None

def obtener_todas_las_ordenes():
    return cargar_ordenes()

def sincronizar_ordenes_binance(simbolos: list[str] | None = None) -> dict[str, Order]:
    """Consulta órdenes abiertas directamente desde Binance y las registra.

    Esto permite reconstruir el estado de las posiciones cuando el bot se
    reinicia y la base de datos local no contiene todas las operaciones
    abiertas. Devuelve el diccionario de órdenes resultante.
    """
    try:
        cliente = obtener_cliente()
        ordenes_api = []
        if simbolos:
            for s in simbolos:
                ordenes_api.extend(cliente.fetch_open_orders(s))
        else:
            ordenes_api = cliente.fetch_open_orders()
    except Exception as e:
        log.error(f"❌ Error consultando órdenes abiertas: {e}")
        return cargar_ordenes()

    for o in ordenes_api:
        symbol = o.get("symbol")
        if not symbol:
            continue
        price = float(o.get("price") or o.get("average") or 0)
        amount = float(o.get("amount") or o.get("remaining") or 0)
        side = o.get("side", "buy").lower()
        direccion = "long" if side == "buy" else "short"
        registrar_orden(symbol, price, amount, 0.0, 0.0, {}, "", direccion)

    return cargar_ordenes()

def actualizar_orden(symbol: str, data: Order | dict) -> None:
    ordenes = cargar_ordenes()
    if ordenes.get(symbol) == data:
        return

    try:
        d = data.to_dict() if isinstance(data, Order) else data.copy()
        if isinstance(d.get("estrategias_activas"), dict):
            d["estrategias_activas"] = json.dumps(d["estrategias_activas"])

        _init_db()
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
            conn.commit()

        ordenes[symbol] = data if isinstance(data, Order) else Order.from_dict(d)
        _CACHE_ORDENES = ordenes
        log.info(f"📌 Order actualizada para {symbol}.")
    except Exception as e:
        log.error(f"❌ Error al actualizar orden para {symbol}: {e}")
        raise



def eliminar_orden(symbol: str, forzar_log: bool = False) -> None:
    """Elimina una orden activa del sistema si existe."""
    ordenes = cargar_ordenes()

    if symbol not in ordenes:
        if forzar_log:
            log.warning(f"⚠️ Intento de eliminar orden inexistente: {symbol}")
        else:
            log.debug(f"Intento de eliminar orden inexistente ignorado: {symbol}")
        return

    try:
        with sqlite3.connect(RUTA_DB) as conn:
            conn.execute("DELETE FROM ordenes WHERE symbol = ?", (symbol,))
            conn.commit()

        del ordenes[symbol]
        _CACHE_ORDENES = ordenes
        log.info(f"🗑️ Order eliminada correctamente para {symbol}.")
    except sqlite3.Error as e:
        log.error(f"❌ Error eliminando orden de la base de datos: {e}")
        raise


def registrar_orden(
    symbol: str,
    precio: float,
    cantidad: float,
    sl: float,
    tp: float,
    estrategias,
    tendencia: str,
    direccion: str = "long",
) -> None:
    """Registra una nueva orden activa y la guarda en base de datos."""
    
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("❌ El símbolo debe ser una cadena no vacía.")

    if precio <= 0 or cantidad <= 0:
        raise ValueError(f"❌ Precio o cantidad inválidos para {symbol}: precio={precio}, cantidad={cantidad}")

    if not isinstance(estrategias, dict):
        log.warning(f"⚠️ Estrategias activas no en formato dict para {symbol}, se forzará conversión...")
        estrategias = dict(estrategias) if estrategias else {}

    orden = Order(
        symbol=symbol,
        precio_entrada=precio,
        cantidad=cantidad,
        stop_loss=sl,
        take_profit=tp,
        timestamp=datetime.utcnow().isoformat(),
        estrategias_activas=estrategias,
        tendencia=tendencia,
        max_price=precio,
        direccion=direccion,
    )
    actualizar_orden(symbol, orden)
    log.info(f"✅ Order registrada para {symbol} — cantidad: {cantidad}, entrada: {precio}")


def registrar_operacion(data: dict | Order) -> None:
    """Agrega una operación ejecutada al buffer. Se persistirá automáticamente."""
    global _ULTIMO_FLUSH

    registro = data.to_dict() if isinstance(data, Order) else data

    symbol = registro.get("symbol")
    if not symbol:
        log.warning("⚠️ Registro sin símbolo recibido en registrar_operacion(), ignorado.")
        return

    with _BUFFER_LOCK:
        _BUFFER_OPERACIONES.append(registro)
        log.debug(f"📥 Operación registrada en buffer para {symbol}")

    ahora = time.time()
    if (
        len(_BUFFER_OPERACIONES) >= _MAX_BUFFER
        or ahora - _ULTIMO_FLUSH >= _FLUSH_INTERVAL
    ):
        log.debug("🔁 Buffer de operaciones lleno o expirado, iniciando flush...")
        flush_operaciones()
        _ULTIMO_FLUSH = ahora

def ejecutar_orden_market(symbol: str, cantidad: float) -> float:
    """Ejecuta una compra de mercado y devuelve la cantidad realmente comprada."""

    if cantidad <= 0:
        log.warning(f"⚠️ Cantidad inválida para compra en {symbol}: {cantidad}")
        return 0.0

    try:
        cliente = obtener_cliente()
        markets = cliente.load_markets()
        market_info = markets.get(symbol.replace("/", ""), {})

        min_amount = float(market_info.get("limits", {}).get("amount", {}).get("min") or 0)
        min_cost = float(market_info.get("limits", {}).get("cost", {}).get("min") or 0)

        ticker = cliente.fetch_ticker(symbol.replace("/", ""))
        precio = float(ticker.get("last") or ticker.get("close") or 0)

        if cantidad < min_amount or (precio and cantidad * precio < min_cost):
            log.error(
                f"⛔ Compra inválida para {symbol}. Cantidad: {cantidad}, "
                f"Precio: {precio}, Mínimos → amount: {min_amount}, notional: {min_cost}"
            )
            return 0.0

        log.debug(f"📤 Enviando orden de compra para {symbol} | Cantidad: {cantidad} | Precio estimado: {precio:.4f}")
        response = cliente.create_market_buy_order(symbol.replace("/", ""), cantidad)
        ejecutado = float(response.get("amount") or response.get("filled") or 0)
        if ejecutado <= 0:
            ejecutado = cantidad
        log.info(f"🟢 Order real ejecutada: {symbol}, cantidad: {ejecutado}")
        return ejecutado

    except BaseError as e:
        log.error(f"❌ Error en Binance al ejecutar compra en {symbol}: {e}")
        raise
    except Exception as e:
        log.error(f"❌ Error estructural al ejecutar orden real en {symbol}: {e}")
        raise

def ejecutar_orden_market_sell(symbol: str, cantidad: float) -> float:
    """Ejecuta una venta de mercado validando saldo, límites y precision exacto."""

    if symbol in _VENTAS_FALLIDAS:
        log.warning(
            f"⏭️ Venta omitida para {symbol} por intento previo fallido de saldo."
        )
        return 0.0

    try:
        cliente = obtener_cliente()

        # 1. Verificar saldo base disponible
        balance = cliente.fetch_balance()
        base = symbol.split("/")[0]
        disponible = balance.get("free", {}).get(base, 0)

        if disponible <= 0:
            raise InsufficientFunds(f"Saldo 0 disponible para {symbol}")

        # 2. Obtener info del mercado
        markets = cliente.load_markets()
        info = markets.get(symbol.replace("/", ""), {})

        precision = info.get("precision", {}).get("amount", 8)
        step_size = 10**-precision

        # 3. Calcular la cantidad máxima exacta permitida (ajustada al step_size)
        cantidad_vender = math.floor(disponible / step_size) * step_size
        cantidad_vender = min(cantidad, cantidad_vender)

        if cantidad_vender <= 0:
            raise ValueError(f"Cantidad inválida ({cantidad_vender}) para vender en {symbol}")

        # 4. Validar mínimos del mercado
        min_amount = float(info.get("limits", {}).get("amount", {}).get("min") or 0)
        min_cost = float(info.get("limits", {}).get("cost", {}).get("min") or 0)

        ticker = cliente.fetch_ticker(symbol.replace("/", ""))
        precio = float(ticker.get("last") or ticker.get("close") or 0)

        if cantidad_vender < min_amount or (precio and cantidad_vender * precio < min_cost):
            log.error(
                f"⛔ Venta rechazada por mínimos: {symbol} → cantidad: {cantidad_vender:.8f}, "
                f"mínimos: amount={min_amount}, notional={min_cost}"
            )
            _VENTAS_FALLIDAS.add(symbol)
            return 0.0

        # 5. Ejecutar orden real
        log.info(f"💱 Ejecutando venta real en {symbol}: {cantidad_vender:.8f} unidades (precio estimado: {precio:.2f})")

        response = cliente.create_market_sell_order(
            symbol.replace("/", ""), cantidad_vender
        )

        ejecutado = float(response.get("amount") or response.get("filled") or 0)
        if ejecutado <= 0:
            ejecutado = cantidad_vender

        log.info(f"🔴 Order de venta ejecutada: {symbol}, cantidad: {ejecutado:.8f}")
        _VENTAS_FALLIDAS.discard(symbol)
        return ejecutado

    except InsufficientFunds as e:
        log.error(f"❌ Venta rechazada por saldo insuficiente en {symbol}: {e}")
        _VENTAS_FALLIDAS.add(symbol)
        return 0.0

    except BaseError as e:
        log.error(f"❌ Error en intercambio al vender {symbol}: {e}")
        raise

    except Exception as e:
        log.exception(f"❌ Error estructural al ejecutar venta para {symbol}: {e}")
        raise



def _persist_operations(operaciones: list[dict]) -> None:
    """Guarda en disco una lista de operaciones de forma segura y eficiente."""

    if not operaciones:
        return

    _init_db()
    errores_sqlite = 0
    errores_parquet = 0

    try:
        with sqlite3.connect(RUTA_DB) as conn:
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
                    log.error(f"❌ Error SQLite al insertar operación para {data.get('symbol')}: {e}")
                    errores_sqlite += 1

    except sqlite3.Error as e:
        log.error(f"❌ Error global al guardar operaciones en SQLite: {e}")
        errores_sqlite += 1

    for op in operaciones:
        data = op.copy()
        symbol = data.get("symbol")

        if isinstance(data.get("estrategias_activas"), dict):
            data["estrategias_activas"] = json.dumps(data["estrategias_activas"])

        if symbol:
            try:
                guardar_orden_real(symbol, data)
            except Exception as e:
                log.error(f"❌ Error guardando operación en Parquet para {symbol}: {e}")
                errores_parquet += 1


    if errores_sqlite == 0 and errores_parquet == 0:
        log.info(f"✅ {len(operaciones)} operaciones guardadas correctamente.")
    else:
        log.warning(
            f"⚠️ Guardadas {len(operaciones)} operaciones con errores — SQLite: {errores_sqlite}, Parquet: {errores_parquet}"
        )


def flush_operaciones() -> None:
    """Envía las operaciones acumuladas al worker de persistencia."""

    with _BUFFER_LOCK:
        operaciones = list(_BUFFER_OPERACIONES)
        _BUFFER_OPERACIONES.clear()

    if not operaciones:
        return

    from core.workers.order_worker_client import send_async

    batch = orders_pb2.OrdersBatch(
        orders=[
            orders_pb2.Order(
                symbol=op.get("symbol", ""),
                precio_entrada=op.get("precio_entrada", 0.0),
                cantidad=op.get("cantidad", 0.0),
                stop_loss=op.get("stop_loss", 0.0),
                take_profit=op.get("take_profit", 0.0),
                timestamp=op.get("timestamp", ""),
                estrategias_activas=(
                    json.dumps(op["estrategias_activas"])
                    if isinstance(op.get("estrategias_activas"), dict)
                    else op.get("estrategias_activas", "")
                ),
                tendencia=op.get("tendencia", ""),
                max_price=op.get("max_price", 0.0),
                direccion=op.get("direccion", ""),
                precio_cierre=op.get("precio_cierre", 0.0),
                fecha_cierre=op.get("fecha_cierre", ""),
                motivo_cierre=op.get("motivo_cierre", ""),
                retorno_total=op.get("retorno_total", 0.0),
            )
            for op in operaciones
        ]
    )

    send_async(batch)
    global _ULTIMO_FLUSH
    _ULTIMO_FLUSH = time.time()

@log_exceptions_async
async def flush_periodico(interval: int = _FLUSH_INTERVAL) -> None:
    """Ejecuta :func:`flush_operaciones` cada ``interval`` segundos."""
    while True:
        await asyncio.sleep(interval)
        try:
            await asyncio.to_thread(flush_operaciones)
        except Exception as e:  # noqa: BLE001
            log.error(f"❌ Error en flush periódico: {e}")
            
def _handle_exit(signum, frame) -> None:
    log.info(f"📴 Señal de salida recibida ({signal.Signals(signum).name}). Guardando operaciones...")
    try:
        flush_operaciones()
        from core.workers.order_worker_client import wait_pending
        wait_pending()
        log.info("✅ Buffer de operaciones guardado correctamente al salir.")
    except Exception as e:
        log.error(f"❌ Error al guardar operaciones en la salida: {e}")
    finally:
        if "PYTEST_CURRENT_TEST" not in os.environ:
            sys.exit(0)

# Registrar señales de salida seguras
for _sig in (signal.SIGTERM, signal.SIGINT):
    try:
        signal.signal(_sig, _handle_exit)
    except (ValueError, RuntimeError) as e:
        log.warning(f"⚠️ No se pudo registrar la señal {_sig}: {e}")

# Registro a la salida normal del proceso
def _flush_and_wait() -> None:
    flush_operaciones()
    from core.workers.order_worker_client import wait_pending
    wait_pending()

atexit.register(_flush_and_wait)
