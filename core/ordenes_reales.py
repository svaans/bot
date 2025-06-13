"""Utilidades para registrar y mantener las órdenes reales.

El archivo Parquet se protege de escrituras concurrentes entre procesos
Las órdenes se almacenan en una pequeña base SQLite para facilitar la
persistencia entre reinicios del bot.
"""

import os
import json
import sqlite3
import time
import atexit
import signal
import threading
from datetime import datetime
from binance_api.cliente import obtener_cliente
from core.logger import configurar_logger
from ccxt.base.errors import InsufficientFunds, BaseError
from core.ordenes_model import Orden
from core.utils import guardar_orden_real

log = configurar_logger("ordenes")

RUTA_DB = os.path.join("ordenes_reales", "ordenes.db")


_CACHE_ORDENES: dict[str, Orden] | None = None
# Registra símbolos con intentos fallidos de venta por saldo insuficiente
_VENTAS_FALLIDAS: set[str] = set()

# Buffer temporal de operaciones realizadas
_BUFFER_OPERACIONES: list[dict] = []
_BUFFER_LOCK = threading.Lock()
_MAX_BUFFER = 10
_FLUSH_INTERVAL = 300  # segundos
_ULTIMO_FLUSH = time.time()

def esperar_balance(
    cliente,
    symbol: str,
    cantidad_esperada: float,
    max_intentos: int = 10,
    delay: float = 0.3,
) -> float:
    """Espera hasta que el balance disponible alcance la cantidad esperada."""

    base = symbol.split("/")[0]
    disponible = 0.0
    for _ in range(max_intentos):
        balance = cliente.fetch_balance()
        disponible = balance.get("free", {}).get(base, 0)
        if disponible >= cantidad_esperada:
            break
        time.sleep(delay)
    return disponible

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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS operaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    except sqlite3.Error as e:
        log.warning(f"⚠️ Error al leer órdenes desde la base de datos: {e}")
        raise
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
    except sqlite3.Error as e:
        log.error(f"❌ Error al guardar órdenes: {e}")
        raise

def obtener_orden(symbol: str) -> Orden | None:
    return cargar_ordenes().get(symbol)

def obtener_todas_las_ordenes():
    return cargar_ordenes()

def sincronizar_ordenes_binance(simbolos: list[str] | None = None) -> dict[str, Orden]:
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
                ordenes_api.extend(
                    cliente.fetch_open_orders(s.replace("/", ""))
                )
        else:
            ordenes_api = cliente.fetch_open_orders()
    except BaseError as e:
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
    except sqlite3.Error as e:
        log.error(f"❌ Error actualizando la orden en la base de datos: {e}")
        raise

def eliminar_orden(symbol):
    ordenes = cargar_ordenes()
    if symbol in ordenes:
        try:
            with sqlite3.connect(RUTA_DB) as conn:
                conn.execute("DELETE FROM ordenes WHERE symbol = ?", (symbol,))
            del ordenes[symbol]
            _CACHE_ORDENES = ordenes
            log.info(f"🗑️ Orden eliminada para {symbol}.")
        except sqlite3.Error as e:
            log.error(f"❌ Error eliminando orden de la base de datos: {e}")
            raise
    else:
        log.debug(f"Intento de eliminar orden inexistente ignorado: {symbol}")

def registrar_orden(
    symbol: str,
    precio: float,
    cantidad: float,
    sl: float,
    tp: float,
    estrategias,
    tendencia,
    direccion: str = "long",
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
        direccion=direccion,
    )
    actualizar_orden(symbol, orden)

def registrar_operacion(data: dict | Orden) -> None:
    """Agrega una operación al buffer en memoria."""
    global _ULTIMO_FLUSH
    registro = data.to_dict() if isinstance(data, Orden) else data
    with _BUFFER_LOCK:
        _BUFFER_OPERACIONES.append(registro)
    ahora = time.time()
    if (
        len(_BUFFER_OPERACIONES) >= _MAX_BUFFER
        or ahora - _ULTIMO_FLUSH >= _FLUSH_INTERVAL
    ):
        flush_operaciones()
        _ULTIMO_FLUSH = ahora

def ejecutar_orden_market(symbol, cantidad):
    """Ejecuta una compra de mercado y devuelve la cantidad realmente comprada."""
    try:
        cliente = obtener_cliente()
        response = cliente.create_market_buy_order(symbol.replace("/", ""), cantidad)
        ejecutado = float(response.get("amount") or response.get("filled") or 0)
        if ejecutado <= 0:
            ejecutado = cantidad
        log.info(f"🟢 Orden real ejecutada: {symbol}, cantidad: {ejecutado}")
        return ejecutado
    except Exception as e:
        log.error(f"❌ Error ejecutando orden real para {symbol}: {e}")
        raise

def ejecutar_orden_market_sell(symbol: str, cantidad: float) -> float:
    """Ejecuta una venta de mercado validando saldo y límites."""

    if symbol in _VENTAS_FALLIDAS:
        log.warning(
            f"⏭️ Venta omitida para {symbol} por intento previo fallido de saldo."
        )
        return 0.0
    try:
        cliente = obtener_cliente()
        balance = cliente.fetch_balance()
        base = symbol.split("/")[0]
        disponible = esperar_balance(cliente, symbol, cantidad)

        log.debug(
            f"{symbol}: saldo disponible {base} {disponible}, intento vender {cantidad}"
        )

        if disponible <= 0:
            raise InsufficientFunds(f"Saldo 0 disponible para {symbol}")

        # Aplica margen de seguridad
        cantidad_vender = min(cantidad, disponible * 0.999)  # evita vender de más

        if cantidad_vender <= 0:
            raise ValueError(f"Cantidad inválida ({cantidad_vender}) para vender en {symbol}")

        markets = cliente.load_markets()
        info = markets.get(symbol.replace("/", ""), {})
        min_amount = float(info.get("limits", {}).get("amount", {}).get("min") or 0)
        min_cost = float(info.get("limits", {}).get("cost", {}).get("min") or 0)

        ticker = cliente.fetch_ticker(symbol.replace("/", ""))
        precio = float(ticker.get("last") or ticker.get("close") or 0)

        if cantidad_vender < min_amount or (precio and cantidad_vender * precio < min_cost):
            log.error(
                f"Cantidad inválida {cantidad_vender} para {symbol}. "
                f"Mínimos → amount: {min_amount}, notional: {min_cost}"
            )
            _VENTAS_FALLIDAS.add(symbol)
            raise ValueError("cantidad invalida")

        log.info(f"💱 Ejecutando venta real en {symbol}: {cantidad_vender} unidades (precio estimado: {precio:.2f})")

        response = cliente.create_market_sell_order(
            symbol.replace("/", ""), cantidad_vender
        )
        ejecutado = float(response.get("amount") or response.get("filled") or 0)
        if ejecutado <= 0:
            ejecutado = cantidad_vender
        log.info(
            f"🔴 Orden de venta ejecutada: {symbol}, cantidad: {ejecutado}"
        )
        _VENTAS_FALLIDAS.discard(symbol)
        return ejecutado
    except InsufficientFunds as e:
        log.error(f"❌ Venta rechazada por saldo insuficiente en {symbol}: {e}")
        _VENTAS_FALLIDAS.add(symbol)
        raise
    except BaseError as e:
        log.error(f"❌ Error en intercambio al vender {symbol}: {e}")
        raise
    except Exception as e:
        log.error(f"❌ Error estructural al ejecutar venta para {symbol}: {e}")
        raise



def flush_operaciones() -> None:
    """Guarda en disco todas las operaciones acumuladas en el buffer."""
    with _BUFFER_LOCK:
        operaciones = list(_BUFFER_OPERACIONES)
        _BUFFER_OPERACIONES.clear()

    if not operaciones:
        return

    _init_db()
    try:
        with sqlite3.connect(RUTA_DB) as conn:
            for op in operaciones:
                data = op.copy()
                if isinstance(data.get("estrategias_activas"), dict):
                    data["estrategias_activas"] = json.dumps(data["estrategias_activas"])
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
        log.error(f"❌ Error al guardar operaciones en la base de datos: {e}")

    for op in operaciones:
        data = op.copy()
        symbol = data.get("symbol")
        if isinstance(data.get("estrategias_activas"), dict):
            data["estrategias_activas"] = json.dumps(data["estrategias_activas"])
        if symbol:
            try:
                guardar_orden_real(symbol, data)
            except Exception as e:  # pragma: no cover - logging only
                log.error(f"❌ Error guardando operación en Parquet para {symbol}: {e}")

    global _ULTIMO_FLUSH
    _ULTIMO_FLUSH = time.time()


def _handle_exit(signum, frame) -> None:
    flush_operaciones()


for _sig in (signal.SIGTERM, signal.SIGINT):
    try:
        signal.signal(_sig, _handle_exit)
    except (ValueError, RuntimeError):
        pass

atexit.register(flush_operaciones)