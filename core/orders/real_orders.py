"""Utilidades para registrar y mantener las órdenes reales.

El archivo Parquet se protege de escrituras concurrentes entre procesos
Las órdenes se almacenan en una pequeña base SQLite para facilitar la
persistencia entre reinicios del bot.

⚠️ **Estado global**: este módulo utiliza cachés y buffers globales como
``_CACHE_ORDENES`` o ``_BUFFER_OPERACIONES``. Está pensado para una sola
instancia del bot por proceso. Si se desean ejecutar múltiples bots o tests
concurrentes en el mismo proceso, se recomienda encapsular este estado en una
clase o crear instancias independientes.

Variables de entorno relevantes:
- ``MAX_BUFFER_OPERATIONS`` controla cuántas operaciones pueden acumularse en
  memoria antes de forzar un ``flush`` (por defecto ``10``).
- ``FLUSH_INTERVAL`` define el tiempo máximo en segundos entre ``flush``
  automáticos (por defecto ``300``).
- ``USE_PROCESS_POOL`` si se establece en ``1``/``true``/``yes`` permite
  persistir operaciones en un ``ProcessPoolExecutor`` evitando el GIL.
  ``FLUSH_BATCH_SIZE`` determina cuántas operaciones se persisten por lote
  (por defecto ``100``). Si un ``flush`` supera los 30s, el tamaño del lote se
  reduce automáticamente para disminuir la carga de cada escritura.
"""
import os
import json
import sqlite3
import time
import atexit
import threading
import asyncio
from typing import Any, Mapping
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from binance_api.cliente import obtener_cliente
from .order_model import Order, normalizar_precio_cantidad
from core.utils.utils import configurar_logger
from core.supervisor import tick
from . import real_orders
from core.utils.utils import guardar_orden_real
from core.notificador import crear_notificador_desde_env
from core.adaptador_dinamico import calcular_tp_sl_adaptativos
from config.exit_defaults import load_exit_config
import pandas as pd
import math
try:
    from ccxt.base.errors import InsufficientFunds
except ImportError:


    class InsufficientFunds(Exception):
        pass
log = configurar_logger('ordenes')
notificador = crear_notificador_desde_env()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORDENES_DB_PATH = os.getenv(
    'ORDENES_DB_PATH',
    os.path.join(BASE_DIR, 'ordenes_reales', 'ordenes.db'),
)
RUTA_DB = ORDENES_DB_PATH
_CACHE_ORDENES: dict[str, Order] | None = None
_VENTAS_FALLIDAS: set[str] = set()
_BUFFER_OPERACIONES: list[dict] = []
_BUFFER_LOCK = threading.Lock()
_MAX_BUFFER = int(os.getenv('MAX_BUFFER_OPERATIONS', '10') or 10)
_FLUSH_INTERVAL = int(os.getenv('FLUSH_INTERVAL', '300') or 300)
_ULTIMO_FLUSH = time.time()
_SLOW_FLUSHES = 0
_SLOW_FLUSH_THRESHOLD = 30
_SLOW_FLUSH_LIMIT = 3
_USE_PROCESS_POOL = os.getenv('USE_PROCESS_POOL', '0').lower() in {'1', 'true', 'yes'}
_FLUSH_FUTURE: asyncio.Future | None = None
_FLUSH_BATCH_SIZE = int(os.getenv('FLUSH_BATCH_SIZE', '100') or 100)
"""Número máximo de operaciones a persistir por lote."""
_METRICAS_OPERACION: dict[str, dict[str, float]] = {}

_MAX_SLIPPAGE_PCT = float(os.getenv('MAX_SLIPPAGE_PCT', '0.05') or 0.05)
_LIMIT_TIMEOUT = float(os.getenv('LIMIT_ORDER_TIMEOUT', '10') or 10)
_OFFSET_REPRICE = float(os.getenv('OFFSET_REPRICE', '0.001') or 0.001)
_LIMIT_MAX_RETRY = int(os.getenv('LIMIT_ORDER_MAX_RETRY', '3') or 3)



def _connect_db() -> sqlite3.Connection:
    """Abre la conexión SQLite aplicando configuraciones de rendimiento."""
    conn = sqlite3.connect(RUTA_DB)
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
    except sqlite3.Error as e:
        log.warning(f'⚠️ No se pudieron aplicar PRAGMA en SQLite: {e}')
    return conn


def _validar_datos_orden(data: Mapping[str, Any]) -> dict:
    """Valida campos mínimos requeridos para persistir una orden."""
    d = data.to_dict() if isinstance(data, Order) else dict(data)
    symbol = d.get('symbol')
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("Orden inválida: 'symbol' es obligatorio")
    for campo in ('precio_entrada', 'cantidad'):
        try:
            d[campo] = float(d[campo])
        except (TypeError, ValueError, KeyError):
            raise ValueError(f"Orden inválida: '{campo}' debe ser numérico")
    return d


def esperar_balance(cliente, symbol: str, cantidad_esperada: float,
    max_intentos: int=10, delay: float=0.3) ->float:
    log.info('➡️ Entrando en esperar_balance()')
    """Espera hasta que el balance disponible alcance la cantidad esperada."""
    try:
        base = symbol.split('/')[0]
    except Exception as e:
        log.error(f'❌ Error al interpretar símbolo {symbol}: {e}')
        return 0.0
    for intento in range(max_intentos):
        try:
            balance = cliente.fetch_balance()
            disponible = balance.get('free', {}).get(base, 0.0)
            if disponible >= cantidad_esperada:
                return disponible
        except Exception as e:
            log.warning(
                f'⚠️ Error al obtener balance en intento {intento + 1}/{max_intentos}: {e}'
                )
        time.sleep(delay)
    log.warning(
        f'⏱️ Tiempo de espera agotado para obtener balance suficiente en {symbol}. Disponible: {disponible}, requerido: {cantidad_esperada}'
        )
    return disponible


def _acumular_metricas(operation_id: str, response: dict) -> dict[str, float]:
    """Acumula comisiones y PnL por ``operation_id`` a partir de la respuesta.

    Para cada trade se calcula el PnL con el signo correcto según ``side`` y se
    deduce la comisión de forma proporcional a la base correspondiente
    (notional o cantidad) dependiendo de la moneda en la que se cobra la fee.
    """

    metricas = _METRICAS_OPERACION.setdefault(
        operation_id, {"fee": 0.0, "pnl": 0.0}
    )

    symbol = (
        response.get("symbol")
        or response.get("info", {}).get("symbol")
        or ""
    )
    base, quote = (symbol.split("/") if "/" in symbol else ("", ""))

    trades = response.get("trades") or []
    for trade in trades:
        side = trade.get("side") or ""
        price = float(trade.get("price") or 0.0)
        amount = float(trade.get("amount") or 0.0)
        cost = float(trade.get("cost") or price * amount)

        # Fees
        fee_info = trade.get("fee") or {}
        fee_cost = fee_info.get("cost")
        if fee_cost is None:
            rate = fee_info.get("rate")
            currency = fee_info.get("currency")
            if rate is not None:
                if currency == base:
                    fee_cost = float(rate) * amount
                else:
                    fee_cost = float(rate) * cost
        fee_cost = float(fee_cost or 0.0)
        metricas["fee"] += fee_cost

        # PnL sign: buys are negative cash flow, sells positive
        pnl = cost if side == "sell" else -cost
        pnl -= fee_cost
        metricas["pnl"] += pnl

    # Some responses include aggregated fees outside of ``trades``
    for fee in response.get("fees", []) or []:
        try:
            costo = float(fee.get("cost") or 0.0)
        except Exception:
            continue
        metricas["fee"] += costo
        metricas["pnl"] -= costo
        
    return metricas


def _init_db() ->None:
    log.info('➡️ Entrando en _init_db()')
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
        with _connect_db() as conn:
            conn.execute(
                f'CREATE TABLE IF NOT EXISTS ordenes ({schema_base}, PRIMARY KEY(symbol))'
                )
            conn.execute(
                f'CREATE TABLE IF NOT EXISTS operaciones (id INTEGER PRIMARY KEY AUTOINCREMENT, {schema_base})'
                )
        log.info('🗃️ Tablas de órdenes y operaciones verificadas/creadas.')
    except sqlite3.Error as e:
        log.error(f'❌ Error al crear las tablas en SQLite: {e}')
        raise


def cargar_ordenes() ->dict[str, Order]:
    log.info('➡️ Entrando en cargar_ordenes()')
    """Carga las órdenes almacenadas desde la base de datos SQLite."""
    global _CACHE_ORDENES
    if _CACHE_ORDENES is not None:
        return _CACHE_ORDENES
    _init_db()
    ordenes: dict[str, Order] = {}
    try:
        with _connect_db() as conn:
            conn.row_factory = sqlite3.Row
            filas = conn.execute(
                "SELECT * FROM ordenes WHERE fecha_cierre IS NULL OR fecha_cierre = ''"
            ).fetchall()
            for row in filas:
                data = dict(row)
                orden = Order.from_dict(data)
                ordenes[orden.symbol] = orden
        log.info(
            f'📥 {len(ordenes)} órdenes abiertas cargadas desde la base de datos.'
        )
    except sqlite3.Error as e:
        log.error(f'❌ Error al cargar órdenes desde SQLite: {e}')
        return {}
    _CACHE_ORDENES = ordenes
    return _CACHE_ORDENES


def guardar_ordenes(ordenes: dict[str, Order]) ->None:
    log.info('➡️ Entrando en guardar_ordenes()')
    """Guarda las órdenes en la base de datos si han cambiado respecto al caché."""
    global _CACHE_ORDENES

    def ordenar_dict(d):
        log.info('➡️ Entrando en ordenar_dict()')
        return json.dumps({k: o.to_dict() for k, o in d.items()}, sort_keys
            =True)
    if _CACHE_ORDENES and ordenar_dict(_CACHE_ORDENES) == ordenar_dict(ordenes
        ):
        return
    _init_db()
    try:
        with _connect_db() as conn:
            with conn:
                for orden in ordenes.values():
                    data = _validar_datos_orden(orden)
                    if isinstance(data.get('estrategias_activas'), dict):
                        data['estrategias_activas'] = json.dumps(
                            data['estrategias_activas']
                        )
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
                            data.get('symbol'),
                            data.get('precio_entrada'),
                            data.get('cantidad'),
                            data.get('stop_loss'),
                            data.get('take_profit'),
                            data.get('timestamp'),
                            data.get('estrategias_activas'),
                            data.get('tendencia'),
                            data.get('max_price'),
                            data.get('direccion'),
                            data.get('precio_cierre'),
                            data.get('fecha_cierre'),
                            data.get('motivo_cierre'),
                            data.get('retorno_total'),
                        ),
                    )
        _CACHE_ORDENES = ordenes
        log.info(
            f'💾 {len(ordenes)} órdenes guardadas correctamente en la base de datos.'
        )
    except (sqlite3.Error, ValueError) as e:
        log.error(f'❌ Error al guardar órdenes en SQLite: {e}')
        raise


def obtener_orden(symbol: str) ->(Order | None):
    log.info('➡️ Entrando en obtener_orden()')
    try:
        return cargar_ordenes().get(symbol)
    except Exception as e:
        log.error(f'❌ Error al obtener orden de {symbol}: {e}')
        return None


def obtener_todas_las_ordenes():
    log.info('➡️ Entrando en obtener_todas_las_ordenes()')
    return cargar_ordenes()


def reconciliar_ordenes(simbolos: list[str] | None = None) -> dict[str, Order]:
    log.info('➡️ Entrando en reconciliar_ordenes()')
    local = cargar_ordenes()
    try:
        cliente = obtener_cliente()
        markets = cliente.load_markets()
        ordenes_api: list[dict] = []
        if simbolos:
            for s in simbolos:
                ordenes_api.extend(cliente.fetch_open_orders(s))
        else:
            ordenes_api = cliente.fetch_open_orders()
    except Exception as e:
        log.error(f'❌ Error consultando órdenes abiertas: {e}')
        return local
    exchange: dict[str, dict] = {}
    for o in ordenes_api:
        symbol = o.get('symbol')
        if not symbol:
            continue
        market = markets.get(symbol, {})
        limits = market.get('limits', {}) if market else {}
        min_amount = (limits.get('amount') or {}).get('min') or 0.0
        min_cost = (limits.get('cost') or {}).get('min') or 0.0
        price = float(o.get('price') or o.get('average') or 0.0)
        amount = float(o.get('amount') or o.get('remaining') or 0.0)
        if amount < min_amount or price * amount < min_cost:
            continue
        exchange[symbol] = {
            'price': price,
            'amount': amount,
            'side': o.get('side', 'buy').lower(),
        }
    local_symbols = set(local.keys())
    exchange_symbols = set(exchange.keys())
    local_only = sorted(local_symbols - exchange_symbols)
    exchange_only = sorted(exchange_symbols - local_symbols)
    both = sorted(local_symbols & exchange_symbols)
    log.info(f'local_only: {local_only}')
    log.info(f'exchange_only: {exchange_only}')
    log.info(f'both: {both}')
    for sym in local_only:
        ord_ = local.get(sym)
        if not ord_:
            continue
        ord_.fecha_cierre = datetime.utcnow().isoformat()
        ord_.motivo_cierre = 'closed_by_reconciliation'
        try:
            registrar_operacion(ord_)
        except Exception as e:
            log.warning(f'⚠️ No se pudo registrar cierre para {sym}: {e}')
        try:
            eliminar_orden(sym)
        except Exception as e:
            log.warning(f'⚠️ No se pudo eliminar orden local {sym}: {e}')
        local.pop(sym, None)
    for sym in exchange_only:
        info = exchange[sym]
        side = info['side']
        direccion = 'long' if side == 'buy' else 'short'
        sl = tp = 0.0
        try:
            ohlcv = cliente.fetch_ohlcv(sym, timeframe='1h', limit=120)
            if ohlcv:
                df = pd.DataFrame(
                    ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume']
                )
                cfg = load_exit_config(sym)
                sl_calc, tp_calc = calcular_tp_sl_adaptativos(sym, df, cfg)
                if direccion == 'long':
                    sl, tp = sl_calc, tp_calc
                else:
                    sl, tp = tp_calc, sl_calc
        except Exception as e:
            log.warning(f'⚠️ Error calculando SL/TP para {sym}: {e}')
        try:
            registrar_orden(sym, info['price'], info['amount'], sl, tp, {}, '', direccion)
        except Exception as e:
            log.warning(f'⚠️ No se pudo registrar orden reconciliada para {sym}: {e}')
    return cargar_ordenes()


def sincronizar_ordenes_binance(simbolos: (list[str] | None)=None) ->dict[
    str, Order]:
    log.info('➡️ Entrando en sincronizar_ordenes_binance()')
    """Consulta órdenes abiertas directamente desde Binance y las registra.

    Esto permite reconstruir el estado de las posiciones cuando el bot se
    reinicia y la base de datos local no contiene todas las operaciones
    abiertas. Devuelve el diccionario de órdenes resultante.
    """
    try:
        cliente = obtener_cliente()
        markets = cliente.load_markets()
        ordenes_api = []
        if simbolos:
            for s in simbolos:
                ordenes_api.extend(cliente.fetch_open_orders(s))
        else:
            ordenes_api = cliente.fetch_open_orders()
    except Exception as e:
        log.error(f'❌ Error consultando órdenes abiertas: {e}')
        return cargar_ordenes()
    for o in ordenes_api:
        symbol = o.get('symbol')
        if not symbol:
            continue
        market = markets.get(symbol, {})
        limits = market.get('limits', {}) if market else {}
        min_amount = (limits.get('amount') or {}).get('min') or 0.0
        min_cost = (limits.get('cost') or {}).get('min') or 0.0
        price = float(o.get('price') or o.get('average') or 0)
        amount = float(o.get('amount') or o.get('remaining') or 0)
        if amount < min_amount or price * amount < min_cost:
            continue
        side = o.get('side', 'buy').lower()
        direccion = 'long' if side == 'buy' else 'short'
        sl = 0.0
        tp = 0.0
        try:
            ohlcv = cliente.fetch_ohlcv(symbol, timeframe='1h', limit=120)
            if ohlcv:
                df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
                cfg = load_exit_config(symbol)
                sl_calc, tp_calc = calcular_tp_sl_adaptativos(symbol, df, cfg)
                if direccion == 'long':
                    sl, tp = sl_calc, tp_calc
                else:
                    sl, tp = tp_calc, sl_calc
        except Exception as e:
            log.warning(f'⚠️ Error calculando SL/TP para {symbol}: {e}')
        registrar_orden(symbol, price, amount, sl, tp, {}, '', direccion)
    return cargar_ordenes()


def reconciliar_trades_binance(simbolos: list[str] | None = None, limit: int = 50) -> None:
    log.info('➡️ Entrando en reconciliar_trades_binance()')
    try:
        cliente = obtener_cliente()
        markets = cliente.load_markets()
        if simbolos is None:
            simbolos = list(markets.keys())
        for s in simbolos:
            market = markets.get(s, {})
            limits = market.get('limits', {}) if market else {}
            min_amount = (limits.get('amount') or {}).get('min') or 0.0
            min_cost = (limits.get('cost') or {}).get('min') or 0.0
            try:
                trades = cliente.fetch_my_trades(s.replace('/', ''), limit=limit)
            except Exception:
                continue
            for t in trades:
                try:
                    price = float(t.get('price') or 0)
                    amount = float(t.get('amount') or 0)
                except (TypeError, ValueError):
                    continue
                cost = price * amount
                if amount < min_amount or cost < min_cost:
                    continue
                side = t.get('side', 'buy').lower()
                sl = tp = 0.0
                if side == 'buy':
                    try:
                        ohlcv = cliente.fetch_ohlcv(s.replace('/', ''), timeframe='1h', limit=100)
                        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                        cfg = load_exit_config(s)
                        sl, tp = calcular_tp_sl_adaptativos(s, df, cfg, precio_actual=price)
                    except Exception as e:
                        log.debug(f'No se pudieron calcular SL/TP para {s}: {e}')
                data = {
                    'symbol': s,
                    'precio_entrada': price,
                    'cantidad': amount,
                    'timestamp': datetime.utcfromtimestamp(t.get('timestamp', 0) / 1000).isoformat(),
                    'stop_loss': sl,
                    'take_profit': tp,
                    'estrategias_activas': {},
                    'tendencia': '',
                    'max_price': price,
                    'direccion': 'long' if side == 'buy' else 'short',
                }
                guardar_orden_real(s, data)
                if side == 'buy' and amount > 0 and not obtener_orden(s):
                    try:
                        registrar_orden(s, price, amount, 0.0, 0.0, {}, '', 'long')
                    except Exception as e:
                        log.warning(f'⚠️ No se pudo registrar orden reconciliada para {s}: {e}')
    except Exception as e:
        log.error(f'❌ Error al reconciliar trades: {e}')


def actualizar_orden(symbol: str, data: (Order | dict)) ->None:
    log.info('➡️ Entrando en actualizar_orden()')
    ordenes = cargar_ordenes()
    if ordenes.get(symbol) == data:
        return
    try:
        d = _validar_datos_orden(data)
        if isinstance(d.get('estrategias_activas'), dict):
            d['estrategias_activas'] = json.dumps(d['estrategias_activas'])
        _init_db()
        with _connect_db() as conn:
            with conn:
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
                        d.get('symbol'),
                        d.get('precio_entrada'),
                        d.get('cantidad'),
                        d.get('stop_loss'),
                        d.get('take_profit'),
                        d.get('timestamp'),
                        d.get('estrategias_activas'),
                        d.get('tendencia'),
                        d.get('max_price'),
                        d.get('direccion'),
                        d.get('precio_cierre'),
                        d.get('fecha_cierre'),
                        d.get('motivo_cierre'),
                        d.get('retorno_total'),
                    ),
                )
        ordenes[symbol] = data if isinstance(data, Order) else Order.from_dict(d)
        _CACHE_ORDENES = ordenes
        log.info(f'📌 Order actualizada para {symbol}.')
    except Exception as e:
        log.error(f'❌ Error al actualizar orden para {symbol}: {e}')
        raise


def eliminar_orden(symbol: str, forzar_log: bool=False) ->None:
    log.info('➡️ Entrando en eliminar_orden()')
    """Elimina una orden activa del sistema si existe."""
    ordenes = cargar_ordenes()
    if symbol not in ordenes:
        if forzar_log:
            log.warning(f'⚠️ Intento de eliminar orden inexistente: {symbol}')
        else:
            log.debug(
                f'Intento de eliminar orden inexistente ignorado: {symbol}')
        return
    try:
        with _connect_db() as conn:
            conn.execute('DELETE FROM ordenes WHERE symbol = ?', (symbol,))
        del ordenes[symbol]
        _CACHE_ORDENES = ordenes
        log.info(f'🗑️ Order eliminada correctamente para {symbol}.')
    except sqlite3.Error as e:
        log.error(f'❌ Error eliminando orden de la base de datos: {e}')
        raise


def registrar_orden(symbol: str, precio: float, cantidad: float, sl: float,
    tp: float, estrategias, tendencia: str, direccion: str='long') ->None:
    log.info('➡️ Entrando en registrar_orden()')
    """Registra una nueva orden activa y la guarda en base de datos."""
    if not isinstance(symbol, str) or not symbol:
        raise ValueError('❌ El símbolo debe ser una cadena no vacía.')
    if precio <= 0 or cantidad <= 0:
        raise ValueError(
            f'❌ Precio o cantidad inválidos para {symbol}: precio={precio}, cantidad={cantidad}'
            )
    if not isinstance(estrategias, dict):
        log.warning(
            f'⚠️ Estrategias activas no en formato dict para {symbol}, se forzará conversión...'
            )
        estrategias = dict(estrategias) if estrategias else {}
    orden = Order(
        symbol=symbol,
        precio_entrada=precio,
        cantidad=cantidad,
        cantidad_abierta=cantidad,
        stop_loss=sl,
        take_profit=tp,
        timestamp=datetime.utcnow().isoformat(),
        estrategias_activas=estrategias,
        tendencia=tendencia,
        max_price=precio,
        direccion=direccion,
    )
    actualizar_orden(symbol, orden)
    log.info(
        f'✅ Order registrada para {symbol} — cantidad: {cantidad}, entrada: {precio}'
        )


def registrar_operacion(data: (dict | Order)) ->None:
    log.info('➡️ Entrando en registrar_operacion()')
    """Agrega una operación ejecutada al buffer. Se persistirá automáticamente."""
    global _ULTIMO_FLUSH
    registro = data.to_dict() if isinstance(data, Order) else data
    symbol = registro.get('symbol')
    if not symbol:
        log.warning(
            '⚠️ Registro sin símbolo recibido en registrar_operacion(), ignorado.'
            )
        return
    with _BUFFER_LOCK:
        _BUFFER_OPERACIONES.append(registro)
        log.debug(f'📥 Operación registrada en buffer para {symbol}')
    ahora = time.time()
    if len(_BUFFER_OPERACIONES
        ) >= _MAX_BUFFER or ahora - _ULTIMO_FLUSH >= _FLUSH_INTERVAL:
        log.debug(
            '🔁 Buffer de operaciones lleno o expirado, iniciando flush...')
        flush_operaciones()
        _ULTIMO_FLUSH = ahora


def ejecutar_orden_market(symbol: str, cantidad: float, operation_id: str | None = None) -> dict:
    log.info('➡️ Entrando en ejecutar_orden_market()')
    """Ejecuta una compra de mercado y devuelve detalles de la ejecución."""
    if cantidad <= 0:
        log.warning(f'⚠️ Cantidad inválida para compra en {symbol}: {cantidad}')
        return {'ejecutado': 0.0, 'restante': 0.0, 'status': 'FILLED', 'min_qty': 0.0, 'fee': 0.0, 'pnl': 0.0}
    try:
        cliente = obtener_cliente()
        markets = cliente.load_markets()
        market_info = markets.get(symbol.replace('/', ''), {})
        precision = market_info.get('precision', {}).get('amount', 8)
        step_size = 10 ** -precision
        min_cost = float(market_info.get('limits', {}).get('cost', {}).get(
            'min') or 0)
        ticker = cliente.fetch_ticker(symbol.replace('/', ''))
        precio = float(ticker.get('last') or ticker.get('close') or 0)
        precio, cantidad = normalizar_precio_cantidad(market_info, precio, cantidad, 'compra')
        if cantidad <= 0:
            log.error(f'⛔ Cantidad ajustada inválida para {symbol}: {cantidad}')
            return {'ejecutado': 0.0, 'restante': 0.0, 'status': 'FILLED', 'min_qty': 0.0, 'fee': 0.0, 'pnl': 0.0}
        min_amount = float(market_info.get('limits', {}).get('amount', {}).get('min') or 0)
        min_cost = float(market_info.get('limits', {}).get('cost', {}).get('min') or 0)
        quote = symbol.split('/')[1]
        balance = cliente.fetch_balance()
        disponible_quote = balance.get('free', {}).get(quote, 0)
        if precio:
            costo = cantidad * precio
            if costo > disponible_quote:
                cantidad_ajustada = math.floor((disponible_quote / precio) / step_size) * step_size
                cantidad_ajustada = normalizar_precio_cantidad(market_info, precio, cantidad_ajustada, 'compra')[1]
                if cantidad_ajustada <= 0:
                    log.error(
                        f'⛔ Compra cancelada por saldo insuficiente en {symbol}. Requerido: {costo:.2f} {quote}, disponible: {disponible_quote:.2f}'
                    )
                    try:
                        notificador.enviar(
                            f'Compra cancelada por saldo insuficiente en {symbol}',
                            'CRITICAL',
                        )
                    except Exception:
                        pass
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
                notificador.enviar(
                    f'Compra inválida para {symbol}', 'WARNING'
                )
            except Exception:
                pass
            return {'ejecutado': 0.0, 'restante': 0.0, 'status': 'FILLED', 'min_qty': min_amount, 'fee': 0.0, 'pnl': 0.0}
        log.debug(
            f'📤 Enviando orden de compra para {symbol} | Cantidad: {cantidad} | Precio estimado: {precio:.4f}'
            )
        params = {}
        if operation_id:
            params['newClientOrderId'] = operation_id
        response = cliente.create_market_buy_order(symbol.replace('/', ''), cantidad, params)
        ejecutado = float(response.get('amount') or response.get('filled') or 0)
        if ejecutado <= 0:
            ejecutado = cantidad
        restante = max(cantidad - ejecutado, 0.0)
        status = 'FILLED'
        if restante > 1e-8:
            status = 'PARTIAL'
        metricas = {'fee': 0.0, 'pnl': 0.0}
        if operation_id:
            metricas = _acumular_metricas(operation_id, response)
        precio_fill = float(response.get('price') or response.get('average') or precio)
        slippage = abs(precio_fill - precio) / precio if precio else 0.0
        if slippage > _MAX_SLIPPAGE_PCT:
            log.warning(
                f'⚠️ Slippage alto en {symbol}: {slippage:.2%} (máx {_MAX_SLIPPAGE_PCT:.2%})'
            )
            try:
                notificador.enviar(
                    f'Slippage alto en {symbol}: {slippage:.2%}', 'WARNING'
                )
            except Exception:
                pass
        log.info(f'🟢 Order real ejecutada: {symbol}, cantidad: {ejecutado}')
        return {
            'ejecutado': ejecutado,
            'restante': restante,
            'status': status,
            'min_qty': min_amount,
            'fee': metricas['fee'],
            'pnl': metricas['pnl'],
            'slippage': slippage,
        }
    except Exception as e:
        log.error(f'❌ Error en Binance al ejecutar compra en {symbol}: {e}')

        try:
            trades = cliente.fetch_my_trades(symbol.replace('/', ''), limit=1)
            trade = trades[-1] if trades else None
            ejecutado = float(trade.get('amount') or trade.get('qty') or 0) if trade else 0
            if ejecutado > 0:
                log.warning(
                    f'⚠️ Operación detectada tras error: {ejecutado} {symbol}'
                )
                try:
                    notificador.enviar(
                        f'Orden ejecutada tras error en {symbol}', 'WARNING'
                    )
                except Exception:
                    pass
                try:
                    guardar_orden_real(symbol, {
                        'symbol': symbol,
                        'precio_entrada': float(trade.get('price') or 0) if trade else 0,
                        'cantidad': ejecutado,
                        'timestamp': datetime.utcnow().isoformat(),
                        'stop_loss': 0.0,
                        'take_profit': 0.0,
                        'estrategias_activas': {},
                        'tendencia': '',
                        'max_price': float(trade.get('price') or 0) if trade else 0,
                        'direccion': 'long',
                    })
                    registrar_orden(
                        symbol,
                        float(trade.get('price') or 0) if trade else 0,
                        ejecutado,
                        0.0,
                        0.0,
                        {},
                        '',
                        'long',
                    )
                except Exception as e_reg:
                    log.error(f'❌ No se pudo registrar orden tras error: {e_reg}')
        except Exception as ver_err:
            log.error(f'❌ Error verificando trades tras fallo: {ver_err}')
        raise


def ejecutar_orden_market_sell(symbol: str, cantidad: float, operation_id: str | None = None) -> dict:
    log.info('➡️ Entrando en ejecutar_orden_market_sell()')
    """Ejecuta una venta de mercado validando saldo y devuelve detalles."""
    if symbol in _VENTAS_FALLIDAS:
        log.warning(
            f'⏭️ Venta omitida para {symbol} por intento previo fallido de saldo.'
            )
        return {'ejecutado': 0.0, 'restante': cantidad, 'status': 'FILLED', 'min_qty': 0.0, 'fee': 0.0, 'pnl': 0.0}
    try:
        cliente = obtener_cliente()
        balance = cliente.fetch_balance()
        base = symbol.split('/')[0]
        disponible = balance.get('free', {}).get(base, 0)
        if disponible <= 0:
            raise InsufficientFunds(f'Saldo 0 disponible para {symbol}')
        markets = cliente.load_markets()
        info = markets.get(symbol.replace('/', ''), {})
        precision = info.get('precision', {}).get('amount', 8)
        step_size = 10 ** -precision
        cantidad_vender = math.floor(disponible / step_size) * step_size
        cantidad_vender = min(cantidad, cantidad_vender)
        if cantidad_vender <= 0:
            raise ValueError(
                f'Cantidad inválida ({cantidad_vender}) para vender en {symbol}'
                )
        min_amount = float(info.get('limits', {}).get('amount', {}).get('min') or 0)
        min_cost = float(info.get('limits', {}).get('cost', {}).get('min') or 0)
        ticker = cliente.fetch_ticker(symbol.replace('/', ''))
        precio = float(ticker.get('last') or ticker.get('close') or 0)
        precio, cantidad_vender = normalizar_precio_cantidad(info, precio, cantidad_vender, 'venta')
        if (cantidad_vender < min_amount or precio and cantidad_vender * precio < min_cost):
            log.error(
                f'⛔ Venta rechazada por mínimos: {symbol} → cantidad: {cantidad_vender:.8f}, mínimos: amount={min_amount}, notional={min_cost}'
                )
            _VENTAS_FALLIDAS.add(symbol)
            try:
                notificador.enviar(
                    f'Venta rechazada por mínimos en {symbol}', 'WARNING'
                )
            except Exception:
                pass
            return {'ejecutado': 0.0, 'restante': cantidad_vender, 'status': 'FILLED', 'min_qty': min_amount, 'fee': 0.0, 'pnl': 0.0}
        log.info(
            f'💱 Ejecutando venta real en {symbol}: {cantidad_vender:.8f} unidades (precio estimado: {precio:.2f})'
            )
        params = {}
        if operation_id:
            params['newClientOrderId'] = operation_id
        response = cliente.create_market_sell_order(symbol.replace('/', ''), cantidad_vender, params)
        ejecutado = float(response.get('amount') or response.get('filled') or 0)
        if ejecutado <= 0:
            ejecutado = cantidad_vender
        restante = max(cantidad_vender - ejecutado, 0.0)
        status = 'FILLED'
        if restante > 1e-8:
            status = 'PARTIAL'
        metricas = {'fee': 0.0, 'pnl': 0.0}
        if operation_id:
            metricas = _acumular_metricas(operation_id, response)
        precio_fill = float(response.get('price') or response.get('average') or precio)
        slippage = abs(precio_fill - precio) / precio if precio else 0.0
        if slippage > _MAX_SLIPPAGE_PCT:
            log.warning(
                f'⚠️ Slippage alto en {symbol}: {slippage:.2%} (máx {_MAX_SLIPPAGE_PCT:.2%})'
            )
            try:
                notificador.enviar(
                    f'Slippage alto en {symbol}: {slippage:.2%}', 'WARNING'
                )
            except Exception:
                pass
        log.info(
            f'🔴 Order de venta ejecutada: {symbol}, cantidad: {ejecutado:.8f}')
        _VENTAS_FALLIDAS.discard(symbol)
        return {
            'ejecutado': ejecutado,
            'restante': restante,
            'status': status,
            'min_qty': min_amount,
            'fee': metricas['fee'],
            'pnl': metricas['pnl'],
            'slippage': slippage,
        }
    except InsufficientFunds as e:
        log.error(f'❌ Venta rechazada por saldo insuficiente en {symbol}: {e}')
        _VENTAS_FALLIDAS.add(symbol)
        try:
            notificador.enviar(
                f'Venta rechazada por saldo insuficiente en {symbol}',
                'CRITICAL',
            )
        except Exception:
            pass
        return {'ejecutado': 0.0, 'restante': cantidad, 'status': 'FILLED', 'min_qty': 0.0, 'fee': 0.0, 'pnl': 0.0}
    except Exception as e:
        log.error(f'❌ Error en intercambio al vender {symbol}: {e}')
    raise


def _market_sell_retry(symbol: str, cantidad: float, operation_id: str | None = None) -> dict:
    log.info('➡️ Entrando en _market_sell_retry()')
    """Envía ventas de mercado reintentando en caso de fills parciales."""
    restante = cantidad
    total = total_fee = total_pnl = 0.0
    min_qty = 0.0
    while restante > 0:
        resp = ejecutar_orden_market_sell(symbol, restante, operation_id)
        ejecutado = float(resp.get('ejecutado', 0.0))
        restante = float(resp.get('restante', 0.0))
        min_qty = float(resp.get('min_qty', 0.0))
        total += ejecutado
        total_fee += float(resp.get('fee', 0.0))
        total_pnl += float(resp.get('pnl', 0.0))
        if resp.get('status') != 'PARTIAL' or restante < min_qty:
            break
    estado = 'FILLED' if restante < 1e-8 else 'PARTIAL'
    return {
        'ejecutado': total,
        'restante': restante,
        'status': estado,
        'min_qty': min_qty,
        'fee': total_fee,
        'pnl': total_pnl,
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
    """Ejecuta una orden ``limit`` con re-precio y *fallback* a market.

    Si la orden no se completa dentro de ``timeout`` segundos se cancela y se
    re-precia usando ``offset_reprice``. Tras ``max_reintentos`` intentos
    fallidos, la función envía una orden de mercado.
    """
    if side not in {'buy', 'sell'}:
        raise ValueError('side debe ser "buy" o "sell"')
    timeout = timeout or _LIMIT_TIMEOUT
    offset_reprice = offset_reprice or _OFFSET_REPRICE
    max_reintentos = max_reintentos or _LIMIT_MAX_RETRY
    cliente = obtener_cliente()
    markets = cliente.load_markets()
    info = markets.get(symbol.replace('/', ''), {})
    params_base = {}
    if operation_id:
        params_base['newClientOrderId'] = operation_id

    for intento in range(1, max_reintentos + 1):
        precio, cantidad = normalizar_precio_cantidad(
            info, precio, cantidad, 'compra' if side == 'buy' else 'venta'
        )
        params = params_base.copy()
        if operation_id:
            params['newClientOrderId'] = f"{operation_id}-{intento}"
        if side == 'buy':
            orden = cliente.create_limit_buy_order(
                symbol.replace('/', ''), cantidad, precio, params
            )
        else:
            orden = cliente.create_limit_sell_order(
                symbol.replace('/', ''), cantidad, precio, params
            )
        order_id = orden.get('id')
        inicio = time.time()
        estado = orden
        while time.time() - inicio < timeout:
            estado = cliente.fetch_order(order_id, symbol.replace('/', ''))
            filled = float(estado.get('filled') or 0)
            if estado.get('status') in {'closed', 'canceled'} or filled >= cantidad:
                break
            time.sleep(1)
        else:
            try:
                cliente.cancel_order(order_id, symbol.replace('/', ''))
            except Exception as e:
                log.warning(f'⚠️ No se pudo cancelar orden {order_id}: {e}')
            if intento >= max_reintentos:
                break
            if side == 'buy':
                precio *= 1 + offset_reprice
            else:
                precio *= 1 - offset_reprice
            continue

        ejecutado = float(estado.get('filled') or 0)
        restante = max(cantidad - ejecutado, 0.0)
        status = 'FILLED'
        if restante > 1e-8:
            status = 'PARTIAL'
        precio_fill = float(estado.get('price') or estado.get('average') or precio)
        slippage = abs(precio_fill - precio) / precio if precio else 0.0
        if slippage > _MAX_SLIPPAGE_PCT:
            log.warning(
                f'⚠️ Slippage alto en {symbol}: {slippage:.2%} (máx {_MAX_SLIPPAGE_PCT:.2%})'
            )
            try:
                notificador.enviar(
                    f'Slippage alto en {symbol}: {slippage:.2%}', 'WARNING'
                )
            except Exception:
                pass
        metricas = {'fee': 0.0, 'pnl': 0.0}
        if operation_id:
            metricas = _acumular_metricas(operation_id, estado)
        return {
            'ejecutado': ejecutado,
            'restante': restante,
            'status': status,
            'min_qty': float(info.get('limits', {}).get('amount', {}).get('min') or 0),
            'fee': metricas['fee'],
            'pnl': metricas['pnl'],
            'slippage': slippage,
        }

    log.warning(f'⚠️ Fallback a orden de mercado en {symbol} tras {max_reintentos} intentos limit fallidos')
    if side == 'buy':
        return ejecutar_orden_market(symbol, cantidad, operation_id)
    return ejecutar_orden_market_sell(symbol, cantidad, operation_id)


def _persistir_operaciones(operaciones: list[dict]) -> tuple[int, int, list[dict]]:
    """Persiste operaciones en SQLite y Parquet devolviendo conteos de errores y pendientes."""
    _init_db()
    errores_sqlite = 0
    errores_parquet = 0
    pendientes: list[dict[str, Any]] = []
    try:
        with _connect_db() as conn:
            for op in operaciones:
                data = op.copy()
                if isinstance(data.get('estrategias_activas'), dict):
                    data['estrategias_activas'] = json.dumps(data['estrategias_activas'])
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
                            data.get('symbol'),
                            data.get('precio_entrada'),
                            data.get('cantidad'),
                            data.get('stop_loss'),
                            data.get('take_profit'),
                            data.get('timestamp'),
                            data.get('estrategias_activas'),
                            data.get('tendencia'),
                            data.get('max_price'),
                            data.get('direccion'),
                            data.get('precio_cierre'),
                            data.get('fecha_cierre'),
                            data.get('motivo_cierre'),
                            data.get('retorno_total'),
                        ),
                    )
                except sqlite3.Error as e:
                    log.error(
                        f"❌ Error SQLite al insertar operación para {data.get('symbol')}: {e}"
                    )
                    errores_sqlite += 1
                    pendientes.append(op)
    except sqlite3.Error as e:
        log.error(f'❌ Error global al guardar operaciones en SQLite: {e}')
        errores_sqlite += 1
        pendientes.extend(operaciones)
    for op in operaciones:
        data = op.copy()
        symbol = data.get('symbol')
        if isinstance(data.get('estrategias_activas'), dict):
            data['estrategias_activas'] = json.dumps(data['estrategias_activas'])
        if symbol:
            try:
                guardar_orden_real(symbol, data)
            except Exception as e:
                log.error(
                    f'❌ Error guardando operación en Parquet para {symbol}: {e}'
                )
                errores_parquet += 1
                if op not in pendientes:
                    pendientes.append(op)
    return errores_sqlite, errores_parquet, pendientes


def _chunked(seq: list, size: int):
    """Divide ``seq`` en lotes del ``size`` indicado."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def flush_operaciones() ->None:
    global _SLOW_FLUSHES, _USE_PROCESS_POOL, _ULTIMO_FLUSH, _FLUSH_BATCH_SIZE
    log.info('➡️ Entrando en flush_operaciones()')
    """Guarda en disco todas las operaciones acumuladas.

    Las operaciones se procesan en lotes para minimizar el impacto de las
    iteraciones largas sobre SQLite y las escrituras en Parquet manejadas por
    :mod:`pandas`.
    """
    with _BUFFER_LOCK:
        operaciones = list(_BUFFER_OPERACIONES)
        _BUFFER_OPERACIONES.clear()
    total_ops = len(operaciones)
    if not total_ops:
        return
    log.info(f'📝 Iniciando flush_operaciones con {total_ops} operaciones.')
    inicio = time.time()
    errores_sqlite = errores_parquet = 0
    pendientes: list[dict] = []
    batches = list(_chunked(operaciones, _FLUSH_BATCH_SIZE))
    if _USE_PROCESS_POOL:
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(_persistir_operaciones, b) for b in batches]
            for f in as_completed(futures):
                e_sql, e_parq, pend = f.result()
                errores_sqlite += e_sql
                errores_parquet += e_parq
                pendientes.extend(pend)
    else:
        for b in batches:
            e_sql, e_parq, pend = _persistir_operaciones(b)
            errores_sqlite += e_sql
            errores_parquet += e_parq
            pendientes.extend(pend)
    duracion = time.time() - inicio
    mensaje_timeout = int(os.getenv('MENSAJE_TIMEOUT', '0') or 0)
    inactivity_intervals = int(os.getenv('INACTIVITY_INTERVALS', '0') or 0)
    log.info(f'🏁 flush_operaciones finalizado en {duracion:.2f}s para {total_ops} operaciones.')
    if mensaje_timeout and duracion > mensaje_timeout:
        log.warning(
            f'⏱️ Duración {duracion:.2f}s supera mensaje_timeout {mensaje_timeout}s'
        )
    if inactivity_intervals and duracion > inactivity_intervals:
        log.warning(
            f'⏱️ Duración {duracion:.2f}s supera inactivity_intervals {inactivity_intervals}s'
        )
    if duracion > _SLOW_FLUSH_THRESHOLD:
        _SLOW_FLUSHES += 1
        log.warning(
            f'⚠️ flush_operaciones tardó {duracion:.2f}s; considerar reducir '
            'FLUSH_BATCH_SIZE o activar USE_PROCESS_POOL'
        )
        if _FLUSH_BATCH_SIZE > 10:
            nuevo_tam = max(_FLUSH_BATCH_SIZE // 2, 10)
            if nuevo_tam < _FLUSH_BATCH_SIZE:
                _FLUSH_BATCH_SIZE = nuevo_tam
                log.info(
                    f'ℹ️ FLUSH_BATCH_SIZE reducido a {_FLUSH_BATCH_SIZE} por flush lento'
                )
    else:
        _SLOW_FLUSHES = 0
    if _SLOW_FLUSHES >= _SLOW_FLUSH_LIMIT and not _USE_PROCESS_POOL:
        log.warning(
            '⚠️ flush_operaciones excede 30s de forma recurrente; activando ProcessPoolExecutor'
        )
        _USE_PROCESS_POOL = True

    if pendientes:
        with _BUFFER_LOCK:
            _BUFFER_OPERACIONES[:0] = pendientes

    global _ULTIMO_FLUSH
    _ULTIMO_FLUSH = time.time()

    if errores_sqlite == 0 and errores_parquet == 0:
        log.info(f'✅ {total_ops} operaciones guardadas correctamente.')
    else:
        log.warning(
            f'⚠️ Guardadas {total_ops} operaciones con errores — SQLite: {errores_sqlite}, Parquet: {errores_parquet}'
        )


async def flush_periodico(
    interval: int = _FLUSH_INTERVAL,
    heartbeat: int = 30,
    max_fallos: int = 5,
    reintento: int = 300,
) -> None:
    log.info('➡️ Entrando en flush_periodico()')
    """
    Ejecuta :func:`flush_operaciones` cada ``interval`` segundos.
    Emite ``tick('flush')`` periódicamente para evitar reinicios por inactividad.
    Tras ``max_fallos`` errores consecutivos, espera ``reintento`` segundos,
    notifica el fallo y reintenta el ciclo.
    """
    global _FLUSH_FUTURE
    fallos_consecutivos = 0
    try:
        while True:
            if _FLUSH_FUTURE and _FLUSH_FUTURE.done():
                try:
                    _FLUSH_FUTURE.result()
                except Exception as e:
                    log.error(f'❌ Error en flush previo: {e}')
                _FLUSH_FUTURE = None
            restante = interval
            while restante > 0:
                sleep_time = min(heartbeat, restante)
                await asyncio.sleep(sleep_time)
                tick('flush')
                restante -= sleep_time
            try:
                if _FLUSH_FUTURE and not _FLUSH_FUTURE.done():
                    log.warning('⚠️ flush_operaciones en curso; omitiendo nueva ejecución.')
                else:
                    loop = asyncio.get_running_loop()
                    _FLUSH_FUTURE = loop.run_in_executor(None, flush_operaciones)
                    await asyncio.wait_for(_FLUSH_FUTURE, timeout=30)
                    _FLUSH_FUTURE = None
                    tick('flush')
                    fallos_consecutivos = 0
            except asyncio.TimeoutError:
                fallos_consecutivos += 1
                log.warning('⚠️ flush_operaciones se excedió de tiempo (30s)')
            except Exception as e:
                fallos_consecutivos += 1
                log.error(f'❌ Error en flush periódico: {e}')
                _FLUSH_FUTURE = None
            if fallos_consecutivos >= max_fallos:
                mensaje = (
                    f'flush_periodico falló {fallos_consecutivos} veces consecutivas; deteniendo.'
                )
                log.error(f'🛑 {mensaje}')
                try:
                    notificador.enviar(mensaje, 'CRITICAL')
                except Exception:
                    pass
                log.info(f'Reintentando flush en {reintento}s...')
                await asyncio.sleep(reintento)
                fallos_consecutivos = 0
    except asyncio.CancelledError:
        if _FLUSH_FUTURE and not _FLUSH_FUTURE.done():
            log.info('Esperando a que flush_operaciones termine antes de cancelar.')
            try:
                await asyncio.wait_for(_FLUSH_FUTURE, timeout=5)
            except Exception:
                pass
        log.info('🛑 flush_periodico cancelado correctamente.')
        raise

atexit.register(flush_operaciones)
