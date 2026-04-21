"""Utilidades para registrar y mantener las órdenes reales.

El archivo Parquet se protege de escrituras concurrentes entre procesos
Las órdenes se almacenan en una pequeña base SQLite para facilitar la
persistencia entre reinicios del bot.

⚠️ **Estado global**: cachés como ``_CACHE_ORDENES``; buffer de operaciones en
:mod:`core.orders.real_orders_operaciones`; reconciliación/exchange en
:mod:`core.orders.real_orders_reconcile`; ejecución mercado/límite en
:mod:`core.orders.real_orders_execution`; métricas por ``operation_id`` en
:mod:`core.orders.real_orders_metrics`; auditoría post-error en
:mod:`core.orders.real_orders_audit`. El caché SQLite en memoria se serializa con
``_CACHE_ORDENES_LOCK`` entre hilos; sigue siendo una sola
instancia del bot por proceso. Si se desean ejecutar múltiples bots o tests
concurrentes en el mismo proceso, se recomienda encapsular este estado en una
clase o crear instancias independientes.

Variables de entorno relevantes:
- ``ORDENES_DB_PATH``: si se define y es ruta relativa, se resuelve respecto a la
  raíz del repositorio (:mod:`core.repo_paths`); si es absoluta, se usa tal cual.
  Por defecto: ``<repo>/ordenes_reales/ordenes.db``.
- ``MAX_BUFFER_OPERATIONS`` controla cuántas operaciones pueden acumularse en
  memoria antes de forzar un ``flush`` (por defecto ``10``).
- ``FLUSH_INTERVAL`` define el tiempo máximo en segundos entre ``flush``
  automáticos (por defecto ``300``).
- ``USE_PROCESS_POOL`` si se establece en ``1``/``true``/``yes`` permite
  persistir operaciones en un ``ProcessPoolExecutor`` evitando el GIL.
  ``FLUSH_BATCH_SIZE`` determina cuántas operaciones se persisten por lote
  (por defecto ``100``). Si un ``flush`` supera los 30s, el tamaño del lote se
  reduce automáticamente para disminuir la carga de cada escritura.
- ``EXECUTION_QUALITY_LOG_*``: ver :mod:`core.diag.execution_quality_log` (log de fills/slippage).
"""
import os
import json
import sqlite3
import time
import atexit
import threading
import asyncio
from pathlib import Path
from typing import Any, Callable, Mapping
from datetime import datetime, timezone

UTC = timezone.utc
from binance_api.ccxt_client import obtener_ccxt as obtener_cliente
from binance_api.filters import get_symbol_filters
from .order_model import Order
from core.orders.real_orders_parse import coerce_open_orders as _coerce_open_orders
from core.orders.real_orders_parse import coincide_operation_id as _coincide_operation_id
from core.orders.real_orders_parse import extraer_float as _extraer_float
from core.orders.real_orders_parse import extraer_valor as _extraer_valor
from core.utils.utils import configurar_logger, guardar_orden_real
from core.utils.logger import log_decision
from core.utils.log_utils import format_exception_for_log
from core.supervisor import tick
from core.notification_manager import crear_notification_manager_desde_env
from core.repo_paths import repo_root, resolve_under_repo
from config.config_manager import Config
from core.orders import real_orders_audit
from core.orders import real_orders_execution
from core.orders import real_orders_metrics
from core.orders import real_orders_operaciones
from core.orders import real_orders_reconcile
from core.orders import real_orders_sqlite

# Compat tests / introspección: mismo buffer y troceo que en ``real_orders_operaciones``.
_chunked = real_orders_operaciones._chunked
_BUFFER_OPERACIONES = real_orders_operaciones._BUFFER_OPERACIONES

try:
    from ccxt.base.errors import InsufficientFunds
except ImportError:


    class InsufficientFunds(Exception):
        pass
log = configurar_logger('ordenes')
notificador = crear_notification_manager_desde_env()
_ORDENES_DB_ENV = os.getenv("ORDENES_DB_PATH", "").strip()
if _ORDENES_DB_ENV:
    RUTA_DB = str(resolve_under_repo(Path(_ORDENES_DB_ENV)))
else:
    RUTA_DB = str((repo_root() / "ordenes_reales" / "ordenes.db").resolve())
ORDENES_DB_PATH = RUTA_DB
_CACHE_ORDENES: dict[str, Order] | None = None
_CACHE_ORDENES_LOCK = threading.RLock()
_VENTAS_FALLIDAS: set[str] = set()
_VENTAS_FALLIDAS_LOCK = threading.Lock()


def venta_fallida(symbol: str) -> bool:
    """Indica si hubo una venta fallida previa para `symbol`."""
    with _VENTAS_FALLIDAS_LOCK:
        return symbol in _VENTAS_FALLIDAS


def registrar_venta_fallida(symbol: str) -> None:
    """Registra `symbol` como venta fallida."""
    with _VENTAS_FALLIDAS_LOCK:
        _VENTAS_FALLIDAS.add(symbol)


def limpiar_venta_fallida(symbol: str) -> None:
    """Elimina `symbol` del registro de ventas fallidas."""
    with _VENTAS_FALLIDAS_LOCK:
        _VENTAS_FALLIDAS.discard(symbol)


_FLUSH_FUTURE: asyncio.Future | None = None

_METRICAS_OPERACION = real_orders_metrics.METRICAS_OPERACION
_acumular_metricas = real_orders_metrics.acumular_metricas
_auditar_operacion_post_error = real_orders_audit.auditar_operacion_post_error
_market_sell_retry = real_orders_execution._market_sell_retry


def _connect_db() -> sqlite3.Connection:
    """Abre la conexión SQLite aplicando configuraciones de rendimiento."""
    return real_orders_sqlite.connect_db(RUTA_DB)


def _validar_datos_orden(data: Mapping[str, Any]) -> dict:
    """Valida campos mínimos requeridos para persistir una orden."""
    return real_orders_sqlite.validar_datos_orden(data)


def esperar_balance(cliente, symbol: str, cantidad_esperada: float,
    max_intentos: int=10, delay: float=0.3) ->float:
    """Espera hasta que el balance disponible alcance la cantidad esperada."""
    try:
        base = symbol.split('/')[0]
    except Exception as e:
        log.error(
            '❌ Error al interpretar símbolo %s: %s',
            symbol,
            format_exception_for_log(e),
        )
        return 0.0
    for intento in range(max_intentos):
        try:
            balance = cliente.fetch_balance()
            disponible = balance.get('free', {}).get(base, 0.0)
            if disponible >= cantidad_esperada:
                return disponible
        except Exception as e:
            log.warning(
                '⚠️ Error al obtener balance en intento %s/%s: %s',
                intento + 1,
                max_intentos,
                format_exception_for_log(e),
            )
        time.sleep(delay)
    log.warning(
        f'⏱️ Tiempo de espera agotado para obtener balance suficiente en {symbol}. Disponible: {disponible}, requerido: {cantidad_esperada}'
        )
    return disponible


def _init_db() -> None:
    """Crea la tabla de órdenes y operaciones si no existen."""
    real_orders_sqlite.init_db(RUTA_DB)


def cargar_ordenes() -> dict[str, Order]:
    """Carga las órdenes almacenadas desde la base de datos SQLite."""
    global _CACHE_ORDENES
    with _CACHE_ORDENES_LOCK:
        if _CACHE_ORDENES is not None:
            return _CACHE_ORDENES
        try:
            ordenes = real_orders_sqlite.load_open_orders(RUTA_DB)
        except sqlite3.Error:
            return {}
        _CACHE_ORDENES = ordenes
        return _CACHE_ORDENES


def guardar_ordenes(ordenes: dict[str, Order]) -> None:
    """Guarda las órdenes en la base de datos si han cambiado respecto al caché."""
    global _CACHE_ORDENES
    with _CACHE_ORDENES_LOCK:
        if _CACHE_ORDENES and real_orders_sqlite.orders_stable_json(
            _CACHE_ORDENES
        ) == real_orders_sqlite.orders_stable_json(ordenes):
            return
        real_orders_sqlite.persist_orders(RUTA_DB, ordenes)
        _CACHE_ORDENES = ordenes


def obtener_orden(symbol: str) ->(Order | None):
    try:
        return cargar_ordenes().get(symbol)
    except Exception as e:
        log.error(
            '❌ Error al obtener orden de %s: %s',
            symbol,
            format_exception_for_log(e),
        )
        return None


def obtener_todas_las_ordenes():
    return cargar_ordenes()


def reconciliar_ordenes(simbolos: list[str] | None = None) -> dict[str, Order]:
    return real_orders_reconcile.reconciliar_ordenes(simbolos)


def sincronizar_ordenes_binance(
    simbolos: list[str] | None = None,
    config: Config | None = None,
    modo_real: bool | None = None,
) -> dict[str, Order]:
    """Consulta órdenes abiertas directamente desde Binance y las registra.

    Esto permite reconstruir el estado de las posiciones cuando el bot se
    reinicia y la base de datos local no contiene todas las operaciones
    abiertas. Devuelve el diccionario de órdenes resultante.
    """
    return real_orders_reconcile.sincronizar_ordenes_binance(
        simbolos, config, modo_real
    )


def consultar_ordenes_abiertas(symbol: str) -> list[dict]:
    """Consulta órdenes abiertas actuales para `symbol` con reintentos y registro detallado."""
    return real_orders_reconcile.consultar_ordenes_abiertas(symbol)


def reconciliar_trades_binance(
    simbolos: list[str] | None = None,
    limit: int = 50,
    apply_changes: bool = False,
    reporter: Callable[[list[dict[str, Any]]], None] | None = None,
) -> list[dict[str, Any]]:
    """Analiza trades recientes y detecta divergencias con el estado local.

    Parameters
    ----------
    simbolos:
        Lista de símbolos a reconciliar. Si se omite se utilizan todos los
        mercados disponibles en el cliente.
    limit:
        Número máximo de trades recientes a consultar por símbolo.
    apply_changes:
        Cuando es ``True`` intenta persistir automáticamente las divergencias.
        Por defecto sólo se reportan para revisión manual.
    reporter:
        Callback opcional que recibe la lista de divergencias detectadas.

    Returns
    -------
    list[dict[str, Any]]
        Detalles de cada divergencia detectada.
    """
    return real_orders_reconcile.reconciliar_trades_binance(
        simbolos, limit, apply_changes, reporter
    )


def actualizar_orden(symbol: str, data: (Order | dict)) ->None:
    with _CACHE_ORDENES_LOCK:
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
            log.info(f'📌 Order actualizada para {symbol}.', extra={'symbol': symbol, 'timeframe': None})
        except Exception as e:
            log.error(
                '❌ Error al actualizar orden para %s: %s',
                symbol,
                format_exception_for_log(e),
            )
            raise


def eliminar_orden(symbol: str, forzar_log: bool=False) ->None:
    """Elimina una orden activa del sistema si existe."""
    with _CACHE_ORDENES_LOCK:
        ordenes = cargar_ordenes()
        if symbol not in ordenes:
            if forzar_log:
                log.warning(f'⚠️ Intento de eliminar orden inexistente: {symbol}')
            else:
                log.debug(
                    f'Intento de eliminar orden inexistente ignorado: {symbol}',
                    extra={'symbol': symbol, 'timeframe': None},
                )
            return
        try:
            with _connect_db() as conn:
                conn.execute('DELETE FROM ordenes WHERE symbol = ?', (symbol,))
            del ordenes[symbol]
            _CACHE_ORDENES = ordenes
            log.info(f'🗑️ Order eliminada correctamente para {symbol}.', extra={'symbol': symbol, 'timeframe': None})
        except sqlite3.Error as e:
            log.error(
                '❌ Error eliminando orden de la base de datos: %s',
                format_exception_for_log(e),
            )
            raise


def registrar_orden(symbol: str, precio: float, cantidad: float, sl: float,
    tp: float, estrategias, tendencia: str, direccion: str='long',
    operation_id: str | None = None) ->None:
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
        timestamp=datetime.now(UTC).isoformat(),
        estrategias_activas=estrategias,
        tendencia=tendencia,
        max_price=precio,
        direccion=direccion,
        operation_id=operation_id,
    )
    actualizar_orden(symbol, orden)
    log_decision(
        log,
        'registrar_orden',
        operation_id,
        {'symbol': symbol, 'precio': precio, 'cantidad': cantidad},
        {'precio_valido': precio > 0, 'cantidad_valida': cantidad > 0},
        'accept',
        {'registrada': True},
    )


def registrar_operacion(data: (dict | Order)) ->None:
    """Agrega una operación ejecutada al buffer. Se persistirá automáticamente."""
    real_orders_operaciones.registrar_operacion_en_buffer(data, RUTA_DB)


def ejecutar_orden_market(
    symbol: str,
    cantidad: float,
    operation_id: str | None = None,
    *,
    order_attempt: int = 1,
    precio_senal_bot: float | None = None,
) -> dict:
    """Ejecuta una compra de mercado y devuelve detalles de la ejecución.

    ``precio_senal_bot`` es el precio que usó el bot al decidir (p. ej. cierre de vela);
    permite medir slippage respecto al ticker del exchange al enviar la orden.
    """
    return real_orders_execution.ejecutar_orden_market(
        symbol,
        cantidad,
        operation_id,
        order_attempt=order_attempt,
        precio_senal_bot=precio_senal_bot,
    )


def ejecutar_orden_market_sell(
    symbol: str,
    cantidad: float,
    operation_id: str | None = None,
    *,
    order_attempt: int = 1,
    precio_senal_bot: float | None = None,
) -> dict:
    """Ejecuta una venta de mercado validando saldo y devuelve detalles."""
    return real_orders_execution.ejecutar_orden_market_sell(
        symbol,
        cantidad,
        operation_id,
        order_attempt=order_attempt,
        precio_senal_bot=precio_senal_bot,
    )


def _market_sell_retry(
    symbol: str,
    cantidad: float,
    operation_id: str | None = None,
    order_attempt_start: int = 1,
    *,
    precio_senal_bot: float | None = None,
) -> dict:
    """Envía ventas de mercado reintentando en caso de fills parciales.

    ``order_attempt_start`` alinea ``newClientOrderId`` con reintentos externos
    (p. ej. :class:`MarketRetryExecutor`): cada llamada a Binance debe usar un
    id de cliente distinto aunque este bucle se reinicie tras un error.
    """
    return real_orders_execution._market_sell_retry(
        symbol,
        cantidad,
        operation_id,
        order_attempt_start,
        precio_senal_bot=precio_senal_bot,
    )


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
    """Ejecuta una orden LIMIT con re-precio y fallback a MARKET.

    Si la orden no se completa dentro de `timeout` segundos se cancela y se
    re-precia usando `offset_reprice`. Tras `max_reintentos` intentos fallidos,
    envía una orden de mercado.
    """
    return real_orders_execution.ejecutar_orden_limit(
        symbol,
        side,
        precio,
        cantidad,
        operation_id,
        timeout,
        offset_reprice,
        max_reintentos,
    )


def _persistir_operaciones(operaciones: list[dict]) -> tuple[int, int, list[dict]]:
    """Persiste operaciones en SQLite y Parquet (delegación sobre ``real_orders_operaciones``)."""
    return real_orders_operaciones.persist_operaciones_batch(operaciones, RUTA_DB)


def flush_operaciones() -> None:
    """Guarda en disco todas las operaciones acumuladas.

    Las operaciones se procesan en lotes para minimizar el impacto de las
    iteraciones largas sobre SQLite y las escrituras en Parquet manejadas por
    :mod:`pandas`.
    """
    real_orders_operaciones.flush_buffer_operaciones(RUTA_DB)


async def flush_periodico(
    interval: int | None = None,
    heartbeat: int = 30,
    max_fallos: int = 5,
    reintento: int = 300,
) -> None:
    """
    Ejecuta :func:`flush_operaciones` cada ``interval`` segundos.
    Emite ``tick('flush')`` periódicamente para evitar reinicios por inactividad.
    Tras ``max_fallos`` errores consecutivos, espera ``reintento`` segundos,
    notifica el fallo y reintenta el ciclo.
    """
    global _FLUSH_FUTURE
    eff_interval = (
        interval
        if interval is not None
        else real_orders_operaciones._FLUSH_INTERVAL
    )
    fallos_consecutivos = 0
    try:
        while True:
            if _FLUSH_FUTURE and _FLUSH_FUTURE.done():
                try:
                    _FLUSH_FUTURE.result()
                except Exception as e:
                    log.error(
                        '❌ Error en flush previo: %s',
                        format_exception_for_log(e),
                    )
                _FLUSH_FUTURE = None
            restante = eff_interval
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
                log.error(
                    '❌ Error en flush periódico: %s',
                    format_exception_for_log(e),
                )
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
                log.info(f'Reintentando flush en {reintento}s...', extra={'symbol': None, 'timeframe': None})
                await asyncio.sleep(reintento)
                fallos_consecutivos = 0
    except asyncio.CancelledError:
        if _FLUSH_FUTURE and not _FLUSH_FUTURE.done():
            log.info('Esperando a que flush_operaciones termine antes de cancelar.', extra={'symbol': None, 'timeframe': None})
            try:
                await asyncio.wait_for(_FLUSH_FUTURE, timeout=5)
            except Exception:
                pass
        log.info('🛑 flush_periodico cancelado correctamente.', extra={'symbol': None, 'timeframe': None})
        raise

atexit.register(flush_operaciones)
