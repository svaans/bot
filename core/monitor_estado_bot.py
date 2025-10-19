import asyncio
import sqlite3
import os
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

UTC = timezone.utc
from typing import Any
import inspect

from binance_api.cliente import BinanceClient, fetch_balance_async
from config import config as app_config
from ccxt.base.errors import AuthenticationError, NetworkError
from core.operational_mode import OperationalMode
from core.utils.utils import configurar_logger
from core.orders import real_orders
from core.reporting import reporter_diario
from core.risk.riesgo import cargar_estado_riesgo
from core.supervisor import beat, tick
from observability.metrics import (
    EMOTIONAL_RISK_GAUGE,
    EMOTIONAL_STATE_SCORE,
    EMOTIONAL_STATE_TRANSITIONS,
    EMOTIONAL_STREAK_GAUGE,
)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORDENES_DB_PATH = os.getenv(
    'ORDENES_DB_PATH',
    os.path.join(BASE_DIR, 'ordenes_reales', 'ordenes.db'),
)
log = configurar_logger('estado_bot')

_ESTADO_EMOCIONAL_ACTUAL: str | None = None
_ESTADO_EMOCIONAL_SCORE = {
    '😎 Determinado': 3.0,
    '🤔 Neutro': 2.0,
    '😐 Observador': 1.0,
    '😰 Cauteloso': 0.0,
}


def obtener_orden_abierta():
    if os.path.exists(ORDENES_DB_PATH):
        try:
            ordenes = real_orders.cargar_ordenes()
            modo = getattr(app_config, 'MODO_OPERATIVO', OperationalMode.from_bool(app_config.MODO_REAL))
            if not ordenes and (modo.is_real or modo.uses_testnet):
                ordenes = real_orders.sincronizar_ordenes_binance(
                    config=app_config.cfg
                )
            return ordenes if ordenes else None
        except (OSError, sqlite3.Error) as e:
            log.warning(f'⚠️ Error al leer órdenes desde la base de datos: {e}')
            return None
    return None


def _contar_rachas(operaciones: list[dict]) -> tuple[int, int]:
    """Calcula las rachas consecutivas de ganancias y pérdidas."""
    ganancias = 0
    perdidas = 0
    for op in reversed(operaciones):
        retorno = op.get('retorno_total', 0)
        if retorno > 0 and perdidas == 0:
            ganancias += 1
        elif retorno < 0 and ganancias == 0:
            perdidas += 1
        else:
            break
    return ganancias, perdidas


def _evaluar_estado_emocional() -> tuple[str, int, int, float]:
    """Calcula estado emocional, rachas y riesgo consolidado."""
    operaciones: list[dict] = []
    for ops in reporter_diario.ultimas_operaciones.values():
        operaciones.extend(ops)
    operaciones.sort(key=lambda o: o.get('fecha_cierre', ''))
    ganancias, perdidas = _contar_rachas(operaciones)
    riesgo = float(cargar_estado_riesgo().get('perdida_acumulada', 0.0))
    if ganancias >= 3:
        estado = '😎 Determinado'
    elif perdidas >= 2 or riesgo > 1.5:
        estado = '😰 Cauteloso'
    elif ganancias == 0 and riesgo < 0.5:
        estado = '😐 Observador'
    else:
        estado = '🤔 Neutro'
    return estado, ganancias, perdidas, riesgo


def estimar_estado_emocional(_ultima_orden=None):
    """Determina el estado emocional actual del bot basado en el desempeño."""

    estado, _, _, _ = _evaluar_estado_emocional()
    return estado


def resumen_emocional() ->str:
    """Genera una breve justificación del estado emocional."""
    _, ganancias, perdidas, riesgo = _evaluar_estado_emocional()
    return (
        f'{ganancias} ganancias consecutivas, {perdidas} pérdidas consecutivas, riesgo acumulado {riesgo:.2f}%'
        )


async def monitorear_estado_bot(
    ordenes_memoria: dict | None = None,
    get_balance: Callable[[], float | Awaitable[float]] | None = None,
    *,
    cliente: Any | None = None,
) -> None:
    """Muestra el estado del bot y las órdenes activas sin bloquear el loop."""


    try:
        orden_abierta = obtener_orden_abierta()
        if not orden_abierta and ordenes_memoria:
            orden_abierta = ordenes_memoria

        euros = await _obtener_saldo_euros(get_balance=get_balance, cliente=cliente)
            
        log.info('======= 🤖 ESTADO ACTUAL DEL BOT =======')
        log.info(
            f"🕒 Hora actual: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC"
            )
        modo = getattr(app_config, 'MODO_OPERATIVO', OperationalMode.from_bool(app_config.MODO_REAL))
        if modo.is_real:
            etiqueta_saldo = '💶 Saldo disponible (EUR)'
        elif modo.uses_testnet:
            etiqueta_saldo = '💶 Saldo staging (EUR)'
        else:
            etiqueta_saldo = '💶 Saldo simulado (EUR)'
        log.info(f'{etiqueta_saldo}: {euros:.2f}')
        if orden_abierta:
            for symbol, orden in orden_abierta.items():
                precio = orden.get('precio_entrada') if isinstance(orden, dict
                    ) else getattr(orden, 'precio_entrada', None)
                sl = orden.get('stop_loss') if isinstance(orden, dict
                    ) else getattr(orden, 'stop_loss', None)
                tp = orden.get('take_profit') if isinstance(orden, dict
                    ) else getattr(orden, 'take_profit', None)
                log.info(
                    f'📈 Orden abierta: {symbol} → Entrada: {precio} | SL: {sl} | TP: {tp}'
                    )
        else:
            log.info('📭 No hay órdenes abiertas.')
        estado_emocional, ganancias, perdidas, riesgo = _evaluar_estado_emocional()
        _actualizar_metricas_emocionales(estado_emocional, ganancias, perdidas, riesgo)
        log.info(
            f'🧠 Estado emocional del bot: {estado_emocional} — {resumen_emocional()}'
            )
        log.info('========================================')
    except AuthenticationError:
        log.error(
            '🔒 Error de autenticación con Binance API. Verifica tus claves.')
    except NetworkError:
        log.error(
            '📡 Error de red al contactar con Binance. Verifica tu conexión.')
    except Exception:
        log.exception('❌ Error inesperado en monitoreo del bot')
        raise


async def monitorear_estado_periodicamente(self, intervalo=300, heartbeat=30):
    """Ejecuta ``monitorear_estado_bot`` de forma periódica sin bloquear el loop.

    ``heartbeat`` controla cada cuántos segundos se emite ``tick('estado')``
    durante la espera para evitar reinicios por inactividad.
    """
    while True:
        try:
            beat('estado', 'start')
            await asyncio.wait_for(
                monitorear_estado_bot(
                    dict(self.ordenes_abiertas),
                    cliente=getattr(self, 'cliente', None),
                ),
                timeout=intervalo,
            )
            beat('estado')
            log.info('🧭 Monitoreo de estado completado.')
            log.debug(
                f'📌 Órdenes abiertas: {list(self.ordenes_abiertas.keys())}')
            restante = intervalo
            while restante > 0:
                sleep_time = min(heartbeat, restante)
                await asyncio.sleep(sleep_time)
                tick('estado')
                restante -= sleep_time
        except asyncio.TimeoutError:
            log.warning('⌛ Monitoreo de estado excedió el tiempo')
            beat('estado', 'timeout')
            await asyncio.sleep(intervalo)
        except asyncio.CancelledError:
            log.info('⏹️ Monitoreo cancelado. Cerrando tarea.')
            break
        except Exception:
            log.exception('⚠️ Error durante el monitoreo de estado')
            beat('estado', 'error')
            await asyncio.sleep(intervalo)


async def _obtener_saldo_euros(
    *,
    get_balance: Callable[[], float | Awaitable[float]] | None,
    cliente: Any | None,
) -> float:
    """Obtiene el saldo en euros (o moneda base) sin bloquear el loop."""

    if get_balance is not None:
        saldo = get_balance()
        if inspect.isawaitable(saldo):
            return float(await saldo)
        return float(saldo)

    balance = await _fetch_balance(cliente)
    totales = balance.get('total', {}) if isinstance(balance, dict) else {}
    if not isinstance(totales, dict):
        totales = {}
    if 'EUR' in totales:
        return float(totales['EUR'])
    if 'USDT' in totales:
        return float(totales['USDT'])
    try:
        return float(next(iter(totales.values())))
    except StopIteration:
        return 0.0


async def _fetch_balance(cliente: Any | None) -> dict:
    """Recupera el balance soportando clientes síncronos y asíncronos."""

    if cliente is None:
        cliente = BinanceClient(app_config.cfg)

    fetch_balance = getattr(cliente, 'fetch_balance', None)
    if callable(fetch_balance):
        resultado = fetch_balance()
        if inspect.isawaitable(resultado):
            return await resultado
        return resultado

    fetch_balance_async_attr = getattr(cliente, 'fetch_balance_async', None)
    if callable(fetch_balance_async_attr):
        return await fetch_balance_async_attr()

    return await fetch_balance_async(cliente if isinstance(cliente, BinanceClient) else None)


def _actualizar_metricas_emocionales(
    estado: str,
    ganancias: int,
    perdidas: int,
    riesgo: float,
) -> None:
    """Actualiza métricas Prometheus relacionadas con el estado emocional."""

    global _ESTADO_EMOCIONAL_ACTUAL

    EMOTIONAL_STATE_SCORE.set(_ESTADO_EMOCIONAL_SCORE.get(estado, 0.0))
    EMOTIONAL_RISK_GAUGE.set(riesgo)
    EMOTIONAL_STREAK_GAUGE.labels(type='ganancias').set(float(ganancias))
    EMOTIONAL_STREAK_GAUGE.labels(type='perdidas').set(float(perdidas))

    if estado != _ESTADO_EMOCIONAL_ACTUAL:
        EMOTIONAL_STATE_TRANSITIONS.labels(state=estado).inc()
        _ESTADO_EMOCIONAL_ACTUAL = estado
