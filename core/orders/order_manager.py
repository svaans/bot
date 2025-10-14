"""Gestión de órdenes simuladas o reales."""
from __future__ import annotations

import asyncio
import math
import os
import random
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from core.orders.order_model import Order
from core.utils.logger import configurar_logger, log_decision
from core.utils.log_utils import safe_extra
from core.orders import real_orders
# Importamos validadores con nombres más descriptivos.  El módulo
# ``validators_binance`` centraliza las restricciones de Binance.
from core.orders.validators_binance import remainder_executable  # renamed from validators
from core.risk.level_validators import (
    validate_levels,
    LevelValidationError,
)
from core.utils.utils import is_valid_number
from core.event_bus import EventBus
from core.metrics import (
    registrar_buy_rejected_insufficient_funds,
    registrar_market_retry_exhausted,
    registrar_orden,
    registrar_orders_sync_failure,
    registrar_orders_sync_success,
    registrar_partial_close_collision,
    registrar_registro_error,
)
from core.registro_metrico import registro_metrico
from binance_api.cliente import obtener_cliente

log = configurar_logger('orders', modo_silencioso=True)
UTC = timezone.utc

MAX_HISTORIAL_ORDENES = 1000


_META_NESTED_KEYS = ("orden", "order", "position", "trade", "entrada", "payload")


def _coerce_float(value: Any) -> float | None:
    """Convierte ``value`` en ``float`` si es posible."""

    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):  # pragma: no cover - defensivo
            return None
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    """Convierte ``value`` en ``int`` si es posible."""

    float_value = _coerce_float(value)
    if float_value is None:
        return None
    try:
        return int(float_value)
    except (TypeError, ValueError):  # pragma: no cover - defensivo
        return None


def _lookup_meta(meta: Mapping[str, Any], keys: Iterable[str]) -> Any:
    """Busca la primera coincidencia de ``keys`` en ``meta`` o sub-mappings conocidos."""

    for key in keys:
        if key in meta:
            return meta[key]
    for nested_key in _META_NESTED_KEYS:
        nested = meta.get(nested_key)
        if isinstance(nested, Mapping):
            for key in keys:
                if key in nested:
                    return nested[key]
    return None


def _resolve_quantity(meta: Mapping[str, Any], precio: float) -> float:
    """Obtiene la cantidad a operar a partir de distintos campos del ``meta``."""

    quantity_value = _lookup_meta(
        meta,
        (
            "cantidad",
            "cantidad_operar",
            "quantity",
            "qty",
            "size",
            "amount",
            "position_size",
        ),
    )
    quantity = _coerce_float(quantity_value)
    if quantity is not None and quantity > 0:
        return float(quantity)

    capital_value = _lookup_meta(
        meta,
        (
            "capital",
            "capital_operar",
            "capital_disponible",
            "risk_capital",
            "notional",
            "capital_usd",
        ),
    )
    capital = _coerce_float(capital_value)
    if capital is not None and capital > 0 and precio > 0:
        return float(capital / precio)

    return 0.0


class OrderManager:
    """Abstrae la creación y cierre de órdenes."""

    def __init__(
        self,
        modo_real: bool,
        bus: EventBus | None = None,
        max_historial: int = MAX_HISTORIAL_ORDENES,
    ) -> None:
        self.modo_real = modo_real
        self.ordenes: Dict[str, Order] = {}
        self.historial: Dict[str, list] = {}
        self.bus = bus
        self.max_historial = max_historial
        self.abriendo: set[str] = set()
        self._locks: Dict[str, asyncio.Lock] = {}
        # Símbolos para los que ya se registró un mensaje de duplicado
        self._dup_warned: set[str] = set()
        self._sync_task: asyncio.Task | None = None
        self._sync_interval = 300
        self._sync_base_interval = self._sync_interval
        self._sync_min_interval = float(
            os.getenv("ORDERS_SYNC_MIN_INTERVAL", "60")
        )
        self._sync_max_interval = float(
            os.getenv("ORDERS_SYNC_MAX_INTERVAL", "900")
        )
        self._sync_backoff_factor = float(
            os.getenv("ORDERS_SYNC_BACKOFF_FACTOR", "2.0")
        )
        self._sync_jitter = float(os.getenv("ORDERS_SYNC_JITTER", "0.1"))
        self._sync_failures = 0
        self._market_retry_max_attempts = int(
            os.getenv("ORDERS_MARKET_MAX_RETRIES", "5")
        )
        self._market_retry_backoff = float(
            os.getenv("ORDERS_MARKET_BACKOFF_SECONDS", "1.0")
        )
        self._market_retry_backoff_multiplier = float(
            os.getenv("ORDERS_MARKET_BACKOFF_MULTIPLIER", "1.5")
        )
        self._market_retry_backoff_cap = float(
            os.getenv("ORDERS_MARKET_BACKOFF_MAX_SECONDS", "30.0")
        )
        self._market_retry_no_progress_limit = int(
            os.getenv("ORDERS_MARKET_MAX_NO_PROGRESS", "3")
        )
        if bus:
            self.subscribe(bus)

    def _generar_operation_id(self, symbol: str) -> str:
        """Genera un identificador único para agrupar fills de una operación."""
        nonce = os.urandom(2).hex()
        ts = int(datetime.now(UTC).timestamp() * 1000)
        return f"{symbol.replace('/', '')}-{ts}-{nonce}"

    async def _ejecutar_market_retry(
        self,
        side: str,
        symbol: str,
        cantidad: float,
        operation_id: str | None = None,
    ) -> tuple[float, float, float]:
        operation_id = operation_id or self._generar_operation_id(symbol)
        entrada = {'side': side, 'symbol': symbol, 'cantidad': cantidad}
        if side == 'sell':
            return await self._ejecutar_market_sell(
                symbol, cantidad, operation_id, entrada
            )
        return await self._ejecutar_market_buy(
            symbol, cantidad, operation_id, entrada
        )

    async def _ejecutar_market_sell(
        self,
        symbol: str,
        cantidad: float,
        operation_id: str,
        entrada: dict,
    ) -> tuple[float, float, float]:
        """Ejecuta ventas de mercado con circuit breaker configurable."""
        intentos = 0
        while True:
            intentos += 1
            try:
                resp = await asyncio.to_thread(
                    real_orders._market_sell_retry, symbol, cantidad, operation_id
                )
                salida = {
                    'ejecutado': float(resp.get('ejecutado', 0.0)),
                    'fee': float(resp.get('fee', 0.0)),
                    'pnl': float(resp.get('pnl', 0.0)),
                }
                log_decision(
                    log, '_market_sell', operation_id, entrada, {}, 'execute', salida
                )
                return salida['ejecutado'], salida['fee'], salida['pnl']
            except Exception as exc:  # pragma: no cover - path defensivo
                log.error(
                    '❌ Error ejecutando venta en %s (intento %s/%s): %s',
                    symbol,
                    intentos,
                    self._market_retry_max_attempts,
                    exc,
                    extra={'event': 'market_sell_retry_error', 'symbol': symbol},
                )
                if intentos >= self._market_retry_max_attempts:
                    await self._manejar_market_retry_exhausted(
                        side='sell', symbol=symbol, operation_id=operation_id
                    )
                    return 0.0, 0.0, 0.0
                await asyncio.sleep(self._market_retry_sleep(intentos))

    async def _ejecutar_market_buy(
        self,
        symbol: str,
        cantidad: float,
        operation_id: str,
        entrada: dict,
    ) -> tuple[float, float, float]:
        """Ejecuta compras de mercado con límite de reintentos y backoff."""
        restante = cantidad
        total = total_fee = total_pnl = 0.0
        intentos = 0
        sin_progreso = 0
        while restante > 0:
            intentos += 1
            restante_previo = restante
            try:
                resp = await asyncio.to_thread(
                    real_orders.ejecutar_orden_market, symbol, restante, operation_id
                )
            except Exception as exc:
                log.error(
                    '❌ Error ejecutando compra en %s (intento %s/%s): %s',
                    symbol,
                    intentos,
                    self._market_retry_max_attempts,
                    exc,
                    extra={'event': 'market_buy_retry_error', 'symbol': symbol},
                )
                sin_progreso += 1
                if self._should_break_retry(intentos, sin_progreso):
                    await self._manejar_market_retry_exhausted(
                        side='buy', symbol=symbol, operation_id=operation_id
                    )
                    break
                await asyncio.sleep(self._market_retry_sleep(intentos))
                continue

            ejecutado = float(resp.get('ejecutado', 0.0))
            total += ejecutado
            restante = float(resp.get('restante', 0.0))
            total_fee += float(resp.get('fee', 0.0))
            total_pnl += float(resp.get('pnl', 0.0))

            if math.isclose(restante, restante_previo, rel_tol=1e-09, abs_tol=1e-12):
                sin_progreso += 1
            else:
                sin_progreso = 0

            if resp.get('status') != 'PARTIAL' or restante < resp.get('min_qty', 0):
                break

            if self._should_break_retry(intentos, sin_progreso):
                await self._manejar_market_retry_exhausted(
                    side='buy', symbol=symbol, operation_id=operation_id
                )
                break

            await asyncio.sleep(self._market_retry_sleep(intentos))
        salida = {'ejecutado': total, 'fee': total_fee, 'pnl': total_pnl}
        log_decision(log, '_market_buy', operation_id, entrada, {}, 'execute', salida)
        return total, total_fee, total_pnl
    
    def _market_retry_sleep(self, attempt: int) -> float:
        """Calcula la espera exponencial con límite superior."""
        base = max(self._market_retry_backoff, 0.0)
        if base <= 0:
            return 0.0
        multiplier = max(self._market_retry_backoff_multiplier, 1.0)
        delay = base * (multiplier ** max(attempt - 1, 0))
        return min(delay, self._market_retry_backoff_cap)

    def _should_break_retry(self, attempts: int, no_progress: int) -> bool:
        """Evalúa si deben interrumpirse los reintentos."""
        if attempts >= self._market_retry_max_attempts:
            return True
        return no_progress >= self._market_retry_no_progress_limit > 0

    async def _manejar_market_retry_exhausted(
        self, *, side: str, symbol: str, operation_id: str
    ) -> None:
        """Gestiona agotamiento de reintentos: logging, métricas y alertas."""
        registrar_market_retry_exhausted(side, symbol)
        log.error(
            '⛔ Reintentos de orden de mercado agotados',
            extra={
                'event': 'market_retry_exhausted',
                'side': side,
                'symbol': symbol,
                'operation_id': operation_id,
                'max_attempts': self._market_retry_max_attempts,
            },
        )
        if self.bus:
            await self.bus.publish(
                'notify',
                {
                    'mensaje': (
                        f'⛔ No se pudo ejecutar orden {side} en {symbol}: '
                        f'{self._market_retry_max_attempts} reintentos fallidos'
                    ),
                    'tipo': 'CRITICAL',
                    'operation_id': operation_id,
                },
            )

    def subscribe(self, bus: EventBus) -> None:
        bus.subscribe('abrir_orden', self._on_abrir)
        bus.subscribe('cerrar_orden', self._on_cerrar)
        bus.subscribe('cerrar_parcial', self._on_cerrar_parcial)
        bus.subscribe('agregar_parcial', self._on_agregar_parcial)
        if self.modo_real:
            self.start_sync()

    async def _on_abrir(self, data: dict) -> None:
        fut = data.pop('future', None)
        result = await self.abrir_async(**data)
        if fut:
            fut.set_result(result)

    async def _on_cerrar(self, data: dict) -> None:
        fut = data.pop('future', None)
        result = await self.cerrar_async(**data)
        if fut:
            fut.set_result(result)

    async def _on_cerrar_parcial(self, data: dict) -> None:
        fut = data.pop('future', None)
        result = await self.cerrar_parcial_async(**data)
        if fut:
            fut.set_result(result)

    async def _on_agregar_parcial(self, data: dict) -> None:
        fut = data.pop('future', None)
        result = await self.agregar_parcial_async(**data)
        if fut:
            fut.set_result(result)

    def start_sync(self, intervalo: int | None = None) -> None:
        if intervalo:
            self._sync_interval = intervalo
            self._sync_base_interval = intervalo
        if self._sync_task is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if loop.is_running():
            # Solo arrancamos el loop; dentro se hace un _sync_once inicial.
            self._sync_task = loop.create_task(self._sync_loop())

    async def _sync_once(self) -> bool:
        actuales = set(self.ordenes.keys())
        try:
            ordenes_reconciliadas: Dict[str, Order] = await asyncio.to_thread(
                real_orders.reconciliar_ordenes
            )
        except Exception as e:
            log.error(f'❌ Error sincronizando órdenes: {e}')
            return

        reconciliadas = set(ordenes_reconciliadas.keys())
        local_only = actuales - reconciliadas
        exchange_only = reconciliadas - actuales

        if local_only or exchange_only:
            for sym in local_only:
                log.warning(f'⚠️ Orden local {sym} ausente en exchange; cerrada.')
                if self.bus:
                    await self.bus.publish(
                        'notify',
                        {
                            'mensaje': f'⚠️ Orden local {sym} cerrada por reconciliación',
                            'tipo': 'WARNING',
                        },
                    )
            for sym in exchange_only:
                log.warning(f'⚠️ Orden {sym} encontrada en exchange y añadida.')
                if self.bus:
                    await self.bus.publish(
                        'notify',
                        {
                            'mensaje': f'🔄 Orden sincronizada desde Binance: {sym}',
                            'tipo': 'WARNING',
                        },
                    )
            registro_metrico.registrar(
                'discrepancia_ordenes',
                {'local': len(local_only), 'exchange': len(exchange_only)},
                    )

        merged: Dict[str, Order] = {}

        # Actualiza/mezcla las que existen en remoto
        for sym, remoto in ordenes_reconciliadas.items():
            local = self.ordenes.get(sym)
            if local:
                # Preserva flags locales si aplican
                setattr(
                    remoto,
                    'registro_pendiente',
                    getattr(local, 'registro_pendiente', False)
                    or getattr(remoto, 'registro_pendiente', False),
                )
                # Si tienes otros campos efímeros, mapéalos aquí
            merged[sym] = remoto

        self.ordenes = merged

        # Intenta registrar las que quedaron pendientes
        errores_registro = False
        for sym, ord_ in list(self.ordenes.items()):
            if getattr(ord_, 'registro_pendiente', False):
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
                    registrar_orden('opened')
                    log.info(f'🟢 Orden registrada tras reintento para {sym}')
                    if self.bus:
                        estrategias_txt = ', '.join(ord_.estrategias_activas.keys())
                        mensaje = (
                            f"""🟢 Compra {sym}\nPrecio: {ord_.precio_entrada:.2f} Cantidad: {ord_.cantidad_abierta or ord_.cantidad}\nSL: {ord_.stop_loss:.2f} TP: {ord_.take_profit:.2f}\nEstrategias: {estrategias_txt}"""
                        )
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': mensaje,
                                'operation_id': ord_.operation_id,
                            },
                        )
                except Exception as e:
                    log.error(f'❌ Error registrando orden pendiente {sym}: {e}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'❌ Error registrando orden pendiente {sym}: {e}',
                                'tipo': 'CRITICAL',
                                'operation_id': ord_.operation_id,
                            },
                        )
                    registrar_orders_sync_failure(type(e).__name__)
                    errores_registro = True

        if errores_registro:
            return False

        registrar_orders_sync_success()
        return True


    async def _sync_loop(self) -> None:
        while True:
            success = await self._sync_once()
            delay = self._compute_next_sync_delay(success)
            await asyncio.sleep(delay)

    def _compute_next_sync_delay(self, success: bool) -> float:
        """Calcula el próximo retardo del loop de sincronización."""
        if success:
            self._sync_failures = 0
            self._sync_interval = self._sync_base_interval
        else:
            self._sync_failures += 1
            next_interval = self._sync_base_interval * (
                self._sync_backoff_factor ** self._sync_failures
            )
            self._sync_interval = min(self._sync_max_interval, next_interval)
        delay = max(self._sync_min_interval, self._sync_interval)
        if self._sync_jitter > 0:
            jitter = random.uniform(-self._sync_jitter, self._sync_jitter)
            delay *= 1 + jitter
        return max(self._sync_min_interval, min(delay, self._sync_max_interval))

    def _schedule_registro_retry(self, symbol: str) -> None:
        """Programa un reintento rápido de registro sin esperar al sync loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        async def _retry():
            try:
                orden = self.ordenes.get(symbol)
                if not orden or not getattr(orden, 'registro_pendiente', False):
                    return
                await asyncio.to_thread(
                    real_orders.registrar_orden,
                    symbol,
                    orden.precio_entrada,
                    orden.cantidad_abierta or orden.cantidad,
                    orden.stop_loss,
                    orden.take_profit,
                    orden.estrategias_activas,
                    orden.tendencia,
                    orden.direccion,
                    orden.operation_id,
                )
                orden.registro_pendiente = False
                registrar_orden('opened')
                log.info(f'🟢 Orden registrada tras reintento rápido para {symbol}')
            except Exception as e:
                log.error(f'❌ Reintento rápido de registro falló para {symbol}: {e}')
        loop.create_task(_retry())

    async def abrir_async(
        self,
        symbol: str,
        precio: float,
        sl: float,
        tp: float,
        estrategias: Dict,
        tendencia: str,
        direccion: str = 'long',
        cantidad: float = 0.0,
        puntaje: float = 0.0,
        umbral: float = 0.0,
        score_tecnico: float = 0.0,
        objetivo: float | None = None,
        fracciones: int = 1,
        detalles_tecnicos: dict | None = None,
        tick_size: float | None = None,
        step_size: float | None = None,
        min_dist_pct: float | None = None,
        *,
        candle_close_ts: int | None = None,   # reservado (no usado aquí)
        strategy_version: str | None = None,  # reservado (no usado aquí)
    ) -> bool:
        operation_id = self._generar_operation_id(symbol)
        tick_size_value = float(tick_size or 0.0)
        step_size_value = float(step_size or 0.0)
        try:
            min_dist_pct_value = float(min_dist_pct) if min_dist_pct is not None else 0.0
        except (TypeError, ValueError):
            min_dist_pct_value = 0.0

        try:
            precio, sl, tp = validate_levels(
                direccion,
                precio,
                sl,
                tp,
                min_dist_pct_value,
                tick_size_value,
                step_size_value,
            )
        except LevelValidationError as exc:
            contexto = dict(exc.context)
            contexto.update({'symbol': symbol, 'reason': exc.reason})
            entrada_log = {
                'symbol': symbol,
                'precio': contexto.get('entry', precio),
                'sl': contexto.get('sl', sl),
                'tp': contexto.get('tp', tp),
                'cantidad': cantidad,
            }
            log.warning(
                f'⚠️ {symbol}: validación SL/TP fallida ({exc.reason})',
                extra=contexto,
            )
            log_decision(
                log,
                'abrir',
                operation_id,
                entrada_log,
                {'niveles_validos': False},
                'reject',
                {'reason': 'invalid_levels', 'contexto': contexto},
            )
            registrar_orden('rejected')
            return False
        
        entrada_log = {
            'symbol': symbol,
            'precio': precio,
            'sl': sl,
            'tp': tp,
            'cantidad': cantidad,
        }
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            # Evitar duplicados si ya se está abriendo o existe localmente
            if symbol in self.abriendo or symbol in self.ordenes:
                if symbol not in self._dup_warned:
                    log.warning(f'⚠️ Orden duplicada evitada para {symbol}')
                    self._dup_warned.add(symbol)
                log_decision(
                    log,
                    'abrir',
                    operation_id,
                    entrada_log,
                    {'duplicada': True},
                    'reject',
                    {'reason': 'duplicate'},
                )
                return False
            ordenes_api = {}
            if self.modo_real:
                try:
                    ordenes_api = await asyncio.to_thread(
                        real_orders.sincronizar_ordenes_binance,
                        [symbol],
                        modo_real=self.modo_real,
                    )
                except Exception as e:
                    log.error(f'❌ Error verificando órdenes abiertas: {e}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'⚠️ No se pudo verificar órdenes abiertas en {symbol}',
                                'tipo': 'WARNING',
                                'operation_id': operation_id,
                            },
                        )
                    log_decision(
                        log,
                        'abrir',
                        operation_id,
                        entrada_log,
                        {'verificacion': 'error'},
                        'reject',
                        {'reason': 'sync_error'},
                    )
                    return False
                
            if symbol in ordenes_api:
                self.ordenes[symbol] = ordenes_api[symbol]
                if symbol not in self._dup_warned:
                    log.warning(f'⚠️ Orden duplicada evitada para {symbol}')
                    self._dup_warned.add(symbol)
                if self.bus:
                    await self.bus.publish(
                        'notify',
                        {'mensaje': f'⚠️ Orden ya abierta en Binance para {symbol}', 'operation_id': operation_id},
                    )
                log_decision(
                    log,
                    'abrir',
                    operation_id,
                    entrada_log,
                    {'duplicada': True},
                    'reject',
                    {'reason': 'already_open'},
                )
                return False
            
            if self.modo_real:
                try:
                    cliente = obtener_cliente()
                    balance = cliente.fetch_balance()
                    quote = symbol.split('/')[1]
                    disponible = balance.get('free', {}).get(quote, 0.0)
                except Exception as e:
                    log.error(f'❌ Error obteniendo balance: {e}')
                    disponible = 0.0
                notional = precio * cantidad
                if not remainder_executable(symbol, precio, cantidad) or notional > disponible:
                    if self.bus:
                        await self.bus.publish(
                            'notify', {'mensaje': 'insuficiente', 'tipo': 'WARNING', 'operation_id': operation_id}
                        )
                    registrar_buy_rejected_insufficient_funds()
                    registrar_orden('rejected')
                    log_decision(
                        log,
                        'abrir',
                        operation_id,
                        entrada_log,
                        {
                            'saldo_disponible': disponible,
                            'notional': notional,
                        },
                        'reject',
                        {'reason': 'insufficient_funds'},
                    )
                    return False

            self.abriendo.add(symbol)
            try:
                objetivo = objetivo if objetivo is not None else cantidad
                orden = Order(
                    symbol=symbol,
                    precio_entrada=precio,
                    cantidad=objetivo,
                    cantidad_abierta=cantidad,
                    stop_loss=sl,
                    take_profit=tp,
                    estrategias_activas=estrategias,
                    tendencia=tendencia,
                    timestamp=datetime.now(UTC).isoformat(),
                    max_price=precio,
                    direccion=direccion,
                    entradas=[{'precio': precio, 'cantidad': cantidad}],
                    fracciones_totales=fracciones,
                    fracciones_restantes=max(fracciones - 1, 0),
                    precio_ultima_piramide=precio,
                    puntaje_entrada=puntaje,
                    umbral_entrada=umbral,
					score_tecnico=score_tecnico,
                    detalles_tecnicos=detalles_tecnicos,
                    break_even_activado=False,
                    duracion_en_velas=0,
                    registro_pendiente=True,
                    operation_id=operation_id,
                )
                self.ordenes[symbol] = orden

                if self.bus:
                    estrategias_txt = ', '.join(estrategias.keys())
                    msg_pendiente = (
                        f"""📝 Compra creada (pendiente de registro) {symbol}
                        Precio: {precio:.2f} Cantidad: {cantidad}
                        SL: {sl:.2f} TP: {tp:.2f}
                        Estrategias: {estrategias_txt}"""
                    )
                    await self.bus.publish('notify', {'mensaje': msg_pendiente, 'operation_id': operation_id})

                try:
                    if self.modo_real and is_valid_number(cantidad) and cantidad > 0:
                        ejecutado, fee, pnl = await self._ejecutar_market_retry('buy', symbol, cantidad, operation_id)
                        # actualiza con lo realmente ejecutado
                        cantidad = float(ejecutado)
                        orden.fee_total = getattr(orden, 'fee_total', 0.0) + fee
                        orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) + pnl
                        if cantidad <= 0:
                            if self.bus:
                                await self.bus.publish(
                                    'notify',
                                    {
                                        'mensaje': f'❌ Orden real no ejecutada en {symbol}',
                                        'tipo': 'CRITICAL',
                                        'operation_id': operation_id,
                                    },
                                )
                            registrar_orden('rejected')
                            log_decision(
                                log,
                                'abrir',
                                operation_id,
                                entrada_log,
                                {'ejecucion': 'sin_fills'},
                                'reject',
                                {'reason': 'no_fills'},
                            )
                            return
                    else:
                        # Simulado: el “coste” inicial lo cargamos como PnL negativo hasta el cierre
                        orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) - (precio * cantidad)

                    if cantidad > 0:
                        if self.modo_real:
                            registrado = False
                            last_error: Exception | None = None
                            for _ in range(3):
                                try:
                                    await asyncio.to_thread(
                                        real_orders.registrar_orden,
                                        symbol,
                                        precio,
                                        cantidad,
                                        sl,
                                        tp,
                                        estrategias,
                                        tendencia,
                                        direccion,
                                        operation_id,
                                    )
                                    registrado = True
                                    break
                                except Exception as e:
                                    last_error = e
                                    registrar_registro_error()
                                    log.error(
                                        'Error registrando orden',
                                        extra={'symbol': symbol, 'error': str(e)},
                                    )
                            if not registrado:
                                msg = (
                                    str(last_error)
                                    if last_error is not None
                                    else 'Error desconocido'
                                )
                                log.error(
                                    'Error registrando orden',
                                    extra={'symbol': symbol, 'error': msg},
                                )
                                if self.bus:
                                    await self.bus.publish(
                                        'notify',
                                        {
                                            'mensaje': f'❌ Error registrando orden {symbol}: {msg}',
                                            'tipo': 'CRITICAL',
                                            'operation_id': operation_id,
                                        },
                                    )
                            await asyncio.sleep(1)

                            if registrado:
                                orden.registro_pendiente = False
                            else:
                                registrar_orden('failed')  # mantenemos etiqueta por compatibilidad
                                if self.bus:
                                    await self.bus.publish(
                                        'notify',
                                        {
                                            'mensaje': f'⚠️ Orden {symbol} ejecutada pero registro pendiente',
                                            'tipo': 'WARNING',
                                        },
                                    )
                                # programa reintento rápido además del loop periódico
                                self._schedule_registro_retry(symbol)

                            orden.cantidad_abierta = cantidad
                            orden.entradas[0]['cantidad'] = cantidad
                        
                        else:
                            if self.bus:
                                await self.bus.publish(
                                    'orden_simulada_creada',
                                    {
                                        'symbol': symbol,
                                        'precio': precio,
                                        'cantidad': cantidad,
                                        'sl': sl,
                                        'tp': tp,
                                        'estrategias': estrategias,
                                        'direccion': direccion,
                                        'operation_id': operation_id,
                                        'orden': orden.to_parquet_record(),
                                    },
                                )
                            orden.registro_pendiente = False

                except Exception as e:
                    log.error(f'❌ No se pudo abrir la orden para {symbol}: {e}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {'mensaje': f'❌ Error al abrir orden en {symbol}: {e}', 'tipo': 'CRITICAL'},
                        )
                    self.ordenes.pop(symbol, None)
                    registrar_orden('failed')
                    return False

            finally:
                self.abriendo.discard(symbol)
                self._dup_warned.discard(symbol)

            if orden.registro_pendiente:
                log.warning(f'⚠️ Orden {symbol} pendiente de registro')
                log_decision(
                    log,
                    'abrir',
                    operation_id,
                    entrada_log,
                    {'registro': 'pendiente'},
                    'reject',
                    {'reason': 'registro_pendiente'},
                )
                return False

            registrar_orden('opened')
            log.info(f'🟢 Orden abierta para {symbol} @ {precio:.2f}')
            if self.bus and self.modo_real:
                estrategias_txt = ', '.join(estrategias.keys())
                mensaje = (
                    f"""🟢 Compra {symbol}\nPrecio: {precio:.2f} Cantidad: {cantidad}\nSL: {sl:.2f} TP: {tp:.2f}\nEstrategias: {estrategias_txt}"""
                )
                await self.bus.publish('notify', {'mensaje': mensaje, 'operation_id': operation_id})

            log_decision(
                log,
                'abrir',
                operation_id,
                entrada_log,
                {'validaciones': 'ok'},
                'accept',
                {'cantidad': cantidad},
            )
            return True
				
    async def agregar_parcial_async(self, symbol: str, precio: float, cantidad: float) -> bool:
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            """Aumenta la posición abierta agregando una compra parcial."""
            orden = self.ordenes.get(symbol)
            if not orden:
                return False
            
            operation_id = self._generar_operation_id(symbol)

            if self.modo_real:
                try:
                    if cantidad > 0:
                        ejecutado, fee, pnl = await self._ejecutar_market_retry('buy', symbol, cantidad, operation_id)
                        cantidad = ejecutado
                        orden.fee_total = getattr(orden, 'fee_total', 0.0) + fee
                        orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) + pnl
                except Exception as e:
                    log.error(f'❌ No se pudo agregar posición real para {symbol}: {e}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'❌ Error al agregar posición en {symbol}: {e}',
                                'tipo': 'CRITICAL',
                                'operation_id': operation_id,
                            },
                        )
                    return False
            else:
                # Simulado: costea la compra al PnL
                orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) - (precio * cantidad)

            total_prev = orden.cantidad_abierta
            orden.cantidad_abierta += cantidad
            orden.cantidad += cantidad

            if orden.cantidad > 0:
                orden.precio_entrada = ((orden.precio_entrada * total_prev) + (precio * cantidad)) / orden.cantidad
            else:
                orden.precio_entrada = precio  # fallback defensivo

            orden.max_price = max(getattr(orden, 'max_price', precio), precio)
            if not getattr(orden, 'entradas', None):
                orden.entradas = []
            orden.entradas.append({'precio': precio, 'cantidad': cantidad})
            orden.precio_ultima_piramide = precio
            return True

    async def cerrar_async(self, symbol: str, precio: float, motivo: str) -> bool:
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            """Cierra la orden indicada completamente."""
            orden = self.ordenes.get(symbol)
            if not orden:
                log.warning(f'⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}')
                return False
            if getattr(orden, 'cerrando', False):
                if symbol not in self._dup_warned:
                    log.warning(f'⚠️ Orden duplicada evitada para {symbol}')
                    self._dup_warned.add(symbol)
                return False

            orden.cerrando = True
            operation_id = self._generar_operation_id(symbol)
            entrada_log = {'symbol': symbol, 'precio': precio, 'cantidad': orden.cantidad}
            try:
                venta_exitosa = True

                if self.modo_real:
                    venta_exitosa = False
                    cantidad = orden.cantidad if is_valid_number(orden.cantidad) else 0.0

                    if cantidad > 1e-08:
                        try:
                            ejecutado, fee, pnl = await self._ejecutar_market_retry('sell', symbol, cantidad, operation_id)
                            restante = cantidad - ejecutado

                            if ejecutado > 0:
                                orden.fee_total = getattr(orden, 'fee_total', 0.0) + fee
                                orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) + pnl

                            if restante > 0 and not remainder_executable(symbol, precio, restante):
                                log.info(f'♻️ Resto no ejecutable para {symbol}: {restante}')
                                venta_exitosa = True
                                motivo += '|non_executable_remainder'
                            elif ejecutado > 0 and restante <= 1e-08:
                                venta_exitosa = True
                            elif ejecutado > 0 and restante > 0:
                                log.error(f'❌ Venta parcial pendiente ejecutable para {symbol}: restante={restante}')
                                real_orders.registrar_venta_fallida(symbol)
                            else:
                                log.error(f'❌ Venta no ejecutada para {symbol} (sin fills)')
                                real_orders.registrar_venta_fallida(symbol)

                            if self.bus and not venta_exitosa:
                                await self.bus.publish('notify', {'mensaje': f'❌ Venta fallida en {symbol}', 'operation_id': operation_id})
                        except Exception as e:
                            log.error(f'❌ Error al cerrar orden real en {symbol}: {e}')
                            if self.bus:
                                await self.bus.publish('notify', {'mensaje': f'❌ Venta fallida en {symbol}: {e}', 'operation_id': operation_id})
                else:
                    # Modo simulado: calcula PnL por diferencia
                    diff = (precio - orden.precio_entrada) * orden.cantidad
                    if orden.direccion in ('short', 'venta'):
                        diff = -diff
                    orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) + diff

                # Si la venta no fue exitosa, no alteramos estado de cierre ni borramos la orden
                if not venta_exitosa:
                    if self.bus and self.modo_real:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'⚠️ Venta no realizada, se reintentará en {symbol}',
                                'operation_id': operation_id,
                            },
                        )
                    log_decision(log, 'cerrar', operation_id, entrada_log, {'venta_exitosa': False}, 'reject', {'reason': 'venta_no_realizada'})
                    return False

                # Venta exitosa: cerrar y registrar
                orden.precio_cierre = precio
                orden.fecha_cierre = datetime.now(UTC).isoformat()
                orden.motivo_cierre = motivo

                base = orden.precio_entrada * orden.cantidad if orden.cantidad else 0.0
                retorno = (orden.pnl_operaciones / base) if base else 0.0
                orden.retorno_total = retorno

                self.historial.setdefault(symbol, []).append(orden.to_dict())
                if len(self.historial[symbol]) > self.max_historial:
                    self.historial[symbol] = self.historial[symbol][-self.max_historial:]

                if retorno < 0 and self.bus:
                    await self.bus.publish('registrar_perdida', {'symbol': symbol, 'perdida': retorno})

                log.info(f'📤 Orden cerrada para {symbol} @ {precio:.2f} | {motivo}')
                if self.bus:
                    if self.modo_real:
                        mensaje = (
                            f"""📤 Venta {symbol}\nEntrada: {orden.precio_entrada:.2f} Salida: {precio:.2f}\nRetorno: {retorno * 100:.2f}%\nMotivo: {motivo}"""
                        )
                        await self.bus.publish('notify', {'mensaje': mensaje, 'operation_id': operation_id})
                    else:
                        await self.bus.publish(
                            'orden_simulada_cerrada',
                            {
                                'symbol': symbol,
                                'precio_cierre': precio,
                                'retorno': retorno,
                                'motivo': motivo,
                                'operation_id': operation_id,
                            },
                        )

                log_decision(log, 'cerrar', operation_id, entrada_log, {'venta_exitosa': True}, 'accept', {'retorno': retorno})

                registrar_orden('closed')

                # Finalmente, elimina del activo
                self.ordenes.pop(symbol, None)
                return True

            finally:
                orden.cerrando = False
                self._dup_warned.discard(symbol)

    async def cerrar_parcial_async(self, symbol: str, cantidad: float, precio: float, motivo: str) -> bool:
        """Cierra parcialmente la orden activa."""
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        if lock.locked():
            registrar_partial_close_collision(symbol)
            log.warning(
                'Cierre parcial concurrente; se encola segundo intento',
                extra={'symbol': symbol},
            )
        async with lock:
            orden = self.ordenes.get(symbol)
            order_id = getattr(orden, 'operation_id', 'N/A') if orden else 'N/A'
            log.debug(
                'Enter cerrar_parcial lock',
                extra={'symbol': symbol, 'order_id': order_id},
            )
            try:
                if not orden or orden.cantidad_abierta <= 0:
                    log.warning(
                        'Se intentó cierre parcial sin orden activa',
                        extra={'symbol': symbol},
                    )
                    return False

                cantidad = min(cantidad, orden.cantidad_abierta)
                if cantidad < 1e-08:
                    log.warning(
                        'Cantidad demasiado pequeña para vender',
                        extra={'symbol': symbol, 'cantidad': cantidad},
                    )
                    return False

                operation_id = self._generar_operation_id(symbol)
				
                if self.modo_real:
                    try:
                        ejecutado, fee, pnl = await self._ejecutar_market_retry('sell', symbol, cantidad, operation_id)
                        cantidad = ejecutado
                        orden.fee_total = getattr(orden, 'fee_total', 0.0) + fee
                        orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) + pnl
                    except Exception as e:
                        log.error(
                            'Error en venta parcial',
                            extra={'symbol': symbol, 'error': str(e)},
                        )
                        if self.bus:
                            await self.bus.publish('notify', {'mensaje': f'❌ Venta parcial fallida en {symbol}: {e}', 'operation_id': operation_id})
                        log_decision(log, 'cerrar_parcial', operation_id, {'symbol': symbol, 'cantidad': cantidad}, {}, 'reject', {'reason': str(e)})
                        return False
                else:
                    diff = (precio - orden.precio_entrada) * cantidad
                    if orden.direccion in ('short', 'venta'):
                        diff = -diff
                    orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) + diff

                orden.cantidad_abierta -= cantidad

                log.info(f'📤 Cierre parcial de {symbol}: {cantidad} @ {precio:.2f} | {motivo}')
                if self.bus:
                    mensaje = f"""📤 Venta parcial {symbol}\nCantidad: {cantidad}\nPrecio: {precio:.2f}\nMotivo: {motivo}"""
                    await self.bus.publish('notify', {'mensaje': mensaje, 'operation_id': operation_id})

                if orden.cantidad_abierta <= 0:
                    orden.precio_cierre = precio
                    orden.fecha_cierre = datetime.now(UTC).isoformat()
                    orden.motivo_cierre = motivo
                    base = orden.precio_entrada * orden.cantidad if orden.cantidad else 0.0
                    retorno = (orden.pnl_operaciones / base) if base else 0.0
                    orden.retorno_total = retorno
                    self.historial.setdefault(symbol, []).append(orden.to_dict())
                    if len(self.historial[symbol]) > self.max_historial:
                        self.historial[symbol] = self.historial[symbol][-self.max_historial:]
                    if retorno < 0 and self.bus:
                        await self.bus.publish('registrar_perdida', {'symbol': symbol, 'perdida': retorno})
                    log.info(f'📤 Orden cerrada para {symbol} @ {precio:.2f} | {motivo}')
                    if self.bus:
                        if self.modo_real:
                            mensaje = (
                                f"""📤 Venta {symbol}\nEntrada: {orden.precio_entrada:.2f} Salida: {precio:.2f}\nRetorno: {retorno * 100:.2f}%\nMotivo: {motivo}"""
                            )
                            await self.bus.publish('notify', {'mensaje': mensaje, 'operation_id': operation_id})
                        else:
                            await self.bus.publish(
                                'orden_simulada_cerrada',
                                {
                                    'symbol': symbol,
                                    'precio_cierre': precio,
                                    'retorno': retorno,
                                    'motivo': motivo,
                                    'operation_id': operation_id,
                                },
                            )
                    self.ordenes.pop(symbol, None)

                    if self.modo_real:
                        try:
                            await asyncio.to_thread(real_orders.eliminar_orden, symbol)
                        except Exception as e:
                            log.error(f'❌ Error eliminando orden {symbol} de SQLite: {e}')
                    registrar_orden('closed')
                    log_decision(log, 'cerrar_parcial', operation_id, {'symbol': symbol, 'cantidad': cantidad}, {}, 'accept', {'retorno': retorno})
                else:
                    registrar_orden('partial')
                    log_decision(log, 'cerrar_parcial', operation_id, {'symbol': symbol, 'cantidad': cantidad}, {}, 'accept', {'parcial': True})
                return True
            finally:
                log.debug(f'🔓 Exit cerrar_parcial lock {symbol} id={order_id}')

    def obtener(self, symbol: str) -> Optional[Order]:
        return self.ordenes.get(symbol)

    async def crear(
        self,
        *,
        symbol: str,
        side: str,
        precio: float,
        sl: float,
        tp: float,
        meta: Mapping[str, Any] | None = None,
    ) -> bool:
        """Crea y envía una orden utilizando la interfaz moderna del Trader."""

        meta_map: Mapping[str, Any]
        if isinstance(meta, Mapping):
            meta_map = meta
        else:
            meta_map = {}

        direccion = str(side or "long").lower()
        if direccion in {"sell", "venta"}:
            direccion = "short"
        elif direccion not in {"long", "short"}:
            direccion = "long"

        estrategias_value = _lookup_meta(
            meta_map,
            ("estrategias", "estrategias_activas", "strategies", "estrategias_detalle"),
        )
        estrategias = estrategias_value if isinstance(estrategias_value, dict) else {}

        tendencia_value = _lookup_meta(meta_map, ("tendencia", "trend", "market_trend"))
        tendencia = str(tendencia_value) if tendencia_value is not None else ""

        cantidad = _resolve_quantity(meta_map, precio)
        if cantidad <= 0:
            log.warning(
                "crear.skip_quantity",
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "side": direccion,
                        "precio": precio,
                        "meta_keys": tuple(meta_map.keys()),
                    }
                ),
            )
            return False

        puntaje_val = _lookup_meta(meta_map, ("score", "puntaje", "score_tecnico"))
        puntaje = _coerce_float(puntaje_val) or 0.0

        umbral_val = _lookup_meta(meta_map, ("umbral_score", "umbral", "threshold"))
        umbral = _coerce_float(umbral_val) or 0.0

        score_tecnico_val = _lookup_meta(
            meta_map,
            ("score_tecnico", "score_tecnico_final", "score_ajustado"),
        )
        score_tecnico = _coerce_float(score_tecnico_val) or puntaje

        objetivo_val = _lookup_meta(meta_map, ("objetivo", "cantidad_objetivo", "target_amount"))
        objetivo = _coerce_float(objetivo_val)

        fracciones_val = _lookup_meta(meta_map, ("fracciones", "fracciones_totales", "chunks"))
        fracciones_int = _coerce_int(fracciones_val)
        fracciones = fracciones_int if fracciones_int and fracciones_int > 0 else 1

        detalles_value = _lookup_meta(
            meta_map,
            ("detalles_tecnicos", "detalles", "technical_details"),
        )
        detalles_tecnicos = detalles_value if isinstance(detalles_value, dict) else None

        tick_size_val = _lookup_meta(
            meta_map,
            ("tick_size", "precio_tick", "tick"),
        )
        step_size_val = _lookup_meta(
            meta_map,
            ("step_size", "cantidad_step", "lot_step", "step"),
        )
        min_dist_val = _lookup_meta(
            meta_map,
            ("min_dist_pct", "min_dist", "min_distance_pct"),
        )

        tick_size = _coerce_float(tick_size_val)
        step_size = _coerce_float(step_size_val)
        min_dist_pct = _coerce_float(min_dist_val)

        filters_value = _lookup_meta(meta_map, ("market_filters", "filtros", "filters"))
        if isinstance(filters_value, Mapping):
            if tick_size is None:
                tick_size = _coerce_float(filters_value.get("tick_size"))
            if step_size is None:
                step_size = _coerce_float(filters_value.get("step_size"))
            if min_dist_pct is None:
                min_dist_pct = _coerce_float(filters_value.get("min_dist_pct"))

        candle_close_val = _lookup_meta(
            meta_map,
            ("candle_close_ts", "timestamp", "bar_close_ts", "close_ts"),
        )
        candle_close_ts = _coerce_int(candle_close_val)

        strategy_version_val = _lookup_meta(
            meta_map,
            ("strategy_version", "version", "pipeline_version", "engine_version"),
        )
        strategy_version = str(strategy_version_val) if strategy_version_val is not None else None

        try:
            return await self.abrir_async(
                symbol,
                precio,
                sl,
                tp,
                estrategias,
                tendencia or "",
                direccion=direccion,
                cantidad=cantidad,
                puntaje=puntaje,
                umbral=umbral,
                score_tecnico=score_tecnico,
                objetivo=objetivo,
                fracciones=fracciones,
                detalles_tecnicos=detalles_tecnicos,
                tick_size=tick_size,
                step_size=step_size,
                min_dist_pct=min_dist_pct,
                candle_close_ts=candle_close_ts,
                strategy_version=strategy_version,
            )
        except Exception as exc:  # pragma: no cover - defensivo
            log.exception(
                "crear.exception",
                extra=safe_extra({"symbol": symbol, "side": direccion, "error": str(exc)}),
            )
            return False

    def eliminar(self, symbol: str) -> None:
        """Elimina la orden local asociada a ``symbol`` si existe."""

        self.ordenes.pop(symbol, None)

    def actualizar(self, orden: Order | None, **kwargs: Any) -> None:
        """Actualiza los atributos de ``orden`` con los ``kwargs`` provistos."""

        if orden is None:
            return
        for key, value in kwargs.items():
            try:
                setattr(orden, key, value)
            except Exception:  # pragma: no cover - defensivo
                continue
