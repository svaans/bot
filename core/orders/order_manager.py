"""Gestión de órdenes simuladas o reales."""
from __future__ import annotations

import asyncio
from typing import Dict, Optional
from datetime import datetime, timezone
import os

from core.orders.order_model import Order
from core.utils.logger import configurar_logger, log_decision
from core.orders import real_orders
from core.orders.validators import remainder_executable
from core.utils.utils import is_valid_number
from core.event_bus import EventBus
from core.metrics import (
    registrar_orden,
    registrar_buy_rejected_insufficient_funds,
	registrar_partial_close_collision,
)
from core.registro_metrico import registro_metrico
from binance_api.cliente import obtener_cliente

log = configurar_logger('orders', modo_silencioso=True)
UTC = timezone.utc

MAX_HISTORIAL_ORDENES = 1000


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
            resp = await asyncio.to_thread(
                real_orders._market_sell_retry, symbol, cantidad, operation_id
            )
            salida = {
                'ejecutado': float(resp.get('ejecutado', 0.0)),
                'fee': float(resp.get('fee', 0.0)),
                'pnl': float(resp.get('pnl', 0.0)),
            }
            log_decision(log, '_market_sell', operation_id, entrada, {}, 'execute', salida)
            return salida['ejecutado'], salida['fee'], salida['pnl']
        restante = cantidad
        total = total_fee = total_pnl = 0.0
        while restante > 0:
            resp = await asyncio.to_thread(
                real_orders.ejecutar_orden_market, symbol, restante, operation_id
            )
            ejecutado = float(resp.get('ejecutado', 0.0))
            total += ejecutado
            restante = float(resp.get('restante', 0.0))
            total_fee += float(resp.get('fee', 0.0))
            total_pnl += float(resp.get('pnl', 0.0))
            if resp.get('status') != 'PARTIAL' or restante < resp.get('min_qty', 0):
                break
        salida = {'ejecutado': total, 'fee': total_fee, 'pnl': total_pnl}
        log_decision(log, '_market_buy', operation_id, entrada, {}, 'execute', salida)
        return total, total_fee, total_pnl

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
        if self._sync_task is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if loop.is_running():
            # Solo arrancamos el loop; dentro se hace un _sync_once inicial.
            self._sync_task = loop.create_task(self._sync_loop())

    async def _sync_once(self) -> None:
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


    async def _sync_loop(self) -> None:
        while True:
            await self._sync_once()
            await asyncio.sleep(self._sync_interval)

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
        *,
        candle_close_ts: int | None = None,   # reservado (no usado aquí)
        strategy_version: str | None = None,  # reservado (no usado aquí)
    ) -> bool:
        operation_id = self._generar_operation_id(symbol)
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
                                    log.error(f'❌ Error registrando orden {symbol}: {e}')
                            if self.bus:
                                await self.bus.publish(
                                    'notify',
                                    {
                                        'mensaje': f'❌ Error registrando orden {symbol}: {e}',
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
                                real_orders._VENTAS_FALLIDAS.add(symbol)
                            else:
                                log.error(f'❌ Venta no ejecutada para {symbol} (sin fills)')
                                real_orders._VENTAS_FALLIDAS.add(symbol)

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
            log.warning(f'⚠️ Cierre parcial concurrente en {symbol}, se encola el segundo intento')
        async with lock:
            orden = self.ordenes.get(symbol)
            order_id = getattr(orden, 'operation_id', 'N/A') if orden else 'N/A'
            log.debug(f'🔒 Enter cerrar_parcial lock {symbol} id={order_id}')
            try:
                if not orden or orden.cantidad_abierta <= 0:
                    log.warning(f'⚠️ Se intentó cierre parcial sin orden activa en {symbol}')
                    return False

                cantidad = min(cantidad, orden.cantidad_abierta)
                if cantidad < 1e-08:
                    log.warning(f'⚠️ Cantidad demasiado pequeña para vender: {cantidad}')
                    return False

                operation_id = self._generar_operation_id(symbol)
				
                if self.modo_real:
                    try:
                        ejecutado, fee, pnl = await self._ejecutar_market_retry('sell', symbol, cantidad, operation_id)
                        cantidad = ejecutado
                        orden.fee_total = getattr(orden, 'fee_total', 0.0) + fee
                        orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) + pnl
                    except Exception as e:
                        log.error(f'❌ Error en venta parcial de {symbol}: {e}')
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
