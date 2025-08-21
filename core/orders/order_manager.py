"""Gestión de órdenes simuladas o reales."""
from __future__ import annotations
import asyncio
from typing import Dict, Optional
from datetime import datetime
import os
from core.orders.order_model import Order
from core.utils.logger import configurar_logger
from core.orders import real_orders, place_order
from core.utils.utils import is_valid_number
from core.event_bus import EventBus
from core.metrics import registrar_orden
log = configurar_logger('orders', modo_silencioso=True)

MAX_HISTORIAL_ORDENES = 1000


class OrderManager:
    """Abstrae la creación y cierre de órdenes."""

    def __init__(
        self,
        modo_real: bool,
        bus: EventBus | None = None,
        max_historial: int = MAX_HISTORIAL_ORDENES,
    ) -> None:
        log.info('➡️ Entrando en __init__()')
        self.modo_real = modo_real
        self.ordenes: Dict[str, Order] = {}
        self.historial: Dict[str, list] = {}
        self.bus = bus
        self.max_historial = max_historial
        self.abriendo: set[str] = set()
        self._locks: Dict[str, asyncio.Lock] = {}
        self._sync_task: asyncio.Task | None = None
        self._sync_interval = 300
        if bus:
            self.subscribe(bus)

    def _generar_operation_id(self, symbol: str) -> str:
        """Genera un identificador único para agrupar fills de una operación."""
        nonce = os.urandom(2).hex()
        ts = int(datetime.utcnow().timestamp() * 1000)
        return f"{symbol.replace('/', '')}-{ts}-{nonce}"

    async def _ejecutar_market_retry(
        self,
        side: str,
        symbol: str,
        cantidad: float,
        operation_id: str | None = None,
    ) -> tuple[float, float, float]:
        """Envía una orden de mercado reintentando en caso de fills parciales.

        Retorna una tupla ``(ejecutado, fee, pnl)`` acumulando los valores de
        cada intento.
        """
        operation_id = operation_id or self._generar_operation_id(symbol)
        restante = cantidad
        total = total_fee = total_pnl = 0.0
        while restante > 0:
            func = (
                real_orders.ejecutar_orden_market
                if side == 'buy'
                else real_orders.ejecutar_orden_market_sell
            )
            resp = await asyncio.to_thread(func, symbol, restante, operation_id)
            ejecutado = float(resp.get('ejecutado', 0.0))
            total += ejecutado
            restante = float(resp.get('restante', 0.0))
            total_fee += float(resp.get('fee', 0.0))
            total_pnl += float(resp.get('pnl', 0.0))
            if resp.get('status') != 'PARTIAL' or restante < resp.get('min_qty', 0):
                break
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
            loop.create_task(self._sync_once())
            self._sync_task = loop.create_task(self._sync_loop())

    async def _sync_once(self) -> None:
        try:
            ordenes_binance = await asyncio.to_thread(
                real_orders.sincronizar_ordenes_binance
            )
        except Exception as e:
            log.error(f'❌ Error sincronizando órdenes: {e}')
            return
        for sym, ord_ in ordenes_binance.items():
            if sym not in self.ordenes:
                self.ordenes[sym] = ord_
                if self.bus:
                    await self.bus.publish(
                        'notify',
                        {
                            'mensaje': f'🔄 Orden sincronizada desde Binance: {sym}',
                            'tipo': 'WARNING',
                        },
                    )
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
                    )
                    ord_.registro_pendiente = False
                    registrar_orden('opened')
                    log.info(f'🟢 Orden registrada tras reintento para {sym}')
                    if self.bus:
                        estrategias_txt = ', '.join(ord_.estrategias_activas.keys())
                        mensaje = (
                            f"""🟢 Compra {sym}\nPrecio: {ord_.precio_entrada:.2f} Cantidad: {ord_.cantidad_abierta or ord_.cantidad}\nSL: {ord_.stop_loss:.2f} TP: {ord_.take_profit:.2f}\nEstrategias: {estrategias_txt}"""
                        )
                        await self.bus.publish('notify', {'mensaje': mensaje})
                except Exception as e:
                    log.error(f'❌ Error registrando orden pendiente {sym}: {e}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'❌ Error registrando orden pendiente {sym}: {e}',
                                'tipo': 'CRITICAL',
                            },
                        )
                        
    async def _sync_loop(self) -> None:
        while True:
            await self._sync_once()
            await asyncio.sleep(self._sync_interval)

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
        objetivo: float | None = None,
        fracciones: int = 1,
        detalles_tecnicos: dict | None = None,
        *,
        candle_close_ts: int | None = None,
        strategy_version: str | None = None,
    ) -> None:
        log.info('➡️ Entrando en abrir_async()')
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            if symbol in self.abriendo or symbol in self.ordenes:
                log.warning(f'⚠️ Orden duplicada evitada para {symbol}')
                return
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            if symbol in self.abriendo or symbol in self.ordenes:
                log.warning(f'⚠️ Orden duplicada evitada para {symbol}')
                try:
                    ordenes_api = await asyncio.to_thread(
                        real_orders.sincronizar_ordenes_binance, [symbol]
                    )
                except Exception as e:
                    log.error(f'❌ Error verificando órdenes abiertas: {e}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'⚠️ No se pudo verificar órdenes abiertas en {symbol}',
                                'tipo': 'WARNING',
                            },
                        )
                        return
                if symbol in ordenes_api:
                    self.ordenes[symbol] = ordenes_api[symbol]
                    log.warning(f'⚠️ Orden existente en Binance para {symbol}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'⚠️ Orden ya abierta en Binance para {symbol}',
                            },
                        )
                    return
            self.abriendo.add(symbol)
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
                timestamp=datetime.utcnow().isoformat(),
                max_price=precio,
                direccion=direccion,
                entradas=[{'precio': precio, 'cantidad': cantidad}],
                fracciones_totales=fracciones,
                fracciones_restantes=max(fracciones - 1, 0),
                precio_ultima_piramide=precio,
                puntaje_entrada=puntaje,
                umbral_entrada=umbral,
                detalles_tecnicos=detalles_tecnicos,
                break_even_activado=False,
                duracion_en_velas=0,
                registro_pendiente=True,
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
                await self.bus.publish('notify', {'mensaje': msg_pendiente})
            try:
                if self.modo_real and is_valid_number(cantidad) and cantidad > 0:
                    ejecutado, fee, pnl = await self._ejecutar_market_retry(
                        'buy', symbol, cantidad
                    )
                    try:
                        cantidad = float(ejecutado)
                    except Exception:
                        cantidad = (
                            float(orden.cantidad_abierta)
                            if is_valid_number(orden.cantidad_abierta)
                            else 0.0
                        )
                    orden.fee_total = fee
                    orden.pnl_operaciones = pnl
                    if cantidad <= 0 and self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'❌ Orden real no ejecutada en {symbol}',
                                'tipo': 'CRITICAL',
                            },
                        )
                        registrar_orden('rejected')
                        return
                else:
                    orden.pnl_operaciones = -precio * cantidad
                if cantidad > 0:
                    registrado = False
                    for intento in range(3):
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
                                    },
                                )
                            await asyncio.sleep(1)
                    if registrado:
                        orden.registro_pendiente = False
                    else:
                        registrar_orden('failed')
                        if self.bus:
                            await self.bus.publish(
                                'notify',
                                {
                                    'mensaje': f'⚠️ Orden {symbol} ejecutada pero registro pendiente',
                                    'tipo': 'WARNING',
                                },
                            )
                if self.modo_real:
                    orden.cantidad_abierta = cantidad
                    orden.entradas[0]['cantidad'] = cantidad
            except Exception as e:
                log.error(f'❌ No se pudo abrir la orden para {symbol}: {e}')
                if self.bus:
                    await self.bus.publish(
                        'notify',
                        {
                            'mensaje': f'❌ Error al abrir orden en {symbol}: {e}',
                            'tipo': 'CRITICAL',
                        },
                    )
                self.ordenes.pop(symbol, None)
                registrar_orden('failed')
                return
            finally:
                self.abriendo.discard(symbol)
            if orden.registro_pendiente:
                log.warning(f'⚠️ Orden {symbol} pendiente de registro')
                return
            registrar_orden('opened')
            log.info(f'🟢 Orden abierta para {symbol} @ {precio:.2f}')
            if self.bus:
                estrategias_txt = ', '.join(estrategias.keys())
                mensaje = (
                    f"""🟢 Compra {symbol}
Precio: {precio:.2f} Cantidad: {cantidad}
SL: {sl:.2f} TP: {tp:.2f}
Estrategias: {estrategias_txt}"""
                )
                await self.bus.publish('notify', {'mensaje': mensaje})

    async def agregar_parcial_async(self, symbol: str, precio: float,
        cantidad: float) -> bool:
        log.info('➡️ Entrando en agregar_parcial_async()')
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            """Aumenta la posición abierta agregando una compra parcial."""
            orden = self.ordenes.get(symbol)
            if not orden:
                return False
            if self.modo_real:
                try:
                    if cantidad > 0:
                        ejecutado, fee, pnl = await self._ejecutar_market_retry('buy', symbol, cantidad)
                        cantidad = ejecutado
                        orden.fee_total += fee
                        orden.pnl_operaciones += pnl
                        orden.cantidad_abierta = max(0.0, orden.cantidad_abierta - ejecutado)
                    
                except Exception as e:
                    log.error(
                        f'❌ No se pudo agregar posición real para {symbol}: {e}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'❌ Error al agregar posición en {symbol}: {e}',
                                'tipo': 'CRITICAL',
                            },
                    )
                    return False
            else:
                orden.pnl_operaciones -= precio * cantidad
            total_prev = orden.cantidad_abierta + 0.0
            orden.cantidad_abierta += cantidad
            orden.cantidad += cantidad
            orden.precio_entrada = (orden.precio_entrada * total_prev + precio *
                cantidad) / orden.cantidad
            orden.max_price = max(orden.max_price, precio)
            if orden.entradas is None:
                orden.entradas = []
            orden.entradas.append({'precio': precio, 'cantidad': cantidad})
            orden.precio_ultima_piramide = precio
            return True

    async def cerrar_async(self, symbol: str, precio: float, motivo: str
        ) -> bool:
        log.info('➡️ Entrando en cerrar_async()')
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            """Cierra la orden indicada completamente."""
            orden = self.ordenes.get(symbol)
            if not orden:
                log.warning(
                    f'⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}')
                return False
            if getattr(orden, 'cerrando', False):
                log.warning(f'⚠️ Cierre duplicado ignorado para {symbol}')
                return False
            orden.cerrando = True
            venta_exitosa = True
            if self.modo_real:
                venta_exitosa = False
                cantidad = orden.cantidad if is_valid_number(orden.cantidad) else 0.0
                if cantidad > 1e-08:
                    try:
                        ejecutado, fee, pnl = await self._ejecutar_market_retry('sell', symbol, cantidad)
                        if ejecutado and ejecutado > 0:
                            venta_exitosa = True
                            orden.fee_total += fee
                            orden.pnl_operaciones += pnl
                        else:
                            log.error(
                                f'❌ Venta no ejecutada o cantidad 0 para {symbol}'
                            )
                            real_orders._VENTAS_FALLIDAS.add(symbol)
                    except Exception as e:
                        log.error(
                            f'❌ No se pudo cerrar la orden real para {symbol}: {e}'
                        )
                        real_orders._VENTAS_FALLIDAS.add(symbol)
                        if self.bus:
                            await self.bus.publish(
                                'notify',
                                {'mensaje': f'❌ Venta fallida en {symbol}: {e}'},
                            )
                else:
                    # en modo simulado calculamos PnL con el precio indicado
                    diff = (precio - orden.precio_entrada) * orden.cantidad
                    if orden.direccion in ('short', 'venta'):
                        diff = -diff
                    orden.pnl_operaciones += diff
            if not venta_exitosa:
                if self.bus and self.modo_real:
                    await self.bus.publish(
                        'notify',
                        {'mensaje': f'⚠️ Venta no realizada, se reintentará en {symbol}'},
                    )
                registrar_orden('failed')
                return False
            try:
                await asyncio.to_thread(real_orders.eliminar_orden, symbol)
            except Exception as e:
                log.error(
                    f'❌ Error consultando o eliminando orden abierta: {e}'
                )
            self.ordenes.pop(symbol, None)
            orden.precio_cierre = precio
            orden.fecha_cierre = datetime.utcnow().isoformat()
            orden.motivo_cierre = motivo
            base = orden.precio_entrada * orden.cantidad if orden.cantidad else 0.0
            retorno = orden.pnl_operaciones / base if base else 0.0
            orden.retorno_total = retorno
            self.historial.setdefault(symbol, []).append(orden.to_dict())
            if len(self.historial[symbol]) > self.max_historial:
                self.historial[symbol] = self.historial[symbol][-self.max_historial:]
            if retorno < 0 and self.bus:
                await self.bus.publish('registrar_perdida', {'symbol': symbol, 'perdida': retorno})
            log.info(f'📤 Orden cerrada para {symbol} @ {precio:.2f} | {motivo}')
            if self.bus:
                mensaje = f"""📤 Venta {symbol}\nEntrada: {orden.precio_entrada:.2f} Salida: {precio:.2f}\nRetorno: {retorno * 100:.2f}%\nMotivo: {motivo}"""
                await self.bus.publish('notify', {'mensaje': mensaje})
            registrar_orden('closed')
            return True

    async def cerrar_parcial_async(self, symbol: str, cantidad: float,
        precio: float, motivo: str) ->bool:
        log.info('➡️ Entrando en cerrar_parcial_async()')
        """Cierra parcialmente la orden activa."""
        orden = self.ordenes.get(symbol)
        if not orden or orden.cantidad_abierta <= 0:
            log.warning(
                f'⚠️ Se intentó cierre parcial sin orden activa en {symbol}')
            return False
        cantidad = min(cantidad, orden.cantidad_abierta)
        if cantidad < 1e-08:
            log.warning(
                f'⚠️ Cantidad demasiado pequeña para vender: {cantidad}')
            return False
        if self.modo_real:
            try:
                ejecutado, fee, pnl = await self._ejecutar_market_retry('sell', symbol, cantidad)
                cantidad = ejecutado
                orden.fee_total += fee
                orden.pnl_operaciones += pnl
            except Exception as e:
                log.error(f'❌ Error en venta parcial de {symbol}: {e}')
                if self.bus:
                    await self.bus.publish('notify', {'mensaje': f'❌ Venta parcial fallida en {symbol}: {e}'})
                return False
        else:
            diff = (precio - orden.precio_entrada) * cantidad
            if orden.direccion in ('short', 'venta'):
                diff = -diff
            orden.pnl_operaciones += diff
        orden.cantidad_abierta -= cantidad
        retorno_unitario = (precio - orden.precio_entrada
            ) / orden.precio_entrada if orden.precio_entrada else 0.0
        fraccion = cantidad / orden.cantidad if orden.cantidad else 0.0
        retorno_total = retorno_unitario * fraccion
        log.info(
            f'📤 Cierre parcial de {symbol}: {cantidad} @ {precio:.2f} | {motivo}'
            )
        if self.bus:
            mensaje = f"""📤 Venta parcial {symbol}\nCantidad: {cantidad}\nPrecio: {precio:.2f}\nMotivo: {motivo}"""
            await self.bus.publish('notify', {'mensaje': mensaje})
        if orden.cantidad_abierta <= 0:
            self.ordenes.pop(symbol, None)
        registrar_orden('partial')
        return True

    def obtener(self, symbol: str) ->Optional[Order]:
        log.info('➡️ Entrando en obtener()')
        return self.ordenes.get(symbol)
