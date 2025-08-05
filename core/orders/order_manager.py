"""Gestión de órdenes simuladas o reales."""
from __future__ import annotations
import asyncio
from typing import Dict, Optional
from datetime import datetime
from core.orders.order_model import Order
from core.utils.logger import configurar_logger
from core.orders import real_orders
from core.utils.utils import is_valid_number
from core.event_bus import EventBus
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
        if bus:
            self.subscribe(bus)

    def subscribe(self, bus: EventBus) -> None:
        bus.subscribe('abrir_orden', self._on_abrir)
        bus.subscribe('cerrar_orden', self._on_cerrar)
        bus.subscribe('cerrar_parcial', self._on_cerrar_parcial)
        bus.subscribe('agregar_parcial', self._on_agregar_parcial)

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

    async def abrir_async(self, symbol: str, precio: float, sl: float, tp:
        float, estrategias: Dict, tendencia: str, direccion: str='long',
        cantidad: float=0.0, puntaje: float=0.0, umbral: float=0.0,
        objetivo: (float | None)=None, fracciones: int=1, detalles_tecnicos:
        (dict | None)=None) ->None:
        log.info('➡️ Entrando en abrir_async()')
        """Registra una nueva orden en memoria y/o en Binance."""
        objetivo = objetivo if objetivo is not None else cantidad
        orden = Order(symbol=symbol, precio_entrada=precio, cantidad=
            objetivo, cantidad_abierta=cantidad, stop_loss=sl, take_profit=
            tp, estrategias_activas=estrategias, tendencia=tendencia,
            timestamp=datetime.utcnow().isoformat(), max_price=precio,
            direccion=direccion, entradas=[{'precio': precio, 'cantidad':
            cantidad}], fracciones_totales=fracciones, fracciones_restantes
            =max(fracciones - 1, 0), precio_ultima_piramide=precio,
            puntaje_entrada=puntaje, umbral_entrada=umbral,
            detalles_tecnicos=detalles_tecnicos, break_even_activado=False,
            duracion_en_velas=0)
        try:
            if self.modo_real and is_valid_number(cantidad) and cantidad > 0:
                cantidad = await asyncio.to_thread(
                    real_orders.ejecutar_orden_market, symbol, cantidad
                )
                try:
                    cantidad = float(cantidad)
                except Exception:
                    cantidad = (
                        float(orden.cantidad_abierta)
                        if is_valid_number(orden.cantidad_abierta)
                        else 0.0
                    )
                if cantidad <= 0 and self.bus:
                    await self.bus.publish(
                        'notify',
                        {
                            'mensaje': f'❌ Orden real no ejecutada en {symbol}',
                            'tipo': 'CRITICAL',
                        },
                    )
                    return
            if cantidad > 0:
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
            return
        self.ordenes[symbol] = orden
        log.info(f'🟢 Orden abierta para {symbol} @ {precio:.2f}')
        if self.bus:
            estrategias_txt = ', '.join(estrategias.keys())
            mensaje = f"""🟢 Compra {symbol}\nPrecio: {precio:.2f} Cantidad: {cantidad}\nSL: {sl:.2f} TP: {tp:.2f}\nEstrategias: {estrategias_txt}"""
            await self.bus.publish('notify', {'mensaje': mensaje})

    async def agregar_parcial_async(self, symbol: str, precio: float,
        cantidad: float) ->bool:
        log.info('➡️ Entrando en agregar_parcial_async()')
        """Aumenta la posición abierta agregando una compra parcial."""
        orden = self.ordenes.get(symbol)
        if not orden:
            return False
        if self.modo_real:
            try:
                if cantidad > 0:
                    await asyncio.to_thread(real_orders.
                        ejecutar_orden_market, symbol, cantidad)
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
        ) ->bool:
        log.info('➡️ Entrando en cerrar_async()')
        """Cierra la orden indicada completamente."""
        orden = self.ordenes.get(symbol)
        if not orden:
            log.warning(
                f'⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}')
            return False
        venta_exitosa = True
        if self.modo_real:
            venta_exitosa = False
            cantidad = orden.cantidad if is_valid_number(orden.cantidad) else 0.0
            if cantidad > 1e-08:
                try:
                    ejecutado = await asyncio.to_thread(
                        real_orders.ejecutar_orden_market_sell, symbol, cantidad
                    )
                    if ejecutado and ejecutado > 0:
                        venta_exitosa = True
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
                venta_exitosa = True
        if not venta_exitosa:
            if self.bus and self.modo_real:
                await self.bus.publish(
                    'notify',
                    {'mensaje': f'⚠️ Venta no realizada, se reintentará en {symbol}'},
                )
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
        retorno = (precio - orden.precio_entrada
            ) / orden.precio_entrada if orden.precio_entrada else 0.0
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
                await asyncio.to_thread(real_orders.ejecutar_orden_market_sell, symbol, cantidad)
            except Exception as e:
                log.error(f'❌ Error en venta parcial de {symbol}: {e}')
                if self.bus:
                    await self.bus.publish('notify', {'mensaje': f'❌ Venta parcial fallida en {symbol}: {e}'})
                return False
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
        return True

    def obtener(self, symbol: str) ->Optional[Order]:
        log.info('➡️ Entrando en obtener()')
        return self.ordenes.get(symbol)
