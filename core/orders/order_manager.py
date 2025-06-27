"""Gestión de órdenes simuladas o reales."""

from __future__ import annotations

import asyncio
import time
from typing import Dict, Optional
from datetime import datetime, timezone

from core.orders.order_model import Order
from core.utils.logger import configurar_logger
from core.orders import real_orders
from core.utils.utils import is_valid_number  # ✅ validación segura
from core.config import COMISION, SLIPPAGE

log = configurar_logger("orders", modo_silencioso=True)


class OrderManager:
    """Abstrae la creación y cierre de órdenes."""

    def __init__(self, modo_real: bool, risk=None, notificador=None) -> None:
        self.modo_real = modo_real
        self.ordenes: Dict[str, Order] = {}
        self.historial: Dict[str, list] = {}
        self.risk = risk
        self.notificador = notificador

    async def _run_with_timeout(self, func, *args, timeout: float = 10.0):
        """Ejecuta ``func`` en un thread con ``timeout`` y mide su duración."""
        inicio = time.monotonic()
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(func, *args), timeout=timeout
            )
        finally:
            duracion = time.monotonic() - inicio
            if duracion > 5:
                log.warning(
                    f"⏱️ {func.__name__} para {args[0]} tardó {duracion:.2f}s"
                )

    # ------------------------------------------------------------------
    # Operaciones de apertura
    # ------------------------------------------------------------------

    async def abrir_async(
        self,
        symbol: str,
        precio: float,
        sl: float,
        tp: float,
        estrategias: Dict,
        tendencia: str,
        direccion: str = "long",
        cantidad: float = 0.0,
        puntaje: float = 0.0,
        umbral: float = 0.0,
        objetivo: float | None = None,
        fracciones: int = 1,
        detalles_tecnicos: dict | None = None,
    ) -> None:
        """Registra una nueva orden en memoria y/o en Binance."""
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
            timestamp=datetime.now(timezone.utc).isoformat(),
            max_price=precio,
            direccion=direccion,
            entradas=[{"precio": precio, "cantidad": cantidad}],
            fracciones_totales=fracciones,
            fracciones_restantes=max(fracciones - 1, 0),
            precio_ultima_piramide=precio,
            puntaje_entrada=puntaje,
            umbral_entrada=umbral,
            detalles_tecnicos=detalles_tecnicos,
            break_even_activado=False,
            duracion_en_velas=0,
        )

        try:
            if self.modo_real and is_valid_number(cantidad) and cantidad > 0:
                cantidad = await asyncio.to_thread(
                    real_orders.ejecutar_orden_market, symbol, cantidad
                )
                try:
                    cantidad = float(cantidad)
                except (TypeError, ValueError):
                    cantidad = float(orden.cantidad_abierta) if is_valid_number(orden.cantidad_abierta) else 0.0

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
                orden.entradas[0]["cantidad"] = cantidad
        except Exception as e:
            log.error(f"❌ No se pudo abrir la orden para {symbol}: {e}")
            raise

        self.ordenes[symbol] = orden
        log.info(f"🟢 Orden abierta para {symbol} @ {precio:.2f}")
        if self.notificador:
            estrategias_txt = ", ".join(estrategias.keys())
            mensaje = (
                f"🟢 Compra {symbol}\n"
                f"Precio: {precio:.2f} Cantidad: {cantidad}\n"
                f"SL: {sl:.2f} TP: {tp:.2f}\n"
                f"Estrategias: {estrategias_txt}"
            )
            try:
                await self.notificador.enviar_async(mensaje)
            except Exception as e:
                log.error(f"❌ Error enviando notificación: {e}", exc_info=True)

    def abrir(self, *args, **kwargs) -> None:
        """Versión síncrona de :meth:`abrir_async`."""
        asyncio.run(self.abrir_async(*args, **kwargs))

    async def agregar_parcial_async(self, symbol: str, precio: float, cantidad: float) -> bool:
        """Aumenta la posición abierta agregando una compra parcial."""
        orden = self.ordenes.get(symbol)
        if not orden:
            return False

        if self.modo_real:
            try:
                if cantidad > 0:
                    await asyncio.to_thread(
                        real_orders.ejecutar_orden_market,
                        symbol,
                        cantidad,
                    )
            except Exception as e:
                log.error(f"❌ No se pudo agregar posición real para {symbol}: {e}")
                return False

        total_prev = orden.cantidad_abierta + 0.0
        orden.cantidad_abierta += cantidad
        orden.cantidad += cantidad
        orden.precio_entrada = (
            orden.precio_entrada * total_prev + precio * cantidad
        ) / orden.cantidad
        orden.max_price = max(orden.max_price, precio)
        if orden.entradas is None:
            orden.entradas = []
        orden.entradas.append({"precio": precio, "cantidad": cantidad})
        return True

    def agregar_parcial(self, *args, **kwargs) -> bool:
        return asyncio.run(self.agregar_parcial_async(*args, **kwargs))

    # ------------------------------------------------------------------
    # Operaciones de cierre completo
    # ------------------------------------------------------------------

    async def cerrar_async(self, symbol: str, precio: float, motivo: str) -> bool:
        """Cierra la orden indicada completamente."""
        orden = self.ordenes.get(symbol)
        if not orden:
            log.warning(f"⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}")
            return False

        try:
            if self.modo_real:
                cantidad = orden.cantidad if is_valid_number(orden.cantidad) else 0.0
                if cantidad > 1e-8:
                    await self._run_with_timeout(
                        real_orders.ejecutar_orden_market_sell,
                        symbol,
                        cantidad,
                        timeout=10.0,
                    )
        except Exception as e:
            log.exception(f"❌ No se pudo cerrar la orden real para {symbol}: {e}")
            if self.notificador:
                try:
                    await self.notificador.enviar_async(f"❌ Venta fallida en {symbol}: {e}")
                except Exception as err:
                    log.error(f"❌ Error enviando notificación: {err}")
        finally:
            try:
                await self._run_with_timeout(
                    real_orders.eliminar_orden, symbol, timeout=10.0
                )
            except Exception as e:
                log.exception(
                    f"❌ Error consultando o eliminando orden abierta: {e}"
                )

        self.ordenes.pop(symbol, None)
        orden.precio_cierre = precio
        orden.fecha_cierre = datetime.now(timezone.utc).isoformat()
        orden.motivo_cierre = motivo
        
        direccion = 1 if orden.direccion in ("long", "compra") else -1
        capital_invertido = orden.precio_entrada * (orden.cantidad or 0.0)
        capital_simbolo = capital_invertido
        retorno = (
            (precio - orden.precio_entrada) / orden.precio_entrada
            if orden.precio_entrada
            else 0.0
        )
        retorno_total = retorno * direccion * (
            capital_invertido / capital_simbolo if capital_simbolo else 0.0
        )
        retorno_total -= COMISION * 2 + SLIPPAGE
        orden.retorno_total = retorno_total
        self.historial.setdefault(symbol, []).append(orden.to_dict())

        if retorno_total < 0 and self.risk is not None:
            try:
                self.risk.registrar_perdida(symbol, retorno_total)
            except Exception as e:
                log.warning(f"⚠️ No se pudo registrar pérdida para {symbol}: {e}")

        log.info(f"📤 Orden cerrada para {symbol} @ {precio:.2f} | {motivo}")
        if self.notificador:
            mensaje = (
                f"📤 Venta {symbol}\n"
                f"Entrada: {orden.precio_entrada:.2f} Salida: {precio:.2f}\n"
                f"Retorno: {retorno_total * 100:.2f}%\n"
                f"Motivo: {motivo}"
            )
            try:
                await self.notificador.enviar_async(mensaje)
            except Exception as e:
                log.error(f"❌ Error enviando notificación: {e}")
        log.info(f"✔️ Venta procesada correctamente para {symbol}")
        return True

    def cerrar(self, *args, **kwargs) -> bool:
        """Versión síncrona de :meth:`cerrar_async`."""
        return asyncio.run(self.cerrar_async(*args, **kwargs))

    # ------------------------------------------------------------------
    # Cierre parcial
    # ------------------------------------------------------------------

    async def cerrar_parcial_async(
        self, symbol: str, cantidad: float, precio: float, motivo: str
    ) -> bool:
        """Cierra parcialmente la orden activa."""
        orden = self.ordenes.get(symbol)
        if not orden or orden.cantidad_abierta <= 0:
            log.warning(f"⚠️ Se intentó cierre parcial sin orden activa en {symbol}")
            return False

        cantidad = min(cantidad, orden.cantidad_abierta)

        if cantidad < 1e-8:
            log.warning(f"⚠️ Cantidad demasiado pequeña para vender: {cantidad}")
            return False

        if self.modo_real:
            try:
                await self._run_with_timeout(
                    real_orders.ejecutar_orden_market_sell,
                    symbol,
                    cantidad,
                    timeout=10.0,
                )
            except Exception as e:
                log.exception(f"❌ Error en venta parcial de {symbol}: {e}")
                if self.notificador:
                    try:
                        await self.notificador.enviar_async(f"❌ Venta parcial fallida en {symbol}: {e}")
                    except Exception as err:
                        log.error(f"❌ Error enviando notificación: {err}")
                return False

        orden.cantidad_abierta -= cantidad
        direccion = 1 if orden.direccion in ("long", "compra") else -1
        capital_invertido = orden.precio_entrada * cantidad
        capital_simbolo = orden.precio_entrada * (orden.cantidad or 0.0)
        retorno_unitario = (
            (precio - orden.precio_entrada) / orden.precio_entrada
            if orden.precio_entrada
            else 0.0
        )
        retorno_total = retorno_unitario * direccion * (
            capital_invertido / capital_simbolo if capital_simbolo else 0.0
        )
        retorno_total -= COMISION * 2 + SLIPPAGE
        if retorno_total < 0 and self.risk is not None:
            try:
                self.risk.registrar_perdida(symbol, retorno_total)
            except Exception as e:
                log.warning(f"⚠️ No se pudo registrar pérdida parcial para {symbol}: {e}")
        log.info(f"📤 Cierre parcial de {symbol}: {cantidad} @ {precio:.2f} | {motivo}")
        if self.notificador:
            mensaje = (
                f"📤 Venta parcial {symbol}\n"
                f"Cantidad: {cantidad}\n"
                f"Precio: {precio:.2f}\n"
                f"Motivo: {motivo}"
            )
            try:
                await self.notificador.enviar_async(mensaje)
            except Exception as e:
                log.error(f"❌ Error enviando notificación: {e}")
        log.info(f"✔️ Venta procesada correctamente para {symbol}")

        if orden.cantidad_abierta <= 0:
            self.ordenes.pop(symbol, None)
        return True

    def cerrar_parcial(self, *args, **kwargs) -> bool:
        """Versión síncrona de :meth:`cerrar_parcial_async`."""
        return asyncio.run(self.cerrar_parcial_async(*args, **kwargs))

    # ------------------------------------------------------------------
    # Obtener orden
    # ------------------------------------------------------------------

    def obtener(self, symbol: str) -> Optional[Order]:
        return self.ordenes.get(symbol)
