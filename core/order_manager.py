"""Gestión de órdenes simuladas o reales."""

from __future__ import annotations

import asyncio
from typing import Dict, Optional
from datetime import datetime

from core.ordenes_model import Orden
from core.logger import configurar_logger
from core import ordenes_reales
from core.utils import is_valid_number  # ✅ validación segura

log = configurar_logger("orders", modo_silencioso=True)


class OrderManager:
    """Abstrae la creación y cierre de órdenes."""

    def __init__(self, modo_real: bool, risk=None, notificador=None) -> None:
        self.modo_real = modo_real
        self.ordenes: Dict[str, Orden] = {}
        self.historial: Dict[str, list] = {}
        self.risk = risk
        self.notificador = notificador

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
    ) -> None:
        """Registra una nueva orden en memoria y/o en Binance."""
        objetivo = objetivo if objetivo is not None else cantidad
        orden = Orden(
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
            entradas=[{"precio": precio, "cantidad": cantidad}],
            fracciones_totales=fracciones,
            fracciones_restantes=max(fracciones - 1, 0),
            precio_ultima_piramide=precio,
            puntaje_entrada=puntaje,
            umbral_entrada=umbral,
        )

        if self.modo_real:
            try:
                if is_valid_number(cantidad) and cantidad > 0:
                    cantidad = await asyncio.to_thread(
                        ordenes_reales.ejecutar_orden_market, symbol, cantidad
                    )
                    try:
                        cantidad = float(cantidad)
                    except Exception:
                        cantidad = 0.0
                await asyncio.to_thread(
                    ordenes_reales.registrar_orden,
                    symbol,
                    precio,
                    cantidad,
                    sl,
                    tp,
                    estrategias,
                    tendencia,
                    direccion,
                )
                orden.cantidad_abierta = cantidad
                orden.entradas[0]["cantidad"] = cantidad
            except Exception as e:
                log.error(f"❌ No se pudo abrir la orden real para {symbol}: {e}")
                return

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
            self.notificador.enviar(mensaje)

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
                        ordenes_reales.ejecutar_orden_market,
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

        if self.modo_real:
            try:
                cantidad = orden.cantidad if is_valid_number(orden.cantidad) else 0.0
                if cantidad > 1e-8:
                    await asyncio.to_thread(
                        ordenes_reales.ejecutar_orden_market_sell,
                        symbol,
                        cantidad,
                    )
            except Exception as e:
                log.error(f"❌ No se pudo cerrar la orden real para {symbol}: {e}")
                if self.notificador:
                    self.notificador.enviar(f"❌ Venta fallida en {symbol}: {e}")
            finally:
                try:
                    await asyncio.to_thread(ordenes_reales.eliminar_orden, symbol)
                except Exception as e:
                    log.error(f"❌ Error consultando o eliminando orden abierta: {e}")

        self.ordenes.pop(symbol, None)
        orden.precio_cierre = precio
        orden.fecha_cierre = datetime.utcnow().isoformat()
        orden.motivo_cierre = motivo

        retorno = (
            (precio - orden.precio_entrada) / orden.precio_entrada
            if orden.precio_entrada
            else 0.0
        )
        orden.retorno_total = retorno
        self.historial.setdefault(symbol, []).append(orden.to_dict())

        if retorno < 0 and self.risk is not None:
            try:
                self.risk.registrar_perdida(symbol, retorno)
            except Exception as e:
                log.warning(f"⚠️ No se pudo registrar pérdida para {symbol}: {e}")

        log.info(f"📤 Orden cerrada para {symbol} @ {precio:.2f} | {motivo}")
        if self.notificador:
            mensaje = (
                f"📤 Venta {symbol}\n"
                f"Entrada: {orden.precio_entrada:.2f} Salida: {precio:.2f}\n"
                f"Retorno: {retorno * 100:.2f}%\n"
                f"Motivo: {motivo}"
            )
            self.notificador.enviar(mensaje)
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
                await asyncio.to_thread(
                    ordenes_reales.ejecutar_orden_market_sell, symbol, cantidad
                )
            except Exception as e:
                log.error(f"❌ Error en venta parcial de {symbol}: {e}")
                if self.notificador:
                    self.notificador.enviar(f"❌ Venta parcial fallida en {symbol}: {e}")
                return False

        orden.cantidad_abierta -= cantidad
        retorno_unitario = (
            (precio - orden.precio_entrada) / orden.precio_entrada
            if orden.precio_entrada
            else 0.0
        )
        fraccion = cantidad / orden.cantidad if orden.cantidad else 0.0
        retorno_total = retorno_unitario * fraccion
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
            self.notificador.enviar(mensaje)

        if orden.cantidad_abierta <= 0:
            self.ordenes.pop(symbol, None)
        return True

    def cerrar_parcial(self, *args, **kwargs) -> bool:
        """Versión síncrona de :meth:`cerrar_parcial_async`."""
        return asyncio.run(self.cerrar_parcial_async(*args, **kwargs))

    # ------------------------------------------------------------------
    # Obtener orden
    # ------------------------------------------------------------------

    def obtener(self, symbol: str) -> Optional[Orden]:
        return self.ordenes.get(symbol)
