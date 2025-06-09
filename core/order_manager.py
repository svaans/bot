"""Gestión de órdenes simuladas o reales."""

from __future__ import annotations

from dataclasses import asdict
from typing import Dict, Optional
from datetime import datetime

from core.ordenes_model import Orden
from core.logger import configurar_logger
from core import ordenes_reales


log = configurar_logger("orders", modo_silencioso=True)


class OrderManager:
    """Abstrae la creación y cierre de órdenes."""

    def __init__(self, modo_real: bool, risk=None, notificador=None) -> None:
        self.modo_real = modo_real
        self.ordenes: Dict[str, Orden] = {}
        self.risk = risk
        self.notificador = notificador

    # ------------------------------------------------------------------
    # Operaciones de apertura
    # ------------------------------------------------------------------
    def abrir(
        self,
        symbol: str,
        precio: float,
        sl: float,
        tp: float,
        estrategias: Dict,
        tendencia: str,
        direccion: str = "long",
        cantidad: float = 0.0,
        
    ) -> None:
        """Registra una nueva orden en memoria y, si ``modo_real`` es ``True``
        ejecuta la orden en Binance."""
        orden = Orden(
            symbol=symbol,
            precio_entrada=precio,
            cantidad=cantidad,
            stop_loss=sl,
            take_profit=tp,
            estrategias_activas=estrategias,
            tendencia=tendencia,
            timestamp=datetime.utcnow().isoformat(),
            max_price=precio,
            direccion=direccion,
        )
        
        if self.modo_real:
            try:
                if cantidad > 0:
                    cantidad = ordenes_reales.ejecutar_orden_market(symbol, cantidad)
                ordenes_reales.registrar_orden(
                    symbol, precio, cantidad, sl, tp, estrategias, tendencia, direccion
                )
                orden.cantidad = cantidad
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
    # ------------------------------------------------------------------
    # Operaciones de cierre
    # ------------------------------------------------------------------
    def cerrar(self, symbol: str, precio: float, motivo: str) -> bool:
        """Cierra la orden indicada y la elimina del registro.

        Returns ``True`` si la orden existía y fue cerrada correctamente,
        ``False`` en caso contrario."""
        orden = self.ordenes.get(symbol)
        if not orden:
            log.warning(
                f"⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}"
            )
            return False
        if self.modo_real:
            try:
                if orden.cantidad > 0:
                    ordenes_reales.ejecutar_orden_market_sell(symbol, orden.cantidad)
            except Exception as e:
                log.error(f"❌ No se pudo cerrar la orden real para {symbol}: {e}")
                if self.notificador:
                    self.notificador.enviar(
                        f"❌ Venta fallida en {symbol}: {e}"
                    )
                return False
            if ordenes_reales.obtener_orden(symbol) is not None:
                ordenes_reales.eliminar_orden(symbol)
            info = asdict(orden)
            ordenes_reales.registrar_orden(
                symbol,
                info["precio_entrada"],
                info["cantidad"],
                info["stop_loss"],
                info["take_profit"],
                info["estrategias_activas"],
                info["tendencia"],
                info.get("direccion", "long"),
            )
        
        self.ordenes.pop(symbol, None)
        orden.precio_cierre = precio
        orden.fecha_cierre = datetime.utcnow().isoformat()
        orden.motivo_cierre = motivo
        retorno = (
            (precio - orden.precio_entrada) / orden.precio_entrada
            if orden.precio_entrada
            else 0.0
        )
        if retorno < 0 and self.risk is not None:
            try:
                self.risk.registrar_perdida(symbol, retorno)
            except Exception as e:
                log.warning(f"⚠️ No se pudo registrar pérdida para {symbol}: {e}")
                raise
            
        log.info(f"📤 Orden cerrada para {symbol} @ {precio:.2f} | {motivo}")
        if self.notificador:
            retorno_pct = retorno * 100
            mensaje = (
                f"📤 Venta {symbol}\n"
                f"Entrada: {orden.precio_entrada:.2f} Salida: {precio:.2f}\n"
                f"Retorno: {retorno_pct:.2f}%\n"
                f"Motivo: {motivo}"
            )
            self.notificador.enviar(mensaje)
        return True

    def obtener(self, symbol: str) -> Optional[Orden]:
        return self.ordenes.get(symbol)
