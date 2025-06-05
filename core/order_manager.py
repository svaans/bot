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

    def __init__(self, modo_real: bool) -> None:
        self.modo_real = modo_real
        self.ordenes: Dict[str, Orden] = {}

    # ------------------------------------------------------------------
    # Operaciones de apertura
    # ------------------------------------------------------------------
    def abrir(self, symbol: str, precio: float, sl: float, tp: float, estrategias: Dict, tendencia: str) -> None:
        """Registra una nueva orden en memoria y, si corresponde, en Binance."""
        orden = Orden(
            symbol=symbol,
            precio_entrada=precio,
            cantidad=0.0,
            stop_loss=sl,
            take_profit=tp,
            estrategias_activas=estrategias,
            tendencia=tendencia,
            timestamp=datetime.utcnow().isoformat(),
            max_price=precio,
        )
        self.ordenes[symbol] = orden
        if self.modo_real:
            ordenes_reales.registrar_orden(symbol, precio, 0.0, sl, tp, estrategias, tendencia)
        log.info(f"🟢 Orden abierta para {symbol} @ {precio:.2f}")
    # ------------------------------------------------------------------
    # Operaciones de cierre
    # ------------------------------------------------------------------
    def cerrar(self, symbol: str, precio: float, motivo: str) -> None:
        """Cierra la orden indicada y la elimina del registro."""
        orden = self.ordenes.pop(symbol, None)
        if not orden:
            return
        orden.precio_cierre = precio
        orden.fecha_cierre = datetime.utcnow().isoformat()
        orden.motivo_cierre = motivo
        if self.modo_real:
            info = asdict(orden)
            ordenes_reales.eliminar_orden(symbol)
            ordenes_reales.registrar_orden(symbol, **info)
        log.info(f"📤 Orden cerrada para {symbol} @ {precio:.2f} | {motivo}")

    def obtener(self, symbol: str) -> Optional[Orden]:
        return self.ordenes.get(symbol)