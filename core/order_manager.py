from typing import Dict
from core.logger import configurar_logger
from core import ordenes_reales


log = configurar_logger("orders", modo_silencioso=True)


class OrderManager:
    """Gestiona el registro de órdenes."""

    def registrar_orden(self, symbol: str, precio: float, cantidad: float, sl: float, tp: float, estrategias: Dict, tendencia: str) -> None:
        ordenes_reales.registrar_orden(symbol, precio, cantidad, sl, tp, estrategias, tendencia)

    def cerrar_orden(self, symbol: str, info: Dict) -> None:
        ordenes_reales.eliminar_orden(symbol)
        ordenes_reales.registrar_orden(symbol, **info)