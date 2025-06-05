from typing import Dict
from core.logger import configurar_logger
from core import ordenes_reales


log = configurar_logger("orders", modo_silencioso=True)


class OrderManager:
    """Gestiona el registro y ejecución de órdenes."""

    def registrar_orden(self, symbol: str, precio: float, cantidad: float, sl: float, tp: float, estrategias: Dict, tendencia: str) -> None:
        """Guarda una nueva orden abierta."""
        ordenes_reales.registrar_orden(symbol, precio, cantidad, sl, tp, estrategias, tendencia)

    def cerrar_orden(self, symbol: str, info: Dict) -> None:
        """Elimina y vuelve a registrar una orden ya cerrada."""
        ordenes_reales.eliminar_orden(symbol)
        ordenes_reales.registrar_orden(symbol, **info)

    def ejecutar_market_buy(self, symbol: str, cantidad: float):
        """Ejecuta una orden real de mercado usando el cliente configurado."""
        return ordenes_reales.ejecutar_orden_market(symbol, cantidad)