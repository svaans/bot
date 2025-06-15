from __future__ import annotations

from core.config_manager import Config
from core.trader_modular import Trader

class TraderSimulado(Trader):
    """Replica la lógica de :class:`Trader` pero siempre en modo simulado."""
    def __init__(self, config: Config) -> None:
        config.modo_real = False
        super().__init__(config)
        # En simulación no se envían notificaciones reales
        self.notificador = None
        self.orders.notificador = None

    async def procesar_vela(self, vela: dict) -> None:
        """Procesa una vela manualmente para backtesting o simulación."""
        await self._procesar_vela(vela)