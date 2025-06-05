import pandas as pd
import asyncio

from core.config_manager import Config
from core.data_feed import DataFeed
from core.strategy_engine import StrategyEngine
from core.risk_manager import RiskManager
from core.order_manager import OrderManager
from core.logger import configurar_logger


log = configurar_logger("trader")


class Trader:
    """Orquestador principal que utiliza componentes desacoplados."""

    def __init__(self, config: Config):
        self.config = config
        self.data_feed = DataFeed(config.intervalo_velas)
        self.engine = StrategyEngine()
        self.risk = RiskManager(config.umbral_riesgo_diario)
        self.orders = OrderManager()
        self._tasks = []

    async def ejecutar(self):
        for symbol in self.config.symbols:
            self._tasks.append(asyncio.create_task(self._procesar_symbol(symbol)))
        await asyncio.gather(*self._tasks)

    async def _procesar_symbol(self, symbol: str):
        async def handle(candle: dict):
            df = pd.DataFrame([candle])
            puntajes = self.engine.evaluar_entrada(df)
            if puntajes:
                self.orders.registrar_orden(symbol, candle["close"], 0, 0, 0, puntajes, "")
        await self.data_feed.stream(symbol, handle)

    async def cerrar(self):
        for t in self._tasks:
            t.cancel()