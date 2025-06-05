import pandas as pd
import asyncio
from datetime import datetime
import os

from core.config_manager import Config
from core.data_feed import DataFeed
from core.strategy_engine import StrategyEngine
from core.risk_manager import RiskManager
from core.order_manager import OrderManager
from core.logger import configurar_logger
from core.adaptador_umbral import calcular_umbral_adaptativo, calcular_tp_sl_adaptativos

log = configurar_logger("trader")


class Trader:
    """Orquestador principal que utiliza componentes desacoplados."""

    def __init__(self, config: Config):
        self.config = config
        self.data_feed = DataFeed(config.intervalo_velas)
        self.engine = StrategyEngine()
        self.orders = OrderManager()
        self.buffers = {s: [] for s in config.symbols}
        self._tasks = []

    async def ejecutar(self):
        for symbol in self.config.symbols:
            self._tasks.append(asyncio.create_task(self._procesar_symbol(symbol)))
        await asyncio.gather(*self._tasks)

    async def _procesar_symbol(self, symbol: str):
        async def handle(candle: dict):
            buf = self.buffers[symbol]
            buf.append(candle)
            if len(buf) > 50:
                self.buffers[symbol] = buf[-50:]
            if len(buf) < 30:
                return
            df = pd.DataFrame(self.buffers[symbol])
            evaluacion = self.engine.evaluar_entrada(symbol, df)
            if not evaluacion:
                return
            puntaje = evaluacion.get("puntaje_total", 0)
            estrategias = evaluacion.get("estrategias_activas", {})
            umbral = calcular_umbral_adaptativo(symbol, df, estrategias, {})
            if puntaje < umbral:
                return
            sl, tp = calcular_tp_sl_adaptativos(df, float(candle["close"]))
            self.orders.registrar_orden(symbol, float(candle["close"]), 0, sl, tp, estrategias, "")
        await self.data_feed.stream(symbol, handle)

    async def cerrar(self):
        for t in self._tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        await asyncio.gather(*self._tasks, return_exceptions=True)