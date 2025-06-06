"""Controlador principal del bot modular."""

from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from core.config_manager import Config
from core.data_feed import DataFeed
from core.strategy_engine import StrategyEngine
from core.risk_manager import RiskManager
from core.order_manager import OrderManager
from core.adaptador_umbral import calcular_tp_sl_adaptativos, calcular_umbral_adaptativo
from core.logger import configurar_logger
from core.monitor_estado_bot import monitorear_estado_periodicamente

log = configurar_logger("trader")


@dataclass
class EstadoSimbolo:
    buffer: List[dict]
    ultimo_umbral: float = 0.0


class Trader:
    """Orquesta el flujo de datos y las operaciones de trading."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.data_feed = DataFeed(config.intervalo_velas)
        self.engine = StrategyEngine()
        self.risk = RiskManager(config.umbral_riesgo_diario)
        self.orders = OrderManager(config.modo_real)
        self.estado: Dict[str, EstadoSimbolo] = {s: EstadoSimbolo([]) for s in config.symbols}
        self._task: asyncio.Task | None = None

    @property
    def ordenes_abiertas(self):
        """Compatibilidad con ``monitorear_estado_periodicamente``."""
        return self.orders.ordenes

    async def ejecutar(self) -> None:
        """Inicia el procesamiento de todos los símbolos."""
        async def handle(candle: dict) -> None:
            await self._procesar_vela(candle)

        symbols = list(self.estado.keys())
        self._task = asyncio.create_task(self.data_feed.escuchar(symbols, handle))
        self._task_estado = asyncio.create_task(monitorear_estado_periodicamente(self))
        await asyncio.gather(self._task, self._task_estado)

    async def _procesar_vela(self, vela: dict) -> None:
        symbol = vela["symbol"]
        estado = self.estado[symbol]
        estado.buffer.append(vela)
        if len(estado.buffer) > 50:
            estado.buffer = estado.buffer[-50:]
        if len(estado.buffer) < 30:
            return

        df = pd.DataFrame(estado.buffer)
        evaluacion = self.engine.evaluar_entrada(symbol, df)
        if not evaluacion:
            return

        puntaje = evaluacion.get("puntaje_total", 0)
        estrategias = evaluacion.get("estrategias_activas", {})
        umbral = calcular_umbral_adaptativo(symbol, df, estrategias, {})
        estado.ultimo_umbral = umbral

        if puntaje < umbral:
            log.debug(f"🚫 {symbol}: puntaje {puntaje:.2f} < umbral {umbral:.2f}")
            return

        if self.risk.riesgo_superado(1.0):  # Capital total ficticio
            log.warning(f"🚫 Riesgo diario superado para {symbol}")
            return

        sl, tp = calcular_tp_sl_adaptativos(df, float(vela["close"]))
        self.orders.abrir(symbol, float(vela["close"]), sl, tp, estrategias, "")

    async def cerrar(self) -> None:
        if self._task:
            await self.data_feed.detener()
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._task_estado:
            self._task_estado.cancel()
            try:
                await self._task_estado
            except asyncio.CancelledError:
                pass