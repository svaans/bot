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
from binance_api.cliente import crear_cliente
from core.adaptador_umbral import calcular_tp_sl_adaptativos, calcular_umbral_adaptativo
from core.logger import configurar_logger
from core.monitor_estado_bot import monitorear_estado_periodicamente
from core import ordenes_reales
from core.adaptador_configuracion import configurar_parametros_dinamicos


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
        self.cliente = crear_cliente(config)
        self.estado: Dict[str, EstadoSimbolo] = {s: EstadoSimbolo([]) for s in config.symbols}
        self.config_por_simbolo: Dict[str, dict] = {s: {} for s in config.symbols}
        self._task: asyncio.Task | None = None
        self._task_estado: asyncio.Task | None = None

        try:
            self.orders.ordenes = ordenes_reales.obtener_todas_las_ordenes()
        except Exception as e:
            log.warning(f"⚠️ Error cargando órdenes previas: {e}")

        if self.orders.ordenes:
            log.warning("⚠️ Órdenes abiertas encontradas al iniciar. Serán monitoreadas.")

    @property
    def ordenes_abiertas(self):
        """Compatibilidad con ``monitorear_estado_periodicamente``."""
        return self.orders.ordenes
    
    def _calcular_cantidad(self, precio: float) -> float:
        """Determina la cantidad de cripto a comprar de forma simplificada."""
        balance = self.cliente.fetch_balance()
        euros = balance['total'].get('EUR', 0)
        if euros <= 0:
            return 0.0
        euros_por_simbolo = euros / max(len(self.estado), 1)
        euros_compra = euros_por_simbolo * 0.95
        cantidad = round(euros_compra / precio, 6)
        return cantidad if cantidad * precio >= 10 else 0.0

    def _abrir_operacion_real(self, symbol: str, precio: float, sl: float, tp: float, estrategias: Dict) -> None:
        cantidad = self._calcular_cantidad(precio)
        if cantidad <= 0:
            log.warning(f"❌ No se pudo calcular cantidad válida para {symbol}")
            return
        self.orders.abrir(symbol, precio, sl, tp, estrategias, "", cantidad)

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
        config_actual = configurar_parametros_dinamicos(
            symbol, df, self.config_por_simbolo.get(symbol, {})
        )
        self.config_por_simbolo[symbol] = config_actual
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

        balance = self.cliente.fetch_balance()
        capital_total = balance['total'].get('EUR', 0)
        if self.risk.riesgo_superado(capital_total):
            log.warning(f"🚫 Riesgo diario superado para {symbol}")
            return

        sl, tp = calcular_tp_sl_adaptativos(df, float(vela["close"]))
        precio = float(vela["close"])
        if self.config.modo_real:
            self._abrir_operacion_real(symbol, precio, sl, tp, estrategias)
        else:
            self.orders.abrir(symbol, precio, sl, tp, estrategias, "")

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