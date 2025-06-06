"""Controlador principal del bot modular."""

from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime

import pandas as pd

from core.config_manager import Config
from core.data_feed import DataFeed
from core.strategy_engine import StrategyEngine
from core.risk_manager import RiskManager
from core.order_manager import OrderManager
from binance_api.cliente import crear_cliente
from core.adaptador_umbral import (
    calcular_tp_sl_adaptativos,
    calcular_umbral_adaptativo,
)
from core.pesos import cargar_pesos_estrategias
from aprendizaje.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from core.logger import configurar_logger
from core.monitor_estado_bot import monitorear_estado_periodicamente
from core import ordenes_reales
from core.adaptador_configuracion import configurar_parametros_dinamicos
from core.reporting import reporter_diario
from estrategias_salida.salida_trailing_stop import verificar_trailing_stop
from estrategias_salida.salida_por_tendencia import verificar_reversion_tendencia
from estrategias_salida.gestor_salidas import evaluar_salidas, verificar_filtro_tecnico
from estrategias_salida.salida_stoploss import salida_stoploss
from filtros.filtro_salidas import validar_necesidad_de_salida
from core.tendencia import detectar_tendencia


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
        self.pesos_por_simbolo: Dict[str, Dict[str, float]] = cargar_pesos_estrategias()
        self._task: asyncio.Task | None = None
        self._task_estado: asyncio.Task | None = None

        try:
            self.orders.ordenes = ordenes_reales.obtener_todas_las_ordenes()
        except Exception as e:
            log.warning(f"⚠️ Error cargando órdenes previas: {e}")

        if self.orders.ordenes:
            log.warning("⚠️ Órdenes abiertas encontradas al iniciar. Serán monitoreadas.")

    def cerrar_operacion(self, symbol: str, precio: float, motivo: str) -> None:
        """Cierra una orden y actualiza los pesos si corresponden."""
        self.orders.cerrar(symbol, precio, motivo)
        actualizar_pesos_estrategias_symbol(symbol)
        self.pesos_por_simbolo = cargar_pesos_estrategias()

    def _cerrar_y_reportar(self, orden, precio: float, motivo: str) -> None:
        """Cierra ``orden`` y registra la operación para el reporte diario."""
        retorno_total = (
            (precio - orden.precio_entrada) / orden.precio_entrada
            if orden.precio_entrada
            else 0.0
        )
        info = orden.to_dict()
        info.update(
            {
                "precio_cierre": precio,
                "fecha_cierre": datetime.utcnow().isoformat(),
                "motivo_cierre": motivo,
                "retorno_total": retorno_total,
            }
        )
        self.orders.cerrar(orden.symbol, precio, motivo)
        reporter_diario.registrar_operacion(info)
        actualizar_pesos_estrategias_symbol(orden.symbol)
        self.pesos_por_simbolo = cargar_pesos_estrategias()

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

    def _verificar_salidas(self, symbol: str, df: pd.DataFrame) -> None:
        """Evalúa si la orden abierta en ``symbol`` debe cerrarse."""
        orden = self.orders.obtener(symbol)
        if not orden:
            return

        precio_min = float(df["low"].iloc[-1])
        precio_max = float(df["high"].iloc[-1])
        precio_cierre = float(df["close"].iloc[-1])
        config_actual = self.config_por_simbolo.get(symbol, {})

        # --- Stop Loss con validación ---
        if precio_min <= orden.stop_loss:
            resultado = salida_stoploss(orden.to_dict(), df, config=config_actual)
            if resultado.get("cerrar", False):
                self._cerrar_y_reportar(orden, orden.stop_loss, "Stop Loss")
            else:
                log.info(f"🛡️ SL evitado para {symbol} → {resultado.get('razon', '')}")
            return

        # --- Take Profit ---
        if precio_max >= orden.take_profit:
            self._cerrar_y_reportar(orden, precio_max, "Take Profit")
            return

        # --- Trailing Stop ---
        if precio_cierre > orden.max_price:
            orden.max_price = precio_cierre

        config_actual = configurar_parametros_dinamicos(symbol, df, config_actual)
        self.config_por_simbolo[symbol] = config_actual

        cerrar, motivo = verificar_trailing_stop(orden.to_dict(), precio_cierre, config=config_actual)
        if cerrar:
            self._cerrar_y_reportar(orden, precio_cierre, motivo)
            return

        # --- Cambio de tendencia ---
        if verificar_reversion_tendencia(symbol, df, orden.tendencia):
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            if not verificar_filtro_tecnico(symbol, df, orden.estrategias_activas, pesos_symbol):
                self._cerrar_y_reportar(orden, precio_cierre, "Cambio de tendencia")
                return

        # --- Estrategias de salida personalizadas ---
        resultado = evaluar_salidas(orden.to_dict(), df, config=config_actual)
        if resultado.get("cerrar", False):
            razon = resultado.get("razon", "Estrategia desconocida")
            evaluacion = self.engine.evaluar_entrada(symbol, df)
            estrategias = evaluacion.get("estrategias_activas", {})
            puntaje = evaluacion.get("puntaje_total", 0)
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            umbral = calcular_umbral_adaptativo(symbol, df, estrategias, pesos_symbol)
            if not validar_necesidad_de_salida(df, orden.to_dict(), estrategias, puntaje=puntaje, umbral=umbral, config=config_actual):
                log.info(f"❌ Cierre por '{razon}' evitado: condiciones técnicas aún válidas.")
                return
            self._cerrar_y_reportar(orden, precio_cierre, f"Estrategia: {razon}")

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
        if self.orders.obtener(symbol):
            self._verificar_salidas(symbol, df)
        config_actual = configurar_parametros_dinamicos(
            symbol, df, self.config_por_simbolo.get(symbol, {})
        )
        self.config_por_simbolo[symbol] = config_actual
        evaluacion = self.engine.evaluar_entrada(symbol, df)
        if not evaluacion:
            return

        puntaje = evaluacion.get("puntaje_total", 0)
        estrategias = evaluacion.get("estrategias_activas", {})
        pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
        umbral = calcular_umbral_adaptativo(symbol, df, estrategias, pesos_symbol)
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