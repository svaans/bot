"""Controlador principal del bot modular."""

from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime
import numpy as np

import pandas as pd

from core.config_manager import Config
from core.data_feed import DataFeed
from core.strategy_engine import StrategyEngine
from core.risk_manager import RiskManager
from core.order_manager import OrderManager
from core.notificador import Notificador
from binance_api.cliente import crear_cliente
from core.adaptador_umbral import (
    calcular_tp_sl_adaptativos,
    calcular_umbral_adaptativo,
)
from core.pesos import cargar_pesos_estrategias
from core.kelly import calcular_fraccion_kelly
from core.persistencia_tecnica import PersistenciaTecnica
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
from core.tendencia import detectar_tendencia, señales_repetidas
from filtros.validador_entradas import evaluar_validez_estrategica
from estrategias_entrada.gestor_entradas import entrada_permitida
from indicadores.rsi import calcular_rsi
from indicadores.momentum import calcular_momentum
from indicadores.slope import calcular_slope


log = configurar_logger("trader")


@dataclass
class EstadoSimbolo:
    buffer: List[dict]
    ultimo_umbral: float = 0.0
    ultimo_timestamp: int | None = None


class Trader:
    """Orquesta el flujo de datos y las operaciones de trading."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.data_feed = DataFeed(config.intervalo_velas)
        self.engine = StrategyEngine()
        self.risk = RiskManager(config.umbral_riesgo_diario)
        self.notificador = Notificador(config.telegram_token, config.telegram_chat_id)
        self.orders = OrderManager(config.modo_real, self.risk, self.notificador)
        self.cliente = crear_cliente(config)
        self.persistencia = PersistenciaTecnica(
            config.persistencia_minima,
            config.peso_extra_persistencia,
        )
        self.fraccion_kelly = calcular_fraccion_kelly()
        log.info(f"⚖️ Fracción Kelly: {self.fraccion_kelly:.4f}")
        try:
            balance = self.cliente.fetch_balance()
            euros = balance['total'].get('EUR', 0)
        except Exception:
            euros = 0
        inicial = euros / max(len(config.symbols), 1)
        inicial = max(inicial, 20.0)
        self.capital_por_simbolo: Dict[str, float] = {
            s: inicial for s in config.symbols
        }
        self.capital_inicial_diario = self.capital_por_simbolo.copy()
        self.fecha_actual = datetime.utcnow().date() 
        self.estado: Dict[str, EstadoSimbolo] = {s: EstadoSimbolo([]) for s in config.symbols}
        self.config_por_simbolo: Dict[str, dict] = {s: {} for s in config.symbols}
        self.pesos_por_simbolo: Dict[str, Dict[str, float]] = cargar_pesos_estrategias()
        self.historial_cierres: Dict[str, dict] = {}
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
        log.info(f"✅ Orden cerrada: {symbol} a {precio:.2f}€ por '{motivo}'")

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
        capital_inicial = self.capital_por_simbolo.get(orden.symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        self.capital_por_simbolo[orden.symbol] = capital_inicial + ganancia
        self.historial_cierres[orden.symbol] = {
            "timestamp": datetime.utcnow().isoformat(),
            "motivo": motivo.lower().strip(),
        }

    @property
    def ordenes_abiertas(self):
        """Compatibilidad con ``monitorear_estado_periodicamente``."""
        return self.orders.ordenes
    
    def ajustar_capital_diario(self, factor: float = 0.2, limite: float = 0.3) -> None:
        """Redistribuye el capital entre símbolos según rendimiento diario."""
        total = sum(self.capital_por_simbolo.values())
        pesos = {}
        for symbol in self.capital_por_simbolo:
            inicio = self.capital_inicial_diario.get(symbol, self.capital_por_simbolo[symbol])
            final = self.capital_por_simbolo[symbol]
            rendimiento = (final - inicio) / inicio if inicio else 0
            peso = 1 + factor * rendimiento
            peso = max(1 - limite, min(1 + limite, peso))
            pesos[symbol] = peso

        suma = sum(pesos.values()) or 1
        for symbol in self.capital_por_simbolo:
            self.capital_por_simbolo[symbol] = round(total * pesos[symbol] / suma, 2)

        self.capital_inicial_diario = self.capital_por_simbolo.copy()
        self.fecha_actual = datetime.utcnow().date()
        log.info(f"💰 Capital redistribuido: {self.capital_por_simbolo}")
    
    def _calcular_cantidad(self, symbol: str, precio: float) -> float:
        """Determina la cantidad de cripto a comprar con capital asignado."""
        balance = self.cliente.fetch_balance()
        euros = balance['total'].get('EUR', 0)
        if euros <= 0:
            log.debug("Saldo en EUR insuficiente")
            return 0.0
        capital_symbol = self.capital_por_simbolo.get(symbol, euros / max(len(self.estado), 1))
        riesgo = max(capital_symbol * self.fraccion_kelly, self.config.min_order_eur)
        riesgo = min(riesgo, euros)
        cantidad = round(riesgo / precio, 6)
        if cantidad * precio < self.config.min_order_eur:
            log.debug(
                f"Orden mínima {self.config.min_order_eur}€, intento {cantidad * precio:.2f}€"
            )
            return 0.0
        return cantidad

    def _abrir_operacion_real(self, symbol: str, precio: float, sl: float, tp: float, estrategias: Dict) -> None:
        cantidad = self._calcular_cantidad(symbol, precio)
        if cantidad <= 0:
            return
        self.orders.abrir(symbol, precio, sl, tp, estrategias, "", cantidad)
        log.info("✅ Orden abierta en modo real: "
                 f"{symbol} {cantidad} unidades a {precio:.2f}€ SL: {sl:.2f} TP: {tp:.2f}")

    def _verificar_salidas(self, symbol: str, df: pd.DataFrame) -> None:
        """Evalúa si la orden abierta en ``symbol`` debe cerrarse."""
        orden = self.orders.obtener(symbol)
        if not orden:
            log.debug(f"No hay orden abierta para {symbol}.")
            return

        precio_min = float(df["low"].iloc[-1])
        precio_max = float(df["high"].iloc[-1])
        precio_cierre = float(df["close"].iloc[-1])
        config_actual = self.config_por_simbolo.get(symbol, {})
        log.debug(f"Verificando salidas para {symbol} con orden: {orden.to_dict()}")

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
            log.info(f"💰 TP alcanzado para {symbol} a {precio_max:.2f}€")
            return
        

        # --- Trailing Stop ---
        if precio_cierre > orden.max_price:
            orden.max_price = precio_cierre

        config_actual = configurar_parametros_dinamicos(symbol, df, config_actual)
        self.config_por_simbolo[symbol] = config_actual

        cerrar, motivo = verificar_trailing_stop(orden.to_dict(), precio_cierre, config=config_actual)
        if cerrar:
            self._cerrar_y_reportar(orden, precio_cierre, motivo)
            log.info(f"🔄 Trailing Stop activado para {symbol} a {precio_cierre:.2f}€")
            return

        # --- Cambio de tendencia ---
        if verificar_reversion_tendencia(symbol, df, orden.tendencia):
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            if not verificar_filtro_tecnico(symbol, df, orden.estrategias_activas, pesos_symbol):
                self._cerrar_y_reportar(orden, precio_cierre, "Cambio de tendencia")
                log.info(f"🔄 Cambio de tendencia detectado para {symbol}. Cierre recomendado.")
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
        if datetime.utcnow().date() != self.fecha_actual:
            self.ajustar_capital_diario()
        estado.buffer.append(vela)
        if len(estado.buffer) > 50:
            estado.buffer = estado.buffer[-50:]
        if vela.get("timestamp") == estado.ultimo_timestamp:
            return
        estado.ultimo_timestamp = vela.get("timestamp")
        if len(estado.buffer) < 30:
            return

        df = pd.DataFrame(estado.buffer)
        if self.orders.obtener(symbol):
            self._verificar_salidas(symbol, df)
            return
        config_actual = configurar_parametros_dinamicos(
            symbol, df, self.config_por_simbolo.get(symbol, {})
        )
        self.config_por_simbolo[symbol] = config_actual
        evaluacion = self.engine.evaluar_entrada(symbol, df)
        if not evaluacion:
            return

        
        estrategias = evaluacion.get("estrategias_activas", {})
        pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
        umbral = calcular_umbral_adaptativo(symbol, df, estrategias, pesos_symbol)
        estrategias_persistentes = self.persistencia.filtrar_persistentes(symbol, estrategias)
        if not estrategias_persistentes:
            return
        puntaje = sum(pesos_symbol.get(k, 0) for k in estrategias_persistentes)
        puntaje += self.persistencia.peso_extra * len(estrategias_persistentes)
        estado.ultimo_umbral = umbral

        cierre = self.historial_cierres.get(symbol)
        if cierre:
            cooldown = int(config_actual.get("cooldown_tras_perdida", 5)) * 60
            try:
                ts = cierre["timestamp"]
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                elif isinstance(ts, (int, float)):
                    ts = datetime.utcfromtimestamp(ts)
                tiempo_desde_cierre = (datetime.utcnow() - ts).total_seconds()
            except Exception as e:
                log.warning(f"⚠️ No se pudo calcular cooldown para {symbol}: {e}")
                tiempo_desde_cierre = float('inf')

            if cierre["motivo"] in ["stop loss", "estrategia: cambio de tendencia", "cambio de tendencia"] and tiempo_desde_cierre < cooldown:
                log.info(
                    f"🕒 Cooldown activo para {symbol}. Esperando tras pérdida anterior ({tiempo_desde_cierre:.0f}s)"
                )
                return

        estrategias_activas = list(estrategias_persistentes.keys())
        peso_total = sum(pesos_symbol.get(k, 0) for k in estrategias_activas)
        diversidad = len(estrategias_activas)
        peso_min_total = config_actual.get("peso_minimo_total", 0.5)
        diversidad_min = config_actual.get("diversidad_minima", 2)


        if puntaje < umbral:
            log.debug(f"🚫 {symbol}: puntaje {puntaje:.2f} < umbral {umbral:.2f}")
            return
        
        if diversidad < diversidad_min or peso_total < peso_min_total:
            log.debug(
                f"🚫 Entrada bloqueada por diversidad/peso insuficiente: {diversidad}/{diversidad_min}, {peso_total:.2f}/{peso_min_total:.2f}"
            )
            return
        if not evaluar_validez_estrategica(symbol, df, estrategias):
            log.debug(f"❌ Entrada rechazada por filtro estratégico en {symbol}.")
            return

        ventana_close = df["close"].tail(10)
        media_close = np.mean(ventana_close)
        volatilidad_actual = np.std(ventana_close) / media_close if media_close else 0

        repetidas = señales_repetidas(
            buffer=estado.buffer,
            estrategias_func=pesos_symbol,
            tendencia_actual=evaluacion.get("tendencia", ""),
            volatilidad_actual=volatilidad_actual,
            ventanas=5,
        )

        minimo = self.persistencia.minimo
        if repetidas < minimo:
            log.info(
                f"🚫 Entrada rechazada en {symbol}: Persistencia {repetidas}/5 < {minimo}"
            )
            return

        if repetidas < 2 and puntaje < 1.2 * umbral:
            log.info(
                f"🚫 Entrada rechazada en {symbol}: {repetidas}/5 señales persistentes y puntaje débil ({puntaje:.2f})"
            )
            return
        elif repetidas < 2:
            log.info(
                f"⚠️ Entrada débil en {symbol}: Persistencia {repetidas}/5 insuficiente pero puntaje alto ({puntaje}) > Umbral {umbral} — Permitida."
            )

        rsi = calcular_rsi(df)
        momentum = calcular_momentum(df)
        slope = calcular_slope(df)

        if not entrada_permitida(symbol, puntaje, umbral, estrategias_persistentes, rsi, slope, momentum):
            return

        log.info(
            f"✅ Entrada confirmada en {symbol}. Puntaje {puntaje:.2f}, Peso {peso_total:.2f}, Diversidad {diversidad}, Persistentes {len(estrategias_persistentes)}"
        )

        balance = self.cliente.fetch_balance()
        capital_total = balance['total'].get('EUR', 0)
        if self.risk.riesgo_superado(capital_total):
            log.warning(f"🚫 Riesgo diario superado para {symbol}")
            return

        sl, tp = calcular_tp_sl_adaptativos(df, float(vela["close"]))
        precio = float(vela["close"])
        if self.config.modo_real:
            self._abrir_operacion_real(symbol, precio, sl, tp, estrategias_persistentes)
        else:
            self.orders.abrir(symbol, precio, sl, tp, estrategias_persistentes, "")

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