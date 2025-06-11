"""Controlador principal del bot modular."""

from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime
from datetime import datetime, timedelta
import os
import numpy as np

import pandas as pd

from core.config_manager import Config
from core.data_feed import DataFeed
from core.strategy_engine import StrategyEngine
from core.risk_manager import RiskManager
from core.order_manager import OrderManager
from core.notificador import Notificador
from binance_api.cliente import (
    crear_cliente,
    fetch_balance_async,
    load_markets_async,
    fetch_ohlcv_async,
)
from core.adaptador_umbral import (
    calcular_tp_sl_adaptativos,
    calcular_umbral_adaptativo,
)
from aprendizaje.rl_policy import rl_policy
from core.pesos import cargar_pesos_estrategias
from core.kelly import calcular_fraccion_kelly
from core.persistencia_tecnica import PersistenciaTecnica, coincidencia_parcial
from aprendizaje.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from core.logger import configurar_logger
from core.monitor_estado_bot import monitorear_estado_periodicamente
from core import ordenes_reales
from core.adaptador_configuracion import configurar_parametros_dinamicos
from ccxt.base.errors import BaseError
from core.reporting import reporter_diario
from aprendizaje.aprendizaje_en_linea import registrar_resultado_trade
from estrategias_salida.salida_trailing_stop import verificar_trailing_stop
from estrategias_salida.salida_por_tendencia import verificar_reversion_tendencia
from estrategias_salida.gestor_salidas import evaluar_salidas, verificar_filtro_tecnico
from estrategias_salida.salida_stoploss import verificar_salida_stoploss
from filtros.filtro_salidas import validar_necesidad_de_salida
from core.utils import (
    validar_tp,
    distancia_minima_valida,
    margen_tp_sl_valido,
    validar_ratio_beneficio,
)
from core.tendencia import detectar_tendencia
from filtros.validador_entradas import evaluar_validez_estrategica
from estrategias_entrada.gestor_entradas import entrada_permitida
from core.estrategias import filtrar_por_direccion
from indicadores.rsi import calcular_rsi
from indicadores.momentum import calcular_momentum
from indicadores.slope import calcular_slope
from core.market_regime import detectar_regimen


log = configurar_logger("trader")


@dataclass
class EstadoSimbolo:
    buffer: List[dict]
    ultimo_umbral: float = 0.0
    ultimo_timestamp: int | None = None
    regimen: str = ""


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
        self._markets = None
        self.modo_capital_bajo = config.modo_capital_bajo
        self.persistencia = PersistenciaTecnica(
            config.persistencia_minima,
            config.peso_extra_persistencia,
        )
        self.fraccion_kelly = calcular_fraccion_kelly()
        factor_kelly = self.risk.multiplicador_kelly()
        self.fraccion_kelly *= factor_kelly
        log.info(f"⚖️ Fracción Kelly: {self.fraccion_kelly:.4f} (x{factor_kelly:.3f})")
        euros = 0
        if config.api_key and config.api_secret:
            try:
                balance = self.cliente.fetch_balance()
                euros = balance["total"].get("EUR", 0)
            except BaseError as e:
                log.error(f"❌ Error al obtener balance: {e}")
                raise
        else:
            log.info("⚠️ Claves API no proporcionadas, se inicia con balance 0")
        inicial = euros / max(len(config.symbols), 1)
        inicial = max(inicial, 20.0)
        self.capital_por_simbolo: Dict[str, float] = {
            s: inicial for s in config.symbols
        }
        self.capital_inicial_diario = self.capital_por_simbolo.copy()
        self.fecha_actual = datetime.utcnow().date()
        self.estado: Dict[str, EstadoSimbolo] = {
            s: EstadoSimbolo([]) for s in config.symbols
        }
        self.config_por_simbolo: Dict[str, dict] = {s: {} for s in config.symbols}
        self.pesos_por_simbolo: Dict[str, Dict[str, float]] = cargar_pesos_estrategias()
        self.historial_cierres: Dict[str, dict] = {}
        self._task: asyncio.Task | None = None
        self._task_estado: asyncio.Task | None = None

        try:
            self.orders.ordenes = ordenes_reales.obtener_todas_las_ordenes()
            if not self.orders.ordenes:
                self.orders.ordenes = ordenes_reales.sincronizar_ordenes_binance(
                    config.symbols
                )
        except Exception as e:
            log.warning(f"⚠️ Error cargando órdenes previas desde la base de datos: {e}")
            raise

        if self.orders.ordenes:
            log.warning(
                "⚠️ Órdenes abiertas encontradas al iniciar. Serán monitoreadas."
            )

    async def cerrar_operacion(self, symbol: str, precio: float, motivo: str) -> None:
        """Cierra una orden y actualiza los pesos si corresponden."""
        if not await self.orders.cerrar(symbol, precio, motivo):
            log.debug(f"🔁 Intento duplicado de cierre ignorado para {symbol}")
            return
        actualizar_pesos_estrategias_symbol(symbol)
        self.pesos_por_simbolo = cargar_pesos_estrategias()
        log.info(f"✅ Orden cerrada: {symbol} a {precio:.2f}€ por '{motivo}'")

    async def _cerrar_y_reportar(
        self, orden, precio: float, motivo: str, tendencia: str | None = None
    ) -> None:
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
        if not await self.orders.cerrar(orden.symbol, precio, motivo):
            log.warning(
                f"❌ No se pudo confirmar el cierre de {orden.symbol}. Se omitirá el registro."
            )
            return False
        reporter_diario.registrar_operacion(info)
        registrar_resultado_trade(orden.symbol, info, retorno_total)
        actualizar_pesos_estrategias_symbol(orden.symbol)
        self.pesos_por_simbolo = cargar_pesos_estrategias()
        capital_inicial = self.capital_por_simbolo.get(orden.symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        self.capital_por_simbolo[orden.symbol] = capital_inicial + ganancia
        self.historial_cierres[orden.symbol] = {
            "timestamp": datetime.utcnow().isoformat(),
            "motivo": motivo.lower().strip(),
            "velas": 0,
            "precio": precio,
            "tendencia": tendencia,
        }
        metricas = self._metricas_recientes()
        self.risk.ajustar_umbral(metricas)
        return True

    @property
    def ordenes_abiertas(self):
        """Compatibilidad con ``monitorear_estado_periodicamente``."""
        return self.orders.ordenes
        
    
    def ajustar_capital_diario(
        self,
        factor: float = 0.2,
        limite: float = 0.3,
        penalizacion_corr: float = 0.2,
        umbral_corr: float = 0.8,
    ) -> None:
        """Redistribuye el capital según rendimiento y correlación entre símbolos."""
        total = sum(self.capital_por_simbolo.values())
        pesos = {}
        senales = {s: self._contar_senales(s) for s in self.capital_por_simbolo}
        max_senales = max(senales.values()) if senales else 0
        correlaciones = self._calcular_correlaciones()
        for symbol in self.capital_por_simbolo:
            inicio = self.capital_inicial_diario.get(
                symbol, self.capital_por_simbolo[symbol]
            )
            final = self.capital_por_simbolo[symbol]
            rendimiento = (final - inicio) / inicio if inicio else 0
            peso = 1 + factor * rendimiento
            if max_senales > 0:
                peso += 0.2 * senales[symbol] / max_senales
            
            # Penaliza símbolos altamente correlacionados
            corr_media = None
            if not correlaciones.empty and symbol in correlaciones.columns:
                corr_series = (
                    correlaciones[symbol].drop(labels=[symbol], errors="ignore").abs()
                )
                corr_media = corr_series.mean()
            if corr_media and corr_media >= umbral_corr:
                peso *= 1 - penalizacion_corr * corr_media

            peso = max(1 - limite, min(1 + limite, peso))
            pesos[symbol] = peso

        suma = sum(pesos.values()) or 1
        for symbol in self.capital_por_simbolo:
            self.capital_por_simbolo[symbol] = round(total * pesos[symbol] / suma, 2)

        self.capital_inicial_diario = self.capital_por_simbolo.copy()
        self.fecha_actual = datetime.utcnow().date()
        log.info(f"💰 Capital redistribuido: {self.capital_por_simbolo}")

    async def _obtener_minimo_binance(self, symbol: str) -> float | None:
        """Devuelve el valor mínimo de compra permitido por Binance."""
        try:
            if self._markets is None:
                self._markets = await load_markets_async(self.cliente)
            info = self._markets.get(symbol.replace("/", ""))
            minimo = info.get("limits", {}).get("cost", {}).get("min") if info else None
            return float(minimo) if minimo else None
        except Exception as e:
            log.debug(f"No se pudo obtener mínimo para {symbol}: {e}")
            return None

    async def _precargar_historico(self, velas: int = 12) -> None:
        """Carga datos recientes para todos los símbolos antes de iniciar."""
        for symbol in self.estado.keys():
            try:
                datos = await fetch_ohlcv_async(
                    self.cliente,
                    symbol,
                    self.config.intervalo_velas,
                    limit=velas,
                )
            except BaseError as e:
                log.warning(f"⚠️ Error cargando histórico para {symbol}: {e}")
                continue
            except Exception as e:
                log.warning(f"⚠️ Error inesperado cargando histórico para {symbol}: {e}")
                continue

            for ts, o, h, l, c, v in datos:
                self.estado[symbol].buffer.append(
                    {
                        "symbol": symbol,
                        "timestamp": ts,
                        "open": float(o),
                        "high": float(h),
                        "low": float(l),
                        "close": float(c),
                        "volume": float(v),
                    }
                )

            if datos:
                self.estado[symbol].ultimo_timestamp = datos[-1][0]
        log.info("📈 Histórico inicial cargado")
    
    async def _calcular_cantidad(self, symbol: str, precio: float) -> float:
        """Determina la cantidad de cripto a comprar con capital asignado."""
        balance = await fetch_balance_async(self.cliente)
        euros = balance["total"].get("EUR", 0)
        if euros <= 0:
            log.debug("Saldo en EUR insuficiente")
            return 0.0
       capital_symbol = self.capital_por_simbolo.get(
            symbol, euros / max(len(self.estado), 1)
        )
        fraccion = self.fraccion_kelly
        if self.modo_capital_bajo and euros < 500:
            deficit = (500 - euros) / 500
            fraccion = max(fraccion, 0.02 + deficit * 0.1)
        riesgo = max(capital_symbol * fraccion, self.config.min_order_eur)
        riesgo = min(riesgo, euros)
        minimo_binance = await self._obtener_minimo_binance(symbol)
        cantidad = riesgo / precio
        if cantidad * precio < self.config.min_order_eur:
            log.debug(
                f"Orden mínima {self.config.min_order_eur}€, intento {cantidad * precio:.2f}€"
            )
            return 0.0
        orden_eur = cantidad * precio
        log.info(
            "📊 Capital disponible: %.2f€ | Kelly: %.4f | Orden: %.2f€ | Mínimo Binance: %s | %s"
            % (
                euros,
                fraccion,
                orden_eur,
                f"{minimo_binance:.2f}€" if minimo_binance else "desconocido",
                symbol,
            )
        )
        return round(cantidad, 6)
    
    def _metricas_recientes(self, dias: int = 7) -> dict:
        """Calcula ganancia acumulada y drawdown de los últimos ``dias``."""
        carpeta = reporter_diario.carpeta
        if not os.path.isdir(carpeta):
            return {"ganancia_semana": 0.0, "drawdown": 0.0}

        fecha_limite = datetime.utcnow().date() - timedelta(days=dias)
        retornos: list[float] = []

        for archivo in os.listdir(carpeta):
            if not archivo.endswith(".csv"):
                continue
            try:
                fecha = datetime.fromisoformat(archivo.replace(".csv", "")).date()
            except ValueError:
                continue
            if fecha < fecha_limite:
                continue
            try:
                df = pd.read_csv(os.path.join(carpeta, archivo))
            except (pd.errors.EmptyDataError, OSError):
                continue
            if "retorno_total" in df.columns:
                retornos.extend(df["retorno_total"].dropna().tolist())

        if not retornos:
            return {"ganancia_semana": 0.0, "drawdown": 0.0}

        serie = pd.Series(retornos).cumsum()
        drawdown = float((serie - serie.cummax()).min())
        ganancia = float(serie.iloc[-1])
        return {"ganancia_semana": ganancia, "drawdown": drawdown}
    
    def _contar_senales(self, symbol: str, minutos: int = 60) -> int:
        """Cuenta señales válidas recientes para ``symbol``."""
        estado = self.estado.get(symbol)
        if not estado:
            return 0
        limite = datetime.utcnow().timestamp() * 1000 - minutos * 60 * 1000
        return sum(
            1
            for v in estado.buffer
            if v.get("timestamp", 0) >= limite and v.get("estrategias_activas")
        )
    
    def _calcular_correlaciones(self, periodos: int = 1440) -> pd.DataFrame:
        """Calcula correlación histórica de cierres entre símbolos."""
        precios = {}
        for symbol in self.capital_por_simbolo:
            archivo = f"datos/{symbol.replace('/', '_').lower()}_1m.parquet"
            try:
                df = pd.read_parquet(archivo, columns=["close"])
                precios[symbol] = (
                    df["close"].astype(float).tail(periodos).reset_index(drop=True)
                )
            except Exception as e:
                log.debug(f"No se pudo cargar historial para {symbol}: {e}")
        if len(precios) < 2:
            return pd.DataFrame()
        df_precios = pd.DataFrame(precios)
        return df_precios.corr()
    
    # Helpers de soporte -------------------------------------------------

    def _rechazo(self, symbol: str, motivo: str) -> None:
        """Centraliza los mensajes de rechazo para las entradas."""
        log.info(f"🚫 Entrada rechazada en {symbol}: {motivo}")

    def _validar_puntaje(self, symbol: str, puntaje: float, umbral: float) -> bool:
        """Comprueba si ``puntaje`` supera ``umbral``."""
        if puntaje < umbral:
            log.debug(f"🚫 {symbol}: puntaje {puntaje:.2f} < umbral {umbral:.2f}")
            return False
        return True

    async def _validar_diversidad(
        self,
        symbol: str,
        peso_total: float,
        peso_min_total: float,
        diversidad: int,
        diversidad_min: int,
    ) -> bool:
        """Verifica que la diversidad y el peso total sean suficientes."""
        if self.modo_capital_bajo:
            try:
                balance = await fetch_balance_async(self.cliente)
                euros = balance["total"].get("EUR", 0)
            except BaseError:
                euros = 0
            if euros < 500:
                diversidad_min = min(diversidad_min, 2)
                peso_min_total *= 0.7
        if diversidad < diversidad_min or peso_total < peso_min_total:
            self._rechazo(
                symbol,
                f"Diversidad/Peso insuficiente {diversidad}/{diversidad_min}, {peso_total:.2f}/{peso_min_total:.2f}",
            )
            return False
        return True

   def _validar_estrategia(
        self, symbol: str, df: pd.DataFrame, estrategias: Dict
    ) -> bool:
        """Aplica el filtro estratégico de entradas."""
        if not evaluar_validez_estrategica(symbol, df, estrategias):
            log.debug(f"❌ Entrada rechazada por filtro estratégico en {symbol}.")
            return False
        return True

    def _evaluar_persistencia(
        self,
        symbol: str,
        estado: EstadoSimbolo,
        df: pd.DataFrame,
        pesos_symbol: Dict[str, float],
        tendencia_actual: str,
        puntaje: float,
        umbral: float,
    ) -> bool:
        """Evalúa si las señales persistentes son suficientes para entrar."""
        ventana_close = df["close"].tail(10)
        media_close = np.mean(ventana_close)
        if np.isnan(media_close) or media_close == 0:
            log.debug(f"⚠️ {symbol}: Media de cierre inválida para persistencia")
            return False
        volatilidad_actual = np.std(ventana_close) / media_close

        repetidas = coincidencia_parcial(estado.buffer, pesos_symbol, ventanas=5)
        log.info(
            f"Persistencia detectada {repetidas:.2f} | Mínimo requerido {self.persistencia.minimo}"
        )

        minimo = self.persistencia.minimo
        if repetidas < minimo:
            self._rechazo(symbol, f"Persistencia {repetidas:.2f} < {minimo}")
            return False

        if repetidas < 1 and puntaje < 1.2 * umbral:
            self._rechazo(
                symbol,
                f"{repetidas:.2f} coincidencia y puntaje débil ({puntaje:.2f})",
            )
            return False
        elif repetidas < 1:
            log.info(
                f"⚠️ Entrada débil en {symbol}: Coincidencia {repetidas:.2f} insuficiente pero puntaje alto ({puntaje}) > Umbral {umbral} — Permitida."
            )
        return True
    
    def _tendencia_persistente(
        self, symbol: str, df: pd.DataFrame, tendencia: str, velas: int = 3
    ) -> bool:
        if len(df) < 30 + velas:
            return False
        tiempos = pd.to_datetime(df["timestamp"])
        for i in range(velas):
            sub_df = df.iloc[: -(velas - 1 - i)] if velas - 1 - i > 0 else df
            t, _ = detectar_tendencia(symbol, sub_df)
            if t != tendencia:
                return False
        return True

    def _validar_reentrada_tendencia(
        self, symbol: str, df: pd.DataFrame, cierre: dict, precio: float
    ) -> bool:
        if cierre.get("motivo") != "cambio de tendencia":
            return True

        tendencia = cierre.get("tendencia")
        if not tendencia:
            return False

        cierre_dt = pd.to_datetime(cierre.get("timestamp"), errors="coerce")
        if pd.isna(cierre_dt):
            log.warning(f"⚠️ {symbol}: Timestamp de cierre inválido")
            return False
        df_post = df[pd.to_datetime(df["timestamp"]) > cierre_dt]
        if len(df_post) < 3:
            log.info(f"⏳ {symbol}: esperando confirmación de tendencia")
            return False
        if not self._tendencia_persistente(symbol, df_post, tendencia, velas=3):
            log.info(f"⏳ {symbol}: tendencia {tendencia} no persistente tras cierre")
            return False

        precio_salida = cierre.get("precio")
        if precio_salida is not None and abs(precio - precio_salida) <= precio * 0.001:
            log.info(f"🚫 {symbol}: precio de entrada similar al de salida anterior")
            return False

        return True

    async def _abrir_operacion_real(
        self,
        symbol: str,
        precio: float,
        sl: float,
        tp: float,
        estrategias: Dict,
        tendencia: str,
        direccion: str,
    ) -> None:
        cantidad = await self._calcular_cantidad(symbol, precio)
        if cantidad <= 0:
            return
        await self.orders.abrir(
            symbol, precio, sl, tp, estrategias, tendencia, direccion, cantidad
        )
        log.info(
            "✅ Orden abierta: "
            f"{symbol} {cantidad} unidades a {precio:.2f}€ SL: {sl:.2f} TP: {tp:.2f}"
        )

    async def _verificar_salidas(self, symbol: str, df: pd.DataFrame) -> None:
        """Evalúa si la orden abierta en ``symbol`` debe cerrarse."""
        orden = self.orders.obtener(symbol)
        if not orden:
            log.warning(f"⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}")
            return

        precio_min = float(df["low"].iloc[-1])
        precio_max = float(df["high"].iloc[-1])
        precio_cierre = float(df["close"].iloc[-1])
        config_actual = self.config_por_simbolo.get(symbol, {})
        regimen = detectar_regimen(df)
        log.debug(f"Verificando salidas para {symbol} con orden: {orden.to_dict()}")

        # --- Stop Loss con validación ---
        if precio_min <= orden.stop_loss:
            resultado = verificar_salida_stoploss(
                orden.to_dict(), df, config=config_actual
            )
            if resultado.get("cerrar", False):
                await self._cerrar_y_reportar(orden, orden.stop_loss, "Stop Loss")
            else:
                if resultado.get("evitado", False):
                    log.debug("SL evitado correctamente, no se notificará por Telegram")
                    log.info(
                        f"🛡️ SL evitado para {symbol} → {resultado.get('motivo', '')}"
                    )
                else:
                    log.info(f"ℹ️ {symbol} → {resultado.get('motivo', '')}")
            return

        # --- Take Profit ---
        if precio_max >= orden.take_profit:
            if await self._cerrar_y_reportar(orden, precio_max, "Take Profit"):
                log.info(f"💰 TP alcanzado para {symbol} a {precio_max:.2f}€")
            return
        

        # --- Trailing Stop ---
        if precio_cierre > orden.max_price:
            orden.max_price = precio_cierre

        config_actual = configurar_parametros_dinamicos(symbol, df, config_actual)
        self.config_por_simbolo[symbol] = config_actual

        try:
            cerrar, motivo = verificar_trailing_stop(
                orden.to_dict(), precio_cierre, config=config_actual
            )
        except Exception as e:
            log.warning(f"⚠️ Error en trailing stop para {symbol}: {e}")
            cerrar, motivo = False, ""
        if cerrar:
            if await self._cerrar_y_reportar(orden, precio_cierre, motivo):
               log.info(
                    f"🔄 Trailing Stop activado para {symbol} a {precio_cierre:.2f}€"
                )
            return

        # --- Cambio de tendencia ---
        if verificar_reversion_tendencia(symbol, df, orden.tendencia):
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            if not verificar_filtro_tecnico(
                symbol, df, orden.estrategias_activas, pesos_symbol
            ):
                nueva_tendencia, _ = detectar_tendencia(symbol, df)
                if await self._cerrar_y_reportar(
                    orden,
                    precio_cierre,
                    "Cambio de tendencia",
                    tendencia=nueva_tendencia,
                ):
                    log.info(
                        f"🔄 Cambio de tendencia detectado para {symbol}. Cierre recomendado."
                    )
                return

        # --- Estrategias de salida personalizadas ---
        try:
            resultado = evaluar_salidas(orden.to_dict(), df, config=config_actual)
        except Exception as e:
            log.warning(f"⚠️ Error evaluando salidas para {symbol}: {e}")
            resultado = {}
        if resultado.get("cerrar", False):
            razon = resultado.get("razon", "Estrategia desconocida")
            evaluacion = self.engine.evaluar_entrada(symbol, df, regimen)
            estrategias = evaluacion.get("estrategias_activas", {})
            puntaje = evaluacion.get("puntaje_total", 0)
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            umbral = rl_policy.sugerir_umbral(df)
            if umbral is None:
                umbral = calcular_umbral_adaptativo(
                    symbol, df, estrategias, pesos_symbol
                )
            if not validar_necesidad_de_salida(
                df,
                orden.to_dict(),
                estrategias,
                puntaje=puntaje,
                umbral=umbral,
                config=config_actual,
            ):
                log.info(
                    f"❌ Cierre por '{razon}' evitado: condiciones técnicas aún válidas."
                )
                return
            await self._cerrar_y_reportar(orden, precio_cierre, f"Estrategia: {razon}")

    async def ejecutar(self) -> None:
        """Inicia el procesamiento de todos los símbolos."""
        async def handle(candle: dict) -> None:
            await self._procesar_vela(candle)

        symbols = list(self.estado.keys())
        await self._precargar_historico()
        self._task = asyncio.create_task(self.data_feed.escuchar(symbols, handle))
        self._task_estado = asyncio.create_task(monitorear_estado_periodicamente(self))
        await asyncio.gather(self._task, self._task_estado)

    async def _procesar_vela(self, vela: dict) -> None:
        symbol = vela["symbol"]
        estado = self.estado[symbol]
        if datetime.utcnow().date() != self.fecha_actual:
            self.ajustar_capital_diario()

        # Mantiene un buffer de velas reciente por símbolo
        estado.buffer.append(vela)
        if len(estado.buffer) > 30:
            estado.buffer = estado.buffer[-30:]
        if vela.get("timestamp") == estado.ultimo_timestamp:
            return
        estado.ultimo_timestamp = vela.get("timestamp")
        log.info(f"Procesando vela {symbol} | Precio: {vela.get('close')}")
        

        df = pd.DataFrame(estado.buffer)
        regimen = detectar_regimen(df)
        estado.regimen = regimen
        config_actual = configurar_parametros_dinamicos(
            symbol, df, self.config_por_simbolo.get(symbol, {})
        )
        self.config_por_simbolo[symbol] = config_actual
        
        evaluacion = self.engine.evaluar_entrada(symbol, df, regimen)
        estrategias = evaluacion.get("estrategias_activas", {})
        estado.buffer[-1]["estrategias_activas"] = estrategias
        self.persistencia.actualizar(symbol, estrategias)

        if len(estado.buffer) < 30:
            log.debug(
                f"\U0001f4c9 [{symbol}] Buffer insuficiente ({len(estado.buffer)} velas)"
            )
            return

        tendencia_actual, _ = detectar_tendencia(symbol, df)
        if self.orders.obtener(symbol):
            await self._verificar_salidas(symbol, df)
            return

        
        pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
        umbral = rl_policy.sugerir_umbral(df)
        if umbral is None:
            umbral = calcular_umbral_adaptativo(symbol, df, estrategias, pesos_symbol)
        estrategias_persistentes = {
            e: True
            for e, act in estrategias.items()
            if act and self.persistencia.es_persistente(symbol, e)
        }
        if not estrategias_persistentes:
            return
        
        direccion = "short" if tendencia_actual == "bajista" else "long"
        estrategias_persistentes, incoherentes = filtrar_por_direccion(
            estrategias_persistentes, direccion
        )
        if not estrategias_persistentes:
            return

        penalizacion = 0.0
        if incoherentes:
            penalizacion = 0.1 * len(incoherentes)

        puntaje = sum(pesos_symbol.get(k, 0) for k in estrategias_persistentes)
        puntaje += self.persistencia.peso_extra * len(estrategias_persistentes)
        puntaje -= penalizacion
        estado.ultimo_umbral = umbral
        
        # Respeta un número de velas tras un Stop Loss
        cierre = self.historial_cierres.get(symbol)
        if cierre and cierre.get("motivo") == "stop loss":
            cooldown_velas = int(config_actual.get("cooldown_tras_perdida", 5))
            velas = cierre.get("velas", 0)
            if velas < cooldown_velas:
                cierre["velas"] = velas + 1
                restante = cooldown_velas - velas
                log.info(f"🕒 Cooldown activo para {symbol}. Quedan {restante} velas")
                return
            else:
                self.historial_cierres.pop(symbol, None)

        # Validación de reentrada tras cambio de tendencia
        cierre = self.historial_cierres.get(symbol)
        if cierre and cierre.get("motivo") == "cambio de tendencia":
            precio_actual = float(df["close"].iloc[-1])
            if not self._validar_reentrada_tendencia(symbol, df, cierre, precio_actual):
                cierre["velas"] = cierre.get("velas", 0) + 1
                return
            else:
                self.historial_cierres.pop(symbol, None)

        estrategias_activas = list(estrategias_persistentes.keys())
        peso_total = sum(pesos_symbol.get(k, 0) for k in estrategias_activas)
        diversidad = len(estrategias_activas)
        peso_min_total = config_actual.get("peso_minimo_total", 0.5)
        diversidad_min = config_actual.get("diversidad_minima", 2)
        persistencia = coincidencia_parcial(estado.buffer, pesos_symbol, ventanas=5)
        log.info(
            f"Puntaje: {puntaje:.2f}, Umbral: {umbral:.2f}, Peso total: {peso_total:.2f}, Persistencia: {persistencia:.2f}"
        )


        if not self._validar_puntaje(symbol, puntaje, umbral):
            return

       if not await self._validar_diversidad(
            symbol, peso_total, peso_min_total, diversidad, diversidad_min
        ):
            return

        if not self._validar_estrategia(symbol, df, estrategias):
            return

        # Comprueba persistencia y fuerza de las señales
        if not self._evaluar_persistencia(
            symbol, estado, df, pesos_symbol, tendencia_actual, puntaje, umbral
        ):
            return
        
        rsi = calcular_rsi(df)
        momentum = calcular_momentum(df)
        slope = calcular_slope(df)

        if not entrada_permitida(
            symbol,
            puntaje,
            umbral,
            estrategias_persistentes,
            rsi,
            slope,
            momentum,
            df,
            direccion,
        ):
            return

        log.info(
            f"✅ Entrada confirmada en {symbol}. Puntaje {puntaje:.2f}, Peso {peso_total:.2f}, Diversidad {diversidad}, Persistentes {len(estrategias_persistentes)}, Tendencia {tendencia_actual}, Dirección {direccion}"
        )

        balance = await fetch_balance_async(self.cliente)
        capital_total = balance["total"].get("EUR", 0)
        # Verifica el límite de riesgo diario antes de abrir una nueva orden
        if self.risk.riesgo_superado(capital_total):
            log.warning(f"🚫 Riesgo diario superado para {symbol}")
            return

        capital_symbol = self.capital_por_simbolo.get(symbol, 0)
        sl, tp = calcular_tp_sl_adaptativos(
            df,
            float(vela["close"]),
            {**config_actual, "modo_capital_bajo": self.modo_capital_bajo},
            capital_symbol,
        )
        precio = float(vela["close"])

        tp = validar_tp(tp, precio)
        if not distancia_minima_valida(precio, sl, tp):
            log.warning(
                f"📏 Distancia SL/TP insuficiente para {symbol}. SL: {sl:.2f} TP: {tp:.2f}"
            )
            return
        if not margen_tp_sl_valido(tp, sl, precio):
            log.warning(
                f"📏 Margen TP/SL inválido para {symbol}. SL: {sl:.2f} TP: {tp:.2f}"
            )
            return

        ratio_min = config_actual.get("ratio_minimo_beneficio", 1.5)
        if not validar_ratio_beneficio(precio, sl, tp, ratio_min):
            log.warning(f"⚖️ Ratio riesgo/beneficio insuficiente para {symbol}")
            return
        
        await self._abrir_operacion_real(
            symbol,
            precio,
            sl,
            tp,
            estrategias_persistentes,
            tendencia_actual,
            direccion,
        )
        return

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
