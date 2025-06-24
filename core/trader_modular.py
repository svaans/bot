"""Controlador principal del bot modular."""

from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime, timedelta, date
import json
import os
import numpy as np

import pandas as pd

from config.config_manager import Config
from core.data import DataFeed
from core.strategies import StrategyEngine
from core.risk import RiskManager
from core.position_manager import PositionManager
from core.notification_manager import NotificationManager
from core.capital_manager import CapitalManager
from binance_api.cliente import (
    crear_cliente,
    fetch_balance_async,
    fetch_ohlcv_async,
)
from core.adaptador_dinamico import (
    calcular_umbral_adaptativo,
    calcular_tp_sl_adaptativos,
)
from core.utils.utils import distancia_minima_valida, leer_reporte_seguro
from core.strategies import cargar_pesos_estrategias
from core.risk import calcular_fraccion_kelly
from core.data import (
    PersistenciaTecnica,
    coincidencia_parcial,
    calcular_persistencia_minima,
)
from core.metricas_semanales import metricas_tracker, metricas_semanales
from learning.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from core.utils.utils import configurar_logger
from core.monitor_estado_bot import monitorear_estado_periodicamente
from core.contexto_externo import StreamContexto
from core.orders import real_orders
from core.adaptador_dinamico import adaptar_configuracion as adaptar_configuracion_base
from core.adaptador_configuracion_dinamica import adaptar_configuracion
from ccxt.base.errors import BaseError
from core.reporting import reporter_diario
from core.registro_metrico import registro_metrico
from learning.aprendizaje_en_linea import registrar_resultado_trade
from learning.aprendizaje_continuo import ejecutar_ciclo as ciclo_aprendizaje
from core.strategies.exit.salida_trailing_stop import verificar_trailing_stop
from core.strategies.exit.salida_por_tendencia import verificar_reversion_tendencia
from core.strategies.exit.gestor_salidas import (
    evaluar_salidas,
    verificar_filtro_tecnico,
)
from core.strategies.exit.salida_stoploss import verificar_salida_stoploss
from core.strategies.exit.filtro_salidas import validar_necesidad_de_salida
from core.strategies.tendencia import detectar_tendencia
from core.strategies.exit.analisis_salidas import patron_tecnico_fuerte
from core.strategies.entry.validador_entradas import evaluar_validez_estrategica
from core.estrategias import filtrar_por_direccion
from indicators.rsi import calcular_rsi
from indicators.momentum import calcular_momentum
from indicators.slope import calcular_slope
from core.strategies.evaluador_tecnico import (
    evaluar_puntaje_tecnico,
    calcular_umbral_adaptativo as calc_umbral_tecnico,
    cargar_pesos_tecnicos,
    actualizar_pesos_tecnicos,
)
from core.strategies.exit.analisis_previo_salida import (
    permitir_cierre_tecnico,
    evaluar_condiciones_de_cierre_anticipado,
)
from core.auditoria import registrar_auditoria
from indicators.atr import calcular_atr
from core.strategies.exit.verificar_salidas import verificar_salidas
from core.strategies.entry.verificar_entradas import verificar_entrada
from core.procesar_vela import procesar_vela
   

log = configurar_logger("trader")

PESOS_SCORE_TECNICO = {
    "RSI": 1.0,
    "Momentum": 0.5,
    "Slope": 1.0,
    "Tendencia": 1.0,
}


@dataclass
class EstadoSimbolo:
    buffer: List[dict]
    ultimo_umbral: float = 0.0
    ultimo_timestamp: int | None = None
    tendencia_detectada: str | None = None


class Trader:
    """Orquesta el flujo de datos y las operaciones de trading."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.data_feed = DataFeed(config.intervalo_velas)
        self.engine = StrategyEngine()
        self.risk = RiskManager(config.umbral_riesgo_diario)
        self.notificador = NotificationManager(
            config.telegram_token, config.telegram_chat_id
        )
        self.modo_real = getattr(config, "modo_real", False)
        self.orders = PositionManager(self.modo_real, self.risk, self.notificador)
        self.cliente = crear_cliente(config) if self.modo_real else None
        if not self.modo_real:
            log.info("🧪 Modo simulado activado. No se inicializará cliente Binance")
        self._markets = None
        self.modo_capital_bajo = config.modo_capital_bajo
        self.persistencia = PersistenciaTecnica(
            config.persistencia_minima,
            config.peso_extra_persistencia,
        )
        os.makedirs("logs/rechazos", exist_ok=True)
        os.makedirs(os.path.dirname(config.registro_tecnico_csv), exist_ok=True)
        self.umbral_score_tecnico = config.umbral_score_tecnico
        self.usar_score_tecnico = getattr(config, "usar_score_tecnico", True)
        self.contradicciones_bloquean_entrada = config.contradicciones_bloquean_entrada
        self.registro_tecnico_csv = config.registro_tecnico_csv
        self.historicos: Dict[str, pd.DataFrame] = {}
        self.fraccion_kelly = calcular_fraccion_kelly()
        factor_kelly = self.risk.multiplicador_kelly()
        self.fraccion_kelly *= factor_kelly
        factor_vol = 1.0
        try:
            factores = []
            for sym in config.symbols:
                df = self._obtener_historico(sym)
                if df is None or "close" not in df:
                    continue
                cambios = df["close"].pct_change().dropna()
                if cambios.empty:
                    continue
                volatilidad_actual = cambios.tail(1440).std()
                volatilidad_media = cambios.std()
                factores.append(
                    self.risk.factor_volatilidad(
                        float(volatilidad_actual),
                        float(volatilidad_media),
                    )
                )
            if factores:
                factor_vol = min(factores)
        except Exception as e:  # noqa: BLE001
            log.debug(f"No se pudo calcular factor de volatilidad: {e}")

        self.fraccion_kelly *= factor_vol
        log.info(
            f"⚖️ Fracción Kelly: {self.fraccion_kelly:.4f}"
            f" (x{factor_kelly:.3f}, x{factor_vol:.3f})"
        )
        self.piramide_fracciones = max(1, config.fracciones_piramide)
        self.reserva_piramide = max(0.0, min(1.0, config.reserva_piramide))
        self.umbral_piramide = max(0.0, config.umbral_piramide)
        self.riesgo_maximo_diario = 1.0
        self.capital_manager = CapitalManager(
            config,
            self.cliente,
            self.risk,
            self.fraccion_kelly,
        )
        self.capital_por_simbolo = self.capital_manager.capital_por_simbolo
        self.capital_inicial_diario = self.capital_manager.capital_inicial_diario
        self.reservas_piramide = self.capital_manager.reservas_piramide
        self.fecha_actual = self.capital_manager.fecha_actual
        self.estado: Dict[str, EstadoSimbolo] = {
            s: EstadoSimbolo([]) for s in config.symbols
        }
        self.estado_tendencia: Dict[str, str] = {}
        self.config_por_simbolo: Dict[str, dict] = {s: {} for s in config.symbols}
        try:
            self.pesos_por_simbolo: Dict[str, Dict[str, float]] = (
                cargar_pesos_estrategias()
            )
        except ValueError as e:
            log.error(f"❌ {e}")
            raise
        self.historial_cierres: Dict[str, dict] = {}
        self._task: asyncio.Task | None = None
        self._task_estado: asyncio.Task | None = None
        self._task_contexto: asyncio.Task | None = None
        self._task_aprendizaje: asyncio.Task | None = None
        self.context_stream = StreamContexto()

        try:
            self.orders.ordenes = real_orders.obtener_todas_las_ordenes()
            if self.modo_real and not self.orders.ordenes:
                self.orders.ordenes = real_orders.sincronizar_ordenes_binance(
                    config.symbols
                )
        except Exception as e:
            log.warning(f"⚠️ Error cargando órdenes previas desde la base de datos: {e}")
            raise

        if self.orders.ordenes:
            log.warning(
                "⚠️ Órdenes abiertas encontradas al iniciar. Serán monitoreadas."
            )

        if "PYTEST_CURRENT_TEST" not in os.environ:
            self._cargar_estado_persistente()
        else:
            log.debug("🔍 Modo prueba: se omite carga de estado persistente")

    async def cerrar_operacion(self, symbol: str, precio: float, motivo: str) -> None:
        """Cierra una orden y actualiza los pesos si corresponden."""
        if not await self.orders.cerrar_async(symbol, precio, motivo):
            log.debug(f"🔁 Intento duplicado de cierre ignorado para {symbol}")
            return
        actualizar_pesos_estrategias_symbol(symbol)
        try:
            self.pesos_por_simbolo = cargar_pesos_estrategias()
        except ValueError as e:
            log.error(f"❌ {e}")
            return
        log.info(f"✅ Orden cerrada: {symbol} a {precio:.2f}€ por '{motivo}'")

    async def _cerrar_y_reportar(
        self,
        orden,
        precio: float,
        motivo: str,
        tendencia: str | None = None,
        df: pd.DataFrame | None = None,
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
                "capital_inicial": self.capital_por_simbolo.get(orden.symbol, 0.0),
            }
        )
        if not await self.orders.cerrar_async(orden.symbol, precio, motivo):
            log.warning(
                f"❌ No se pudo confirmar el cierre de {orden.symbol}. Se omitirá el registro."
            )
            return False
        
        capital_inicial = self.capital_por_simbolo.get(orden.symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        capital_final = capital_inicial + ganancia
        self.capital_por_simbolo[orden.symbol] = capital_final
        info["capital_final"] = capital_final
        if getattr(orden, "sl_evitar_info", None):
            os.makedirs("logs", exist_ok=True)
            for ev in orden.sl_evitar_info:
                sl_val = ev.get("sl", 0.0)
                peor = (
                    precio < sl_val
                    if orden.direccion in ("long", "compra")
                    else precio > sl_val
                )
                mensaje = (
                    f"❗ Evitar SL en {orden.symbol} resultó en pérdida mayor"
                    f" ({precio:.2f} vs {sl_val:.2f})"
                    if peor
                    else f"👍 Evitar SL en {orden.symbol} fue beneficioso"
                    f" ({precio:.2f} vs {sl_val:.2f})"
                )
                with open("logs/impacto_sl.log", "a") as f:
                    f.write(mensaje + "\n")
                log.info(mensaje)
            orden.sl_evitar_info = []
        reporter_diario.registrar_operacion(info)
        registrar_resultado_trade(orden.symbol, info, retorno_total)
        try:
            if orden.detalles_tecnicos:
                actualizar_pesos_tecnicos(
                    orden.symbol, orden.detalles_tecnicos, retorno_total
                )
        except Exception as e:  # noqa: BLE001
            log.debug(f"No se pudo actualizar pesos tecnicos: {e}")
        actualizar_pesos_estrategias_symbol(orden.symbol)
        try:
            self.pesos_por_simbolo = cargar_pesos_estrategias()
        except ValueError as e:
            log.error(f"❌ {e}")
            return False
        
        duracion = 0.0
        try:
            apertura = datetime.fromisoformat(orden.timestamp)
            duracion = (datetime.utcnow() - apertura).total_seconds() / 60
        except Exception:
            pass
        prev = self.historial_cierres.get(orden.symbol, {})
        self.historial_cierres[orden.symbol] = {
            "timestamp": datetime.utcnow().isoformat(),
            "motivo": motivo.lower().strip(),
            "velas": 0,
            "precio": precio,
            "tendencia": tendencia,
            "duracion": duracion,
            "retorno_total": retorno_total,
        }
        if retorno_total < 0:
            fecha_hoy = datetime.utcnow().date().isoformat()
            if prev.get("fecha_perdidas") != fecha_hoy:
                perdidas = 0
            else:
                perdidas = prev.get("perdidas_consecutivas", 0)
            perdidas += 1
            self.historial_cierres[orden.symbol]["perdidas_consecutivas"] = perdidas
            self.historial_cierres[orden.symbol]["fecha_perdidas"] = fecha_hoy
        else:
            self.historial_cierres[orden.symbol]["perdidas_consecutivas"] = 0
        log.info(
            f"✅ CIERRE {motivo.upper()}: {orden.symbol} | Beneficio: {ganancia:.2f} €"
        )
        registro_metrico.registrar(
            "cierre",
            {
                "symbol": orden.symbol,
                "motivo": motivo,
                "retorno": retorno_total,
                "beneficio": ganancia,
            },
        )
        self._registrar_salida_profesional(
            orden.symbol,
            {
                "tipo_salida": motivo,
                "estrategias_activas": orden.estrategias_activas,
                "score_tecnico_al_cierre": (
                    self._calcular_score_tecnico(
                        df,
                        calcular_rsi(df),
                        calcular_momentum(df),
                        tendencia or "",
                        orden.direccion,
                    )[0]
                    if df is not None
                    else 0.0
                ),
                "capital_final": capital_final,
                "configuracion_usada": self.config_por_simbolo.get(orden.symbol, {}),
                "tiempo_operacion": duracion,
                "beneficio_relativo": retorno_total,
            },
        )
        metricas = self._metricas_recientes()
        self.risk.ajustar_umbral(metricas)
        try:
            rsi_val = calcular_rsi(df) if df is not None else None
            score, _ = (
                self._calcular_score_tecnico(
                    df,
                    rsi_val,
                    calcular_momentum(df),
                    tendencia or "",
                    orden.direccion,
                )
                if df is not None
                else (None, None)
            )
            registrar_auditoria(
                symbol=orden.symbol,
                evento=motivo,
                resultado="ganancia" if retorno_total > 0 else "pérdida",
                estrategias_activas=orden.estrategias_activas,
                score=score,
                rsi=rsi_val,
                tendencia=tendencia,
                capital_actual=capital_final,
                config_usada=self.config_por_simbolo.get(orden.symbol, {}),
            )
        except Exception as e:  # noqa: BLE001
            log.debug(f"No se pudo registrar auditoría de cierre: {e}")
        return True
    
    def _registrar_salida_profesional(self, symbol: str, info: dict) -> None:
        archivo = "reportes_diarios/registro_salidas.parquet"
        os.makedirs(os.path.dirname(archivo), exist_ok=True)
        data = info.copy()
        data["symbol"] = symbol
        data["timestamp"] = datetime.utcnow().isoformat()
        if isinstance(data.get("estrategias_activas"), dict):
            data["estrategias_activas"] = json.dumps(data["estrategias_activas"])
        try:
            if os.path.exists(archivo):
                df = pd.read_parquet(archivo)
                df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
            else:
                df = pd.DataFrame([data])
            df.to_parquet(archivo, index=False)
        except Exception as e:
            log.warning(f"⚠️ Error registrando salida en {archivo}: {e}")
    
    async def _cerrar_parcial_y_reportar(
        self,
        orden,
        cantidad: float,
        precio: float,
        motivo: str,
        df: pd.DataFrame | None = None,
    ) -> bool:
        """Cierre parcial de ``orden`` y registro en el reporte."""
        if not await self.orders.cerrar_parcial_async(
            orden.symbol, cantidad, precio, motivo
        ):
            log.warning(
                f"❌ No se pudo confirmar el cierre parcial de {orden.symbol}. Se omitirá el registro."
            )
            return False

        retorno_unitario = (
            (precio - orden.precio_entrada) / orden.precio_entrada
            if orden.precio_entrada
            else 0.0
        )
        fraccion = cantidad / orden.cantidad if orden.cantidad else 0.0
        retorno_total = retorno_unitario * fraccion
        info = orden.to_dict()
        info.update(
            {
                "precio_cierre": precio,
                "fecha_cierre": datetime.utcnow().isoformat(),
                "motivo_cierre": motivo,
                "retorno_total": retorno_total,
                "cantidad_cerrada": cantidad,
                "capital_inicial": self.capital_por_simbolo.get(orden.symbol, 0.0),
            }
        )
        reporter_diario.registrar_operacion(info)
        registrar_resultado_trade(orden.symbol, info, retorno_total)
        capital_inicial = self.capital_por_simbolo.get(orden.symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        capital_final = capital_inicial + ganancia
        self.capital_por_simbolo[orden.symbol] = capital_final
        info["capital_final"] = capital_final
        log.info(f"✅ CIERRE PARCIAL: {orden.symbol} | Beneficio: {ganancia:.2f} €")
        registro_metrico.registrar(
            "cierre_parcial",
            {
                "symbol": orden.symbol,
                "retorno": retorno_total,
                "beneficio": ganancia,
            },
        )
        self._registrar_salida_profesional(
            orden.symbol,
            {
                "tipo_salida": "parcial",
                "estrategias_activas": orden.estrategias_activas,
                "score_tecnico_al_cierre": (
                    self._calcular_score_tecnico(
                        df,
                        calcular_rsi(df),
                        calcular_momentum(df),
                        orden.tendencia,
                        orden.direccion,
                    )[0]
                    if df is not None
                    else 0.0
                ),
                "configuracion_usada": self.config_por_simbolo.get(orden.symbol, {}),
                "tiempo_operacion": 0.0,
                "beneficio_relativo": retorno_total,
            },
        )
        try:
            rsi_val = calcular_rsi(df) if df is not None else None
            score, _ = (
                self._calcular_score_tecnico(
                    df,
                    rsi_val,
                    calcular_momentum(df),
                    orden.tendencia,
                    orden.direccion,
                )
                if df is not None
                else (None, None)
            )
            registrar_auditoria(
                symbol=orden.symbol,
                evento=motivo,
                resultado="ganancia" if retorno_total > 0 else "pérdida",
                estrategias_activas=orden.estrategias_activas,
                score=score,
                rsi=rsi_val,
                tendencia=orden.tendencia,
                capital_actual=capital_final,
                config_usada=self.config_por_simbolo.get(orden.symbol, {}),
            )
        except Exception as e:  # noqa: BLE001
            log.debug(f"No se pudo registrar auditoría de cierre parcial: {e}")
        return True
    
    def es_salida_parcial_valida(
        self,
        orden,
        precio_tp: float,
        config: dict,
        df: pd.DataFrame,
    ) -> bool:
        """Determina si aplicar TP parcial tiene sentido económico."""

        if not config.get("usar_cierre_parcial", False):
            return False
        try:
            inversion = (orden.precio_entrada or 0.0) * (orden.cantidad or 0.0)
            retorno_potencial = (precio_tp - (orden.precio_entrada or 0.0)) * (
                orden.cantidad or 0.0
            )
        except Exception:
            return False

        if inversion <= config.get("umbral_operacion_grande", 30.0):
            return False
        if retorno_potencial <= config.get("beneficio_minimo_parcial", 5.0):
            return False

        pesos_symbol = self.pesos_por_simbolo.get(orden.symbol, {})
        if not verificar_filtro_tecnico(
            orden.symbol, df, orden.estrategias_activas, pesos_symbol, config=config
        ):
            return False

        return True
    
    async def _piramidar(self, symbol: str, orden, df: pd.DataFrame) -> None:
        """Añade posiciones si el precio avanza a favor."""
        if orden.fracciones_restantes <= 0:
            return
        precio_actual = float(df["close"].iloc[-1])
        if precio_actual >= orden.precio_ultima_piramide * (1 + self.umbral_piramide):
            cantidad = orden.cantidad / orden.fracciones_totales
            if await self.orders.agregar_parcial_async(symbol, precio_actual, cantidad):
                orden.fracciones_restantes -= 1
                orden.precio_ultima_piramide = precio_actual
                log.info(f"🔼 Pirámide ejecutada en {symbol} @ {precio_actual:.2f}")

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
        fecha: datetime.date | None = None,
    ) -> None:
        """Redistribuye el capital según múltiples métricas adaptativas."""
        total = sum(self.capital_por_simbolo.values())
        # Métricas generales de rendimiento (ganancia y drawdown recientes)
        metricas_globales = self._metricas_recientes()
        semanales = metricas_semanales()

        pesos: dict[str, float] = {}

        # Conteo de señales válidas en las últimas 60 min por símbolo
        senales = {s: self._contar_senales(s) for s in self.capital_por_simbolo}
        
        max_senales = max(senales.values()) if senales else 0
        correlaciones = self._calcular_correlaciones()
        stats = getattr(reporter_diario, "estadisticas", pd.DataFrame())
        for symbol in self.capital_por_simbolo:
            inicio = self.capital_inicial_diario.get(
                symbol, self.capital_por_simbolo[symbol]
            )
            final = self.capital_por_simbolo[symbol]
            rendimiento = (final - inicio) / inicio if inicio else 0.0
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
            if corr_media >= umbral_corr:
                peso *= 1 - penalizacion_corr * corr_media

            # Extrae métricas previas del reporte para el símbolo actual
            fila = (
                stats[stats["symbol"] == symbol]
                if (isinstance(stats, pd.DataFrame) and "symbol" in stats.columns)
                else pd.DataFrame()
            )
            drawdown = 0.0
            winrate = 0.0
            ganancia = 0.0
            if not fila.empty:
                drawdown = float(fila["drawdown"].iloc[0])
                operaciones = float(fila["operaciones"].iloc[0])
                wins = float(fila["wins"].iloc[0])
                ganancia = float(fila["retorno_acumulado"].iloc[0])
                winrate = wins / operaciones if operaciones else 0.0
            if not semanales.empty:
                sem = semanales[semanales["symbol"] == symbol]
                if not sem.empty:
                    weekly = float(sem["ganancia_promedio"].iloc[0]) * float(
                        sem["operaciones"].iloc[0]
                    )
                    if weekly < -0.05:
                        peso *= 0.5

            # 4️⃣ Penalización por drawdown acumulado negativo
            if drawdown < 0:
                peso *= 1 + drawdown

            # 5️⃣ Refuerzo por buen desempeño (winrate alto y ganancias)
            if winrate > 0.6 and ganancia > 0:
                refuerzo = min((winrate - 0.6) * ganancia, 0.3)
                peso *= 1 + refuerzo

            # Ajuste global según las métricas recientes de todo el bot
            if metricas_globales:
                ganancia_global = metricas_globales.get("ganancia_semana", 0.0)
                drawdown_global = metricas_globales.get("drawdown", 0.0)
                ajuste_global = 1 + ganancia_global + drawdown_global
                peso *= max(0.5, min(1.5, ajuste_global))

            # Mantiene el peso final dentro del rango establecido

            peso = max(1 - limite, min(1 + limite, peso))
            pesos[symbol] = peso

        suma = sum(pesos.values()) or 1
        for symbol in self.capital_por_simbolo:
            self.capital_por_simbolo[symbol] = round(total * pesos[symbol] / suma, 2)
        for symbol in self.capital_por_simbolo:
            orden = self.orders.obtener(symbol)
            reserva = 0.0
            if orden and orden.cantidad_abierta > 0 and self.estado[symbol].buffer:
                precio_actual = float(self.estado[symbol].buffer[-1].get("close", 0))
                if precio_actual > orden.precio_entrada:
                    reserva = self.capital_por_simbolo[symbol] * self.reserva_piramide
            self.capital_por_simbolo[symbol] -= reserva
            self.reservas_piramide[symbol] = round(reserva, 2)

        self.capital_inicial_diario = self.capital_por_simbolo.copy()
        self.fecha_actual = fecha or datetime.utcnow().date()
        log.info(f"💰 Capital redistribuido: {self.capital_por_simbolo}")


    async def _precargar_historico(self, velas: int = 12) -> None:
        """Carga datos recientes para todos los símbolos antes de iniciar."""
        if not self.modo_real or not self.cliente:
            log.info("📈 Modo simulado: se omite precarga de histórico desde Binance")
            return
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

            for ts, open_, high_, low_, close_, vol in datos:
                self.estado[symbol].buffer.append(
                    {
                        "symbol": symbol,
                        "timestamp": ts,
                        "open": float(open_),
                        "high": float(high_),
                        "low": float(low_),
                        "close": float(close_),
                        "volume": float(vol),
                    }
                )

            if datos:
                self.estado[symbol].ultimo_timestamp = datos[-1][0]
        log.info("📈 Histórico inicial cargado")

    async def _ciclo_aprendizaje(self, intervalo: int = 86400) -> None:
        """Ejecuta el proceso de aprendizaje continuo periódicamente."""
        await asyncio.sleep(1)
        while True:
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, ciclo_aprendizaje)
                log.info("🧠 Ciclo de aprendizaje completado")
            except Exception as e:  # noqa: BLE001
                log.warning(f"⚠️ Error en ciclo de aprendizaje: {e}")
            await asyncio.sleep(intervalo)
    
    async def _calcular_cantidad_async(self, symbol: str, precio: float) -> float:
        """Delegado a :class:`CapitalManager`."""
        return await self.capital_manager.calcular_cantidad_async(symbol, precio)
    
    def _calcular_cantidad(self, symbol: str, precio: float) -> float:
        """Versión síncrona de :meth:`_calcular_cantidad_async`."""
        return self.capital_manager.calcular_cantidad(symbol, precio)

    def _metricas_recientes(self, dias: int = 7) -> dict:
        """Calcula ganancia acumulada y drawdown de los últimos ``dias``."""
        carpeta = reporter_diario.carpeta
        if not os.path.isdir(carpeta):
            return {
                "ganancia_semana": 0.0,
                "drawdown": 0.0,
                "winrate": 0.0,
                "capital_actual": sum(self.capital_por_simbolo.values()),
                "capital_inicial": sum(self.capital_inicial_diario.values()),
            }

        fecha_limite = datetime.utcnow().date() - timedelta(days=dias)
        retornos: list[float] = []

        archivos = sorted(
            [f for f in os.listdir(carpeta) if f.endswith(".csv")], reverse=True
        )[
            :20
        ]  # solo los 20 archivos más recientes

        for archivo in archivos:
            try:
                fecha = datetime.fromisoformat(archivo.replace(".csv", "")).date()
            except ValueError:
                continue
            if fecha < fecha_limite:
                continue
            ruta_archivo = os.path.join(carpeta, archivo)
            df = leer_reporte_seguro(ruta_archivo, columnas_esperadas=20)
            if df.empty:
                continue
            if "retorno_total" in df.columns:
                retornos.extend(df["retorno_total"].dropna().tolist())

        if not retornos:
            return {
                "ganancia_semana": 0.0,
                "drawdown": 0.0,
                "winrate": 0.0,
                "capital_actual": sum(self.capital_por_simbolo.values()),
                "capital_inicial": sum(self.capital_inicial_diario.values()),
            }

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
            if pd.to_datetime(v.get("timestamp")).timestamp() * 1000 >= limite
            and v.get("estrategias_activas")
        )

    def _obtener_historico(self, symbol: str) -> pd.DataFrame | None:
        """Devuelve el DataFrame de histórico para ``symbol`` usando caché."""
        df = self.historicos.get(symbol)
        if df is None:
            archivo = f"datos/{symbol.replace('/', '_').lower()}_1m.parquet"
            try:
                df = pd.read_parquet(archivo)
                self.historicos[symbol] = df
            except Exception as e:
                log.debug(f"No se pudo cargar histórico para {symbol}: {e}")
                self.historicos[symbol] = None
                return None
        return df
        
    def _calcular_correlaciones(self, periodos: int = 1440) -> pd.DataFrame:
        """Calcula correlación histórica de cierres entre símbolos."""
        precios = {}
        for symbol in self.capital_por_simbolo:
            df = self._obtener_historico(symbol)
            if df is not None and "close" in df:
                precios[symbol] = (
                    df["close"].astype(float).tail(periodos).reset_index(drop=True)
                )
        if len(precios) < 2:
            return pd.DataFrame()
        df_precios = pd.DataFrame(precios)
        return df_precios.corr()
    
    # Helpers de soporte -------------------------------------------------

    def _rechazo(
        self,
        symbol: str,
        motivo: str,
        puntaje: float | None = None,
        peso_total: float | None = None,
        estrategias: list[str] | dict | None = None,
    ) -> None:
        """Centraliza los mensajes de descartes de entrada."""
        mensaje = f"🔴 RECHAZO: {symbol} | Causa: {motivo}"
        if puntaje is not None:
            mensaje += f" | Puntaje: {puntaje:.2f}"
        if peso_total is not None:
            mensaje += f" | Peso: {peso_total:.2f}"
        if estrategias:
            estr = estrategias
            if isinstance(estr, dict):
                estr = list(estr.keys())
            mensaje += f" | Estrategias: {estr}"
        log.info(mensaje)

        registro = {
            "symbol": symbol,
            "motivo": motivo,
            "puntaje": puntaje,
            "peso_total": peso_total,
            "estrategias": (
                ",".join(
                    estrategias.keys() if isinstance(estrategias, dict) else estrategias
                )
                if estrategias
                else ""
            ),
        }
        fecha = datetime.utcnow().strftime("%Y%m%d")
        archivo = os.path.join(
            "logs/rechazos", f"{symbol.replace('/', '_')}_{fecha}.csv"
        )
        df = pd.DataFrame([registro])
        modo = "a" if os.path.exists(archivo) else "w"
        df.to_csv(archivo, mode=modo, header=not os.path.exists(archivo), index=False)
        registro_metrico.registrar("rechazo", registro)

        try:
            registrar_auditoria(
                symbol=symbol,
                evento="Entrada rechazada",
                resultado="rechazo",
                estrategias_activas=estrategias,
                score=puntaje,
                razon=motivo,
                capital_actual=self.capital_por_simbolo.get(symbol, 0.0),
                config_usada=self.config_por_simbolo.get(symbol, {}),
            )
        except Exception as e:  # noqa: BLE001
            log.debug(f"No se pudo registrar auditoría de rechazo: {e}")

    def _validar_puntaje(self, symbol: str, puntaje: float, umbral: float) -> bool:
        """Comprueba si ``puntaje`` supera ``umbral``."""
        diferencia = umbral - puntaje
        metricas_tracker.registrar_diferencia_umbral(diferencia)
        if puntaje < umbral:
            log.debug(f"🚫 {symbol}: puntaje {puntaje:.2f} < umbral {umbral:.2f}")
            metricas_tracker.registrar_filtro("umbral")
            return False
        return True

    async def _validar_diversidad(
        self,
        symbol: str,
        peso_total: float,
        peso_min_total: float,
        estrategias_activas: Dict[str, float],
        diversidad_min: int,
        estrategias_disponibles: dict,
        df: pd.DataFrame,
    ) -> bool:
        """Verifica que la diversidad y el peso total sean suficientes."""
        diversidad = len(estrategias_activas)
        
        if self.modo_capital_bajo:
            euros = 0
            if self.modo_real and self.cliente:
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
                f"Diversidad {diversidad} < {diversidad_min} o peso {peso_total:.2f} < {peso_min_total:.2f}",
                peso_total=peso_total,
            )
            metricas_tracker.registrar_filtro("diversidad")
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
        estrategias: Dict[str, bool],
    ) -> tuple[bool, float, float]:
        """Evalúa si las señales persistentes son suficientes para entrar."""
        ventana_close = df["close"].tail(10)
        media_close = np.mean(ventana_close)
        if np.isnan(media_close) or media_close == 0:
            log.debug(f"⚠️ {symbol}: Media de cierre inválida para persistencia")
            return False

        repetidas = coincidencia_parcial(estado.buffer, pesos_symbol, ventanas=5)
        minimo = calcular_persistencia_minima(
            symbol,
            df,
            tendencia_actual,
            base_minimo=self.persistencia.minimo,
        )

        log.info(
            f"Persistencia detectada {repetidas:.2f} | Mínimo requerido {minimo:.2f}"
        )

        
        if repetidas < minimo:
            self._rechazo(
                symbol,
                f"Persistencia {repetidas:.2f} < {minimo}",
                puntaje=puntaje,
                estrategias=list(estrategias.keys()),
            )
            metricas_tracker.registrar_filtro("persistencia")
            return False, repetidas, minimo

        if repetidas < 1 and puntaje < 1.2 * umbral:
            self._rechazo(
                symbol,
                f"{repetidas:.2f} coincidencia y puntaje débil ({puntaje:.2f})",
                puntaje=puntaje,
                estrategias=list(estrategias.keys()),
            )
            return False, repetidas, minimo
        elif repetidas < 1:
            log.info(
                f"⚠️ Entrada débil en {symbol}: Coincidencia {repetidas:.2f} insuficiente pero puntaje alto ({puntaje}) > Umbral {umbral} — Permitida."
            )
            metricas_tracker.registrar_filtro("persistencia")
        return True, repetidas, minimo
    
    def _tendencia_persistente(
        self, symbol: str, df: pd.DataFrame, tendencia: str, velas: int = 3
    ) -> bool:
        if len(df) < 30 + velas:
            return False
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
        duracion = cierre.get("duracion", 0)
        retorno = abs(cierre.get("retorno_total", 0))
        velas_requeridas = 3 + min(int(duracion // 30), 3)
        if retorno > 0.05:
            velas_requeridas += 1
        df_post = df[pd.to_datetime(df["timestamp"]) > cierre_dt]
        if len(df_post) < velas_requeridas:
            log.info(
                f"⏳ {symbol}: esperando confirmación de tendencia {len(df_post)}/{velas_requeridas}"
            )
            return False
        if not self._tendencia_persistente(
            symbol, df_post, tendencia, velas=velas_requeridas
        ):
            log.info(f"⏳ {symbol}: tendencia {tendencia} no persistente tras cierre")
            return False

        precio_salida = cierre.get("precio")
        if precio_salida is not None and abs(precio - precio_salida) <= precio * 0.001:
            log.info(f"🚫 {symbol}: precio de entrada similar al de salida anterior")
            return False

        return True
    
    def _calcular_score_tecnico(
        self,
        df: pd.DataFrame,
        rsi: float | None,
        momentum: float | None,
        tendencia: str,
        direccion: str,
    ) -> tuple[float, dict]:
        """Calcula un puntaje técnico simple a partir de varios indicadores."""

        slope = calcular_slope(df)

        resultados = {
            "RSI": False,
            "Momentum": False,
            "Slope": False,
            "Tendencia": False,
        }

        if rsi is not None:
            if direccion == "long":
                resultados["RSI"] = rsi > 50
            else:
                resultados["RSI"] = rsi < 50

        if momentum is not None:
            resultados["Momentum"] = abs(momentum) > 0.001

        resultados["Slope"] = slope > 0.01

        if direccion == "long":
            resultados["Tendencia"] = tendencia in {"alcista", "lateral"}
        else:
            resultados["Tendencia"] = tendencia in {"bajista", "lateral"}

        score_total = sum(
            PESOS_SCORE_TECNICO.get(k, 1.0) for k, v in resultados.items() if v
        )

        log.info(
            "📊 Score técnico: %.2f | RSI: %s (%.2f), Momentum: %s (%.4f), Slope: %s (%.4f), Tendencia: %s",
            score_total,
            "✅" if resultados["RSI"] else "❌",
            rsi if rsi is not None else 0.0,
            "✅" if resultados["Momentum"] else "❌",
            momentum if momentum is not None else 0.0,
            "✅" if resultados["Slope"] else "❌",
            slope,
            "✅" if resultados["Tendencia"] else "❌",
        )

        return float(score_total), resultados

    def _hay_contradicciones(
        self,
        df: pd.DataFrame,
        rsi: float | None,
        momentum: float | None,
        direccion: str,
        score: float,
    ) -> bool:
        """Detecta si existen contradicciones fuertes en las señales."""

        if direccion == "long":
            if rsi is not None and rsi > 70:
                return True
            if df["close"].iloc[-1] >= df["close"].iloc[-10] * 1.05:
                return True
            if (
                momentum is not None
                and momentum < 0
                and score >= self.umbral_score_tecnico
            ):
                return True
        else:
            if rsi is not None and rsi < 30:
                return True
            if df["close"].iloc[-1] <= df["close"].iloc[-10] * 0.95:
                return True
            if (
                momentum is not None
                and momentum > 0
                and score >= self.umbral_score_tecnico
            ):
                return True
        return False

    def _validar_temporalidad(self, df: pd.DataFrame, direccion: str) -> bool:
        """Verifica que las señales no estén perdiendo fuerza."""

        rsi_series = calcular_rsi(df, serie_completa=True)
        if rsi_series is None or len(rsi_series) < 3:
            return True
        r = rsi_series.iloc[-3:]
        if direccion == "long" and not (r.iloc[-1] > r.iloc[-2] > r.iloc[-3]):
            return False
        if direccion == "short" and not (r.iloc[-1] < r.iloc[-2] < r.iloc[-3]):
            return False

        slope3 = calcular_slope(df, periodo=3)
        slope5 = calcular_slope(df, periodo=5)
        if direccion == "long" and not (slope3 > slope5):
            return False
        if direccion == "short" and not (slope3 < slope5):
            return False
        return True

    def _registrar_rechazo_tecnico(
        self,
        symbol: str,
        score: float,
        puntos: dict,
        tendencia: str,
        precio: float,
        motivo: str,
        estrategias: dict | None = None,
    ) -> None:
        """Guarda detalles de rechazos técnicos en un CSV."""

        if not self.registro_tecnico_csv:
            return
        fila = {
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "puntaje_total": score,
            "indicadores_fallidos": ",".join([k for k, v in puntos.items() if not v]),
            "estado_mercado": tendencia,
            "precio": precio,
            "motivo": motivo,
            "estrategias": ",".join(estrategias.keys()) if estrategias else "",
        }
        df = pd.DataFrame([fila])
        modo = "a" if os.path.exists(self.registro_tecnico_csv) else "w"
        df.to_csv(
            self.registro_tecnico_csv,
            mode=modo,
            header=not os.path.exists(self.registro_tecnico_csv),
            index=False,
        )

    async def evaluar_condiciones_entrada(self, symbol: str, df: pd.DataFrame) -> None:
        """Evalúa y ejecuta una entrada si todas las condiciones se cumplen."""

        estado = self.estado[symbol]
        config_actual = self.config_por_simbolo.get(symbol, {})
        dinamica = adaptar_configuracion(symbol, df)
        if dinamica:
            config_actual.update(dinamica)
        config_actual = adaptar_configuracion_base(symbol, df, config_actual)
        self.config_por_simbolo[symbol] = config_actual

        tendencia_actual = self.estado_tendencia.get(symbol)
        if not tendencia_actual:
            tendencia_actual, _ = detectar_tendencia(symbol, df)
            self.estado_tendencia[symbol] = tendencia_actual

        resultado = self.engine.evaluar_entrada(
            symbol,
            df,
            tendencia=tendencia_actual,
            config=config_actual,
            pesos_symbol=self.pesos_por_simbolo.get(symbol, {}),
        )
        estrategias = resultado.get("estrategias_activas", {})
        estado.buffer[-1]["estrategias_activas"] = estrategias
        self.persistencia.actualizar(symbol, estrategias)

        precio_actual = float(df["close"].iloc[-1])

        if not resultado.get("permitido"):
            if self.usar_score_tecnico:
                rsi = resultado.get("rsi")
                mom = resultado.get("momentum")
                score, puntos = self._calcular_score_tecnico(
                    df,
                    rsi,
                    mom,
                    tendencia_actual,
                    "short" if tendencia_actual == "bajista" else "long",
                )
                self._registrar_rechazo_tecnico(
                    symbol,
                    score,
                    puntos,
                    tendencia_actual,
                    precio_actual,
                    resultado.get("motivo_rechazo", "desconocido"),
                    estrategias,
                )
            self._rechazo(
                symbol,
                resultado.get("motivo_rechazo", "desconocido"),
                puntaje=resultado.get("score_total"),
                estrategias=list(estrategias.keys()),
            )
            return

        info = await self.evaluar_condiciones_de_entrada(symbol, df, estado)
        if not info:
            self._rechazo(
                symbol,
                "filtros_post_engine",
                puntaje=resultado.get("score_total"),
                estrategias=list(estrategias.keys()),
            )
            return

        await self._abrir_operacion_real(**info)

    async def _abrir_operacion_real(
        self,
        symbol: str,
        precio: float,
        sl: float,
        tp: float,
        estrategias: Dict | List,
        tendencia: str,
        direccion: str,
        puntaje: float = 0.0,
        umbral: float = 0.0,
        detalles_tecnicos: dict | None = None,
        **kwargs,  # <- acepta parámetros adicionales sin fallar
    ) -> None:
        cantidad_total = await self.capital_manager.calcular_cantidad_async(
            symbol, precio
        )
        if cantidad_total <= 0:
            return
        fracciones = self.piramide_fracciones
        cantidad = cantidad_total / fracciones
        if isinstance(estrategias, dict):
            estrategias_dict = estrategias
        else:
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            estrategias_dict = {e: pesos_symbol.get(e, 0.0) for e in estrategias}
        await self.orders.abrir_async(
            symbol,
            precio,
            sl,
            tp,
            estrategias_dict,
            tendencia,
            direccion,
            cantidad,
            puntaje,
            umbral,
            objetivo=cantidad_total,
            fracciones=fracciones,
            detalles_tecnicos=detalles_tecnicos or {},
        )
        estrategias_list = list(estrategias_dict.keys())
        log.info(
            f"🟢 ENTRADA: {symbol} | Puntaje: {puntaje:.2f} / Umbral: {umbral:.2f} | Estrategias: {estrategias_list}"
        )
        registro_metrico.registrar(
            "entrada",
            {
                "symbol": symbol,
                "puntaje": puntaje,
                "umbral": umbral,
                "estrategias": ",".join(estrategias_list),
                "precio": precio,
            },
        )
        try:
            registrar_auditoria(
                symbol=symbol,
                evento="Entrada",
                resultado="ejecutada",
                estrategias_activas=estrategias_dict,
                score=puntaje,
                tendencia=tendencia,
                capital_actual=self.capital_por_simbolo.get(symbol, 0.0),
                config_usada=self.config_por_simbolo.get(symbol, {}),
            )
        except Exception as e:  # noqa: BLE001
            log.debug(f"No se pudo registrar auditoría de entrada: {e}")

    async def _verificar_salidas(self, symbol: str, df: pd.DataFrame) -> None:
        await verificar_salidas(self, symbol, df)

    async def evaluar_condiciones_de_entrada(
        self, symbol: str, df: pd.DataFrame, estado: EstadoSimbolo
        ) -> dict | None:
        if not self._validar_config(symbol):
            return None
        return await verificar_entrada(self, symbol, df, estado)



    async def ejecutar(self) -> None:
        """Inicia el procesamiento de todos los símbolos."""
        async def handle(candle: dict) -> None:
            await self._procesar_vela(candle)

        async def handle_context(symbol: str, score: float) -> None:
            log.debug(f"🔁 Contexto actualizado {symbol}: {score:.2f}")

        symbols = list(self.estado.keys())
        await self._precargar_historico(velas=60)

        def _log_fallo_task(task: asyncio.Task):
            if task.cancelled():
                log.warning("⚠️ Una tarea fue cancelada.")
            elif task.exception():
                log.error(f"❌ Error en tarea asincrónica: {task.exception()}")

        self._task = asyncio.create_task(self.data_feed.escuchar(symbols, handle))
        self._task.add_done_callback(_log_fallo_task)

        self._task_estado = asyncio.create_task(monitorear_estado_periodicamente(self))
        self._task_estado.add_done_callback(_log_fallo_task)

        self._task_contexto = asyncio.create_task(
            self.context_stream.escuchar(symbols, handle_context)
        )
        self._task_contexto.add_done_callback(_log_fallo_task)
        self._task_flush = asyncio.create_task(real_orders.flush_periodico())
        self._task_flush.add_done_callback(_log_fallo_task)
        if "PYTEST_CURRENT_TEST" not in os.environ:
            self._task_aprendizaje = asyncio.create_task(self._ciclo_aprendizaje())
            self._task_aprendizaje.add_done_callback(_log_fallo_task)

        try:
            tareas = [
                self._task,
                self._task_estado,
                self._task_contexto,
                self._task_flush,
            ]
            if self._task_aprendizaje:
                tareas.append(self._task_aprendizaje)
            await asyncio.gather(*tareas)
        except Exception as e:
            log.error(f"❌ Error inesperado en ejecución de tareas: {e}")
            
        tareas = [self._task, self._task_estado, self._task_contexto, self._task_flush]
        if self._task_aprendizaje:
            tareas.append(self._task_aprendizaje)
        await asyncio.gather(*tareas)

    async def _procesar_vela(self, vela: dict) -> None:
        symbol = vela.get("symbol")
        if not self._validar_config(symbol):
            return
        
        await procesar_vela(self, vela)
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

        if self._task_contexto:
            await self.context_stream.detener()
            self._task_contexto.cancel()
            try:
                await self._task_contexto
            except asyncio.CancelledError:
                pass
            
        if self._task_aprendizaje:
            self._task_aprendizaje.cancel()
            try:
                await self._task_aprendizaje
            except asyncio.CancelledError:
                pass

        self._guardar_estado_persistente()


    def _guardar_estado_persistente(self) -> None:
        """Guarda historial de cierres y capital en ``estado/``."""
        try:
            os.makedirs("estado", exist_ok=True)
            with open("estado/historial_cierres.json", "w") as f:
                json.dump(self.historial_cierres, f, indent=2)
            with open("estado/capital.json", "w") as f:
                json.dump(self.capital_por_simbolo, f, indent=2)
        except Exception as e:  # noqa: BLE001
            log.warning(f"⚠️ Error guardando estado persistente: {e}")


    def _cargar_estado_persistente(self) -> None:
        """Carga el estado previo de ``estado/`` si existe."""
        try:
            if os.path.exists("estado/historial_cierres.json"):
                with open("estado/historial_cierres.json") as f:
                    contenido = f.read()
                if contenido.strip():
                    try:
                        data = json.loads(contenido)
                    except json.JSONDecodeError as e:
                        log.warning(f"⚠️ Error leyendo historial_cierres.json: {e}")
                        data = {}
                    if isinstance(data, dict):
                        self.historial_cierres.update(data)
            if os.path.exists("estado/capital.json"):
                with open("estado/capital.json") as f:
                    contenido = f.read()
                if contenido.strip():
                    try:
                        data = json.loads(contenido)
                    except json.JSONDecodeError as e:
                        log.warning(f"⚠️ Error leyendo capital.json: {e}")
                        data = {}
                    if isinstance(data, dict):
                        self.capital_por_simbolo.update(
                            {k: float(v) for k, v in data.items()}
                        )
        except Exception as e:  # noqa: BLE001
            log.warning(f"⚠️ Error cargando estado persistente: {e}")

    def _validar_config(self, symbol: str) -> bool:
        """Valida que exista configuración para ``symbol``."""
        cfg = self.config_por_simbolo.get(symbol)
        if not isinstance(cfg, dict):
            log.error(f"⚠️ Configuración no encontrada para {symbol}")
            return False
        return True
