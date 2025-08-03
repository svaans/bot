"""Controlador principal del bot modular."""
from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass, replace, field
from typing import Dict, List, Callable, Awaitable, Any
from collections import OrderedDict
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
from core.event_bus import EventBus
from binance_api.cliente import (
    crear_cliente,
    fetch_balance_async,
    fetch_ohlcv_async,
    filtrar_simbolos_activos,
)
from core.utils.utils import leer_reporte_seguro
from core.strategies import cargar_pesos_estrategias
from core.risk import calcular_fraccion_kelly
from core.data import PersistenciaTecnica, coincidencia_parcial, calcular_persistencia_minima
from core.metricas_semanales import metricas_tracker, metricas_semanales
from learning.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from core.utils.utils import configurar_logger
from core.monitor_estado_bot import monitorear_estado_periodicamente
from core.contexto_externo import StreamContexto
from core.orders import real_orders
from core.config_manager.dinamica import adaptar_configuracion
from ccxt.base.errors import BaseError
from core.reporting import reporter_diario
from core.registro_metrico import registro_metrico
from core.supervisor import supervised_task, task_heartbeat, tick, last_alive, data_heartbeat
from learning.aprendizaje_en_linea import registrar_resultado_trade
from learning.aprendizaje_continuo import ejecutar_ciclo as ciclo_aprendizaje
from core.strategies.exit.gestor_salidas import verificar_filtro_tecnico
from core.strategies.tendencia import detectar_tendencia
from core.strategies.entry.validador_entradas import evaluar_validez_estrategica
from indicators.rsi import calcular_rsi
from indicators.momentum import calcular_momentum
from indicators.slope import calcular_slope
from core.strategies.evaluador_tecnico import actualizar_pesos_tecnicos
from core.auditoria import registrar_auditoria
from indicators.atr import calcular_atr
from core.strategies.exit.verificar_salidas import verificar_salidas
from core.strategies.entry.verificar_entradas import verificar_entrada
from core.procesar_vela import procesar_vela
from core.scoring import calcular_score_tecnico
from binance_api.cliente import fetch_ticker_async
log = configurar_logger('trader')
LOG_DIR = os.getenv('LOG_DIR', 'logs')
PESOS_SCORE_TECNICO = {'RSI': 1.0, 'Momentum': 0.5, 'Slope': 1.0,
    'Tendencia': 1.0}


@dataclass
class EstadoSimbolo:
    buffer: List[dict]
    estrategias_buffer: List[dict] = field(default_factory=list)
    ultimo_umbral: float = 0.0
    ultimo_timestamp: int | None = None
    tendencia_detectada: str | None = None
    timeouts_salidas: int = 0


class Trader:
    """Orquesta el flujo de datos y las operaciones de trading."""

    def __init__(self, config: Config) ->None:
        log.info('➡️ Entrando en __init__()')
        activos, inactivos = filtrar_simbolos_activos(config.symbols, config)
        for sym in inactivos:
            log.warning(
                f'⚠️ Símbolo {sym} no listado o inactivo en Binance. Se desactivará.'
            )
        if not activos:
            raise ValueError('No hay símbolos activos disponibles.')
        config = replace(config, symbols=activos)
        self.config = config
        self.heartbeat_interval = getattr(config, 'heartbeat_interval', 60)
        self.data_feed = DataFeed(
            config.intervalo_velas,
            getattr(config, 'monitor_interval', 5),
            getattr(config, 'max_stream_restarts', 5),
            getattr(config, 'inactivity_intervals', 4),
        )
        self.engine = StrategyEngine()
        self.bus = EventBus()
        self.risk = RiskManager(config.umbral_riesgo_diario, self.bus)
        self.notificador = NotificationManager(config.telegram_token,
            config.telegram_chat_id, bus=self.bus)
        self.modo_real = getattr(config, 'modo_real', False)
        self.orders = PositionManager(self.modo_real, bus=self.bus)
        self.cliente = crear_cliente(config) if self.modo_real else None
        if not self.modo_real:
            log.info(
                '🧪 Modo simulado activado. No se inicializará cliente Binance')
        self._markets = None
        self.modo_capital_bajo = config.modo_capital_bajo
        self.persistencia = PersistenciaTecnica(config.persistencia_minima,
            config.peso_extra_persistencia)
        os.makedirs(os.path.join(LOG_DIR, 'rechazos'), exist_ok=True)
        os.makedirs(os.path.dirname(config.registro_tecnico_csv), exist_ok=True
            )
        self.umbral_score_tecnico = config.umbral_score_tecnico
        self.usar_score_tecnico = getattr(config, 'usar_score_tecnico', True)
        self.contradicciones_bloquean_entrada = (config.
            contradicciones_bloquean_entrada)
        self.registro_tecnico_csv = config.registro_tecnico_csv
        self._rechazos_buffer: list[dict] = []
        self._rechazos_batch_size = 10
        self._rechazos_intervalo_flush = 5
        # Cache limitada para históricos de velas
        self.historicos: OrderedDict[str, pd.DataFrame] = OrderedDict()
        self.max_historicos_cache = getattr(config, 'max_historicos_cache', 20)
        self.max_filas_historico = getattr(config, 'max_filas_historico', 1440)
        self.fraccion_kelly = calcular_fraccion_kelly()
        factor_kelly = self.risk.multiplicador_kelly()
        self.fraccion_kelly *= factor_kelly
        factor_vol = 1.0
        try:
            factores = []
            for sym in config.symbols:
                df = self._obtener_historico(sym)
                if df is None or 'close' not in df:
                    continue
                cambios = df['close'].pct_change().dropna()
                if cambios.empty:
                    continue
                volatilidad_actual = cambios.tail(1440).std()
                volatilidad_media = cambios.std()
                factores.append(self.risk.factor_volatilidad(float(
                    volatilidad_actual), float(volatilidad_media)))
            if factores:
                factor_vol = min(factores)
            intraday_factors = []
            for sym in config.symbols:
                if sym not in self.historicos:
                    continue
                cambios_dia = self.historicos[sym]['close'].pct_change().dropna()
                if cambios_dia.empty:
                    continue
                vol_dia = cambios_dia.tail(60).std()
                vol_hist = cambios_dia.std()
                if vol_hist and vol_dia > vol_hist * 1.5:
                    intraday_factors.append(0.8)
            if intraday_factors:
                factor_vol *= min(intraday_factors)
        except Exception as e:
            log.debug(f'No se pudo calcular factor de volatilidad: {e}')
        self.fraccion_kelly *= factor_vol
        log.info(
            f'⚖️ Fracción Kelly: {self.fraccion_kelly:.4f} (x{factor_kelly:.3f}, x{factor_vol:.3f})'
            )
        self.piramide_fracciones = max(1, config.fracciones_piramide)
        self.reserva_piramide = max(0.0, min(1.0, config.reserva_piramide))
        self.umbral_piramide = max(0.0, config.umbral_piramide)
        self.riesgo_maximo_diario = 1.0
        self.capital_manager = CapitalManager(config, self.cliente, self.risk,
            self.fraccion_kelly, bus=self.bus)
        self.capital_por_simbolo = self.capital_manager.capital_por_simbolo
        self.capital_inicial_diario = (self.capital_manager.
            capital_inicial_diario)
        self.reservas_piramide = self.capital_manager.reservas_piramide
        self.fecha_actual = self.capital_manager.fecha_actual
        self.estado: Dict[str, EstadoSimbolo] = {s: EstadoSimbolo([]) for s in
            config.symbols}
        self.estado_tendencia: Dict[str, str] = {}
        self.config_por_simbolo: Dict[str, dict] = {s: {} for s in config.
            symbols}
        try:
            self.pesos_por_simbolo: Dict[str, Dict[str, float]
                ] = cargar_pesos_estrategias()
        except ValueError as e:
            log.error(f'❌ {e}')
            raise
        self.historial_cierres: Dict[str, dict] = {}
        self._tareas: dict[str, asyncio.Task] = {}
        self._factories: dict[str, Callable[[], Awaitable]] = {}
        self._stop_event = asyncio.Event()
        self._cerrado = False
        self.context_stream = StreamContexto()
        try:
            self.orders.ordenes = real_orders.obtener_todas_las_ordenes()
            if self.modo_real and not self.orders.ordenes:
                self.orders.ordenes = real_orders.sincronizar_ordenes_binance(
                    config.symbols)
        except Exception as e:
            log.warning(
                f'⚠️ Error cargando órdenes previas desde la base de datos: {e}'
                )
            raise
        if self.orders.ordenes:
            log.warning(
                '⚠️ Órdenes abiertas encontradas al iniciar. Serán monitoreadas.'
                )
        if 'PYTEST_CURRENT_TEST' not in os.environ:
            self._cargar_estado_persistente()
        else:
            log.debug('🔍 Modo prueba: se omite carga de estado persistente')

    def _load_json_file(self, path: str) ->dict[str, Any]:
        log.info('➡️ Entrando en _load_json_file()')
        """Return file content as dict or empty dict on error."""
        if not os.path.exists(path):
            return {}
        try:
            with open(path) as f:
                contenido = f.read()
        except OSError as e:
            log.warning(f'⚠️ Error abriendo {path}: {e}')
            return {}
        if not contenido.strip():
            return {}
        try:
            data = json.loads(contenido)
        except json.JSONDecodeError as e:
            log.warning(f'⚠️ Error decodificando {path}: {e}')
            return {}
        return data if isinstance(data, dict) else {}

    def _save_json_file(self, path: str, data: Any) ->None:
        log.info('➡️ Entrando en _save_json_file()')
        """Guardar ``data`` en ``path`` silenciosamente."""
        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        except OSError as e:
            log.warning(f'⚠️ Error guardando {path}: {e}')

    def _log_impacto_sl(self, orden, precio: float) ->None:
        log.info('➡️ Entrando en _log_impacto_sl()')
        """Registra el impacto de evitar el stop loss."""
        os.makedirs('logs', exist_ok=True)
        for ev in orden.sl_evitar_info:
            sl_val = ev.get('sl', 0.0)
            peor = precio < sl_val if orden.direccion in ('long', 'compra'
                ) else precio > sl_val
            mensaje = (
                f'❗ Evitar SL en {orden.symbol} resultó en pérdida mayor ({precio:.2f} vs {sl_val:.2f})'
                 if peor else
                f'👍 Evitar SL en {orden.symbol} fue beneficioso ({precio:.2f} vs {sl_val:.2f})'
                )
            with open(os.path.join(LOG_DIR, 'impacto_sl.log'), 'a') as f:
                f.write(mensaje + '\n')
            log.info(mensaje)
        orden.sl_evitar_info = []

    async def cerrar_operacion(self, symbol: str, precio: float, motivo: str
        ) ->None:
        log.info('➡️ Entrando en cerrar_operacion()')
        """Cierra una orden y actualiza los pesos si corresponden."""
        fut = asyncio.get_running_loop().create_future()
        await self.bus.publish('cerrar_orden', {'symbol': symbol, 'precio': precio, 'motivo': motivo, 'future': fut})
        try:
            confirmado = await asyncio.wait_for(
                fut, timeout=self.config.timeout_bus_eventos
            )
        except asyncio.TimeoutError:
            log.error(f'⏰ Timeout cerrando {symbol}')
            return
        if not confirmado:
            log.debug(f'🔁 Intento duplicado de cierre ignorado para {symbol}')
            return
        actualizar_pesos_estrategias_symbol(symbol)
        try:
            self.pesos_por_simbolo = cargar_pesos_estrategias()
        except ValueError as e:
            log.error(f'❌ {e}')
            return
        log.info(f"✅ Orden cerrada: {symbol} a {precio:.2f}€ por '{motivo}'")

    async def _cerrar_y_reportar(self, orden, precio: float, motivo: str,
        tendencia: (str | None)=None, df: (pd.DataFrame | None)=None) ->None:
        log.info('➡️ Entrando en _cerrar_y_reportar()')
        """Cierra ``orden`` y registra la operación para el reporte diario."""
        retorno_total = (precio - orden.precio_entrada
            ) / orden.precio_entrada if orden.precio_entrada else 0.0
        info = orden.to_dict()
        info.update({'precio_cierre': precio, 'fecha_cierre': datetime.
            utcnow().isoformat(), 'motivo_cierre': motivo, 'retorno_total':
            retorno_total, 'capital_inicial': self.capital_por_simbolo.get(
            orden.symbol, 0.0)})
        fut = asyncio.get_running_loop().create_future()
        await self.bus.publish('cerrar_orden', {'symbol': orden.symbol, 'precio': precio, 'motivo': motivo, 'future': fut})
        try:
            confirmado = await asyncio.wait_for(
                fut, timeout=self.config.timeout_bus_eventos
            )
        except asyncio.TimeoutError:
            log.error(f'⏰ Timeout confirmando cierre de {orden.symbol}')
            return False
        if not confirmado:
            log.warning(
                f'❌ No se pudo confirmar el cierre de {orden.symbol}. Se omitirá el registro.'
            )
            return False
        capital_inicial = self.capital_por_simbolo.get(orden.symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        capital_final = capital_inicial + ganancia
        self.capital_por_simbolo[orden.symbol] = capital_final
        info['capital_final'] = capital_final
        if getattr(orden, 'sl_evitar_info', None):
            self._log_impacto_sl(orden, precio)
        reporter_diario.registrar_operacion(info)
        registrar_resultado_trade(orden.symbol, info, retorno_total)
        try:
            if orden.detalles_tecnicos:
                actualizar_pesos_tecnicos(orden.symbol, orden.
                    detalles_tecnicos, retorno_total)
        except Exception as e:
            log.debug(f'No se pudo actualizar pesos tecnicos: {e}')
        actualizar_pesos_estrategias_symbol(orden.symbol)
        try:
            self.pesos_por_simbolo = cargar_pesos_estrategias()
        except ValueError as e:
            log.error(f'❌ {e}')
            return False
        duracion = 0.0
        try:
            apertura = datetime.fromisoformat(orden.timestamp)
            duracion = (datetime.utcnow() - apertura).total_seconds() / 60
        except Exception:
            pass
        prev = self.historial_cierres.get(orden.symbol, {})
        self.historial_cierres[orden.symbol] = {'timestamp': datetime.
            utcnow().isoformat(), 'motivo': motivo.lower().strip(), 'velas':
            0, 'precio': precio, 'tendencia': tendencia, 'duracion':
            duracion, 'retorno_total': retorno_total}
        if retorno_total < 0:
            fecha_hoy = datetime.utcnow().date().isoformat()
            if prev.get('fecha_perdidas') != fecha_hoy:
                perdidas = 0
            else:
                perdidas = prev.get('perdidas_consecutivas', 0)
            perdidas += 1
            self.historial_cierres[orden.symbol]['perdidas_consecutivas'
                ] = perdidas
            self.historial_cierres[orden.symbol]['fecha_perdidas'] = fecha_hoy
        else:
            self.historial_cierres[orden.symbol]['perdidas_consecutivas'] = 0
        log.info(
            f'✅ CIERRE {motivo.upper()}: {orden.symbol} | Beneficio: {ganancia:.2f} €'
            )
        registro_metrico.registrar('cierre', {'symbol': orden.symbol,
            'motivo': motivo, 'retorno': retorno_total, 'beneficio': ganancia})
        self._registrar_salida_profesional(orden.symbol, {'tipo_salida':
            motivo, 'estrategias_activas': orden.estrategias_activas,
            'score_tecnico_al_cierre': self._calcular_score_tecnico(df,
            calcular_rsi(df), calcular_momentum(df), tendencia or '', orden
            .direccion)[0] if df is not None else 0.0, 'capital_final':
            capital_final, 'configuracion_usada': self.config_por_simbolo.
            get(orden.symbol, {}), 'tiempo_operacion': duracion,
            'beneficio_relativo': retorno_total})
        metricas = self._metricas_recientes()
        await self.bus.publish('ajustar_riesgo', metricas)
        try:
            rsi_val = calcular_rsi(df) if df is not None else None
            score, _ = self._calcular_score_tecnico(df, rsi_val,
                calcular_momentum(df), tendencia or '', orden.direccion
                ) if df is not None else (None, None)
            registrar_auditoria(symbol=orden.symbol, evento=motivo,
                resultado='ganancia' if retorno_total > 0 else 'pérdida',
                estrategias_activas=orden.estrategias_activas, score=score,
                rsi=rsi_val, tendencia=tendencia, capital_actual=
                capital_final, config_usada=self.config_por_simbolo.get(
                orden.symbol, {}))
        except Exception as e:
            log.debug(f'No se pudo registrar auditoría de cierre: {e}')
        return True

    def _registrar_salida_profesional(self, symbol: str, info: dict) ->None:
        log.info('➡️ Entrando en _registrar_salida_profesional()')
        archivo = 'reportes_diarios/registro_salidas.parquet'
        os.makedirs(os.path.dirname(archivo), exist_ok=True)
        data = info.copy()
        data['symbol'] = symbol
        data['timestamp'] = datetime.utcnow().isoformat()
        if isinstance(data.get('estrategias_activas'), dict):
            data['estrategias_activas'] = json.dumps(data[
                'estrategias_activas'])
        try:
            if os.path.exists(archivo):
                df = pd.read_parquet(archivo)
                df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
            else:
                df = pd.DataFrame([data])
            df.to_parquet(archivo, index=False)
        except Exception as e:
            log.warning(f'⚠️ Error registrando salida en {archivo}: {e}')

    async def _cerrar_parcial_y_reportar(self, orden, cantidad: float,
        precio: float, motivo: str, df: (pd.DataFrame | None)=None) ->bool:
        log.info('➡️ Entrando en _cerrar_parcial_y_reportar()')
        """Cierre parcial de ``orden`` y registro en el reporte."""
        fut = asyncio.get_running_loop().create_future()
        await self.bus.publish('cerrar_parcial', {'symbol': orden.symbol, 'cantidad': cantidad, 'precio': precio, 'motivo': motivo, 'future': fut})
        try:
            confirmado = await asyncio.wait_for(
                fut, timeout=self.config.timeout_bus_eventos
            )
        except asyncio.TimeoutError:
            log.error(f'⏰ Timeout confirmando cierre parcial de {orden.symbol}')
            return False
        if not confirmado:
            log.warning(
                f'❌ No se pudo confirmar el cierre parcial de {orden.symbol}. Se omitirá el registro.'
            )
            return False
        retorno_unitario = (precio - orden.precio_entrada
            ) / orden.precio_entrada if orden.precio_entrada else 0.0
        fraccion = cantidad / orden.cantidad if orden.cantidad else 0.0
        retorno_total = retorno_unitario * fraccion
        info = orden.to_dict()
        info.update({'precio_cierre': precio, 'fecha_cierre': datetime.
            utcnow().isoformat(), 'motivo_cierre': motivo, 'retorno_total':
            retorno_total, 'cantidad_cerrada': cantidad, 'capital_inicial':
            self.capital_por_simbolo.get(orden.symbol, 0.0)})
        reporter_diario.registrar_operacion(info)
        registrar_resultado_trade(orden.symbol, info, retorno_total)
        capital_inicial = self.capital_por_simbolo.get(orden.symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        capital_final = capital_inicial + ganancia
        self.capital_por_simbolo[orden.symbol] = capital_final
        info['capital_final'] = capital_final
        log.info(
            f'✅ CIERRE PARCIAL: {orden.symbol} | Beneficio: {ganancia:.2f} €')
        registro_metrico.registrar('cierre_parcial', {'symbol': orden.
            symbol, 'retorno': retorno_total, 'beneficio': ganancia})
        self._registrar_salida_profesional(orden.symbol, {'tipo_salida':
            'parcial', 'estrategias_activas': orden.estrategias_activas,
            'score_tecnico_al_cierre': self._calcular_score_tecnico(df,
            calcular_rsi(df), calcular_momentum(df), orden.tendencia, orden
            .direccion)[0] if df is not None else 0.0,
            'configuracion_usada': self.config_por_simbolo.get(orden.symbol,
            {}), 'tiempo_operacion': 0.0, 'beneficio_relativo': retorno_total})
        try:
            rsi_val = calcular_rsi(df) if df is not None else None
            score, _ = self._calcular_score_tecnico(df, rsi_val,
                calcular_momentum(df), orden.tendencia, orden.direccion
                ) if df is not None else (None, None)
            registrar_auditoria(symbol=orden.symbol, evento=motivo,
                resultado='ganancia' if retorno_total > 0 else 'pérdida',
                estrategias_activas=orden.estrategias_activas, score=score,
                rsi=rsi_val, tendencia=orden.tendencia, capital_actual=
                capital_final, config_usada=self.config_por_simbolo.get(
                orden.symbol, {}))
        except Exception as e:
            log.debug(f'No se pudo registrar auditoría de cierre parcial: {e}')
        return True

    def es_salida_parcial_valida(self, orden, precio_tp: float, config:
        dict, df: pd.DataFrame) ->bool:
        log.info('➡️ Entrando en es_salida_parcial_valida()')
        """Determina si aplicar TP parcial tiene sentido económico."""
        if not config.get('usar_cierre_parcial', False):
            return False
        try:
            inversion = (orden.precio_entrada or 0.0) * (orden.cantidad or 0.0)
            retorno_potencial = (precio_tp - (orden.precio_entrada or 0.0)) * (
                orden.cantidad or 0.0)
        except Exception:
            return False
        if inversion <= config.get('umbral_operacion_grande', 30.0):
            return False
        if retorno_potencial <= config.get('beneficio_minimo_parcial', 5.0):
            return False
        pesos_symbol = self.pesos_por_simbolo.get(orden.symbol, {})
        if not verificar_filtro_tecnico(orden.symbol, df, orden.
            estrategias_activas, pesos_symbol, config=config):
            return False
        return True

    async def _piramidar(self, symbol: str, orden, df: pd.DataFrame) ->None:
        log.info('➡️ Entrando en _piramidar()')
        """Añade posiciones si el precio avanza a favor."""
        if orden.fracciones_restantes <= 0:
            return
        precio_actual = float(df['close'].iloc[-1])
        if precio_actual >= orden.precio_ultima_piramide * (1 + self.
            umbral_piramide):
            cantidad = orden.cantidad / orden.fracciones_totales
            fut = asyncio.get_running_loop().create_future()
            await self.bus.publish('agregar_parcial', {'symbol': symbol, 'precio': precio_actual, 'cantidad': cantidad, 'future': fut})
            try:
                confirmado = await asyncio.wait_for(
                    fut, timeout=self.config.timeout_bus_eventos
                )
            except asyncio.TimeoutError:
                log.error(f'⏰ Timeout agregando parcial en {symbol}')
                return
            if confirmado:
                orden.fracciones_restantes -= 1
                orden.precio_ultima_piramide = precio_actual
                log.info(
                    f'🔼 Pirámide ejecutada en {symbol} @ {precio_actual:.2f}')

    @property
    def ordenes_abiertas(self):
        log.info('➡️ Entrando en ordenes_abiertas()')
        """Compatibilidad con ``monitorear_estado_periodicamente``."""
        return self.orders.ordenes

    def ajustar_capital_diario(self, factor: float=0.2, limite: float=0.3,
        penalizacion_corr: float=0.2, umbral_corr: float=0.8, fecha: (
        datetime.date | None)=None) ->None:
        log.info('➡️ Entrando en ajustar_capital_diario()')
        """Redistribuye el capital según múltiples métricas adaptativas."""
        total = sum(self.capital_por_simbolo.values())
        metricas_globales = self._metricas_recientes()
        semanales = metricas_semanales()
        pesos: dict[str, float] = {}
        senales = {s: self._contar_senales(s) for s in self.capital_por_simbolo
            }
        max_senales = max(senales.values()) if senales else 0
        correlaciones = self._calcular_correlaciones()
        stats = getattr(reporter_diario, 'estadisticas', pd.DataFrame())
        for symbol in self.capital_por_simbolo:
            inicio = self.capital_inicial_diario.get(symbol, self.
                capital_por_simbolo[symbol])
            final = self.capital_por_simbolo[symbol]
            rendimiento = (final - inicio) / inicio if inicio else 0.0
            peso = 1 + factor * rendimiento
            if max_senales > 0:
                peso += 0.2 * senales[symbol] / max_senales
            corr_media = None
            if not correlaciones.empty and symbol in correlaciones.columns:
                corr_series = correlaciones[symbol].drop(labels=[symbol],
                    errors='ignore').abs()
                corr_media = corr_series.mean()
            if corr_media >= umbral_corr:
                peso *= 1 - penalizacion_corr * corr_media
            fila = stats[stats['symbol'] == symbol] if isinstance(stats, pd
                .DataFrame) and 'symbol' in stats.columns else pd.DataFrame()
            drawdown = 0.0
            winrate = 0.0
            ganancia = 0.0
            if not fila.empty:
                drawdown = float(fila['drawdown'].iloc[0])
                operaciones = float(fila['operaciones'].iloc[0])
                wins = float(fila['wins'].iloc[0])
                ganancia = float(fila['retorno_acumulado'].iloc[0])
                winrate = wins / operaciones if operaciones else 0.0
            if not semanales.empty:
                sem = semanales[semanales['symbol'] == symbol]
                if not sem.empty:
                    weekly = float(sem['ganancia_promedio'].iloc[0]) * float(
                        sem['operaciones'].iloc[0])
                    if weekly < -0.05:
                        peso *= 0.5
            if drawdown < 0:
                peso *= 1 + drawdown
            if winrate > 0.6 and ganancia > 0:
                refuerzo = min((winrate - 0.6) * ganancia, 0.3)
                peso *= 1 + refuerzo
            if metricas_globales:
                ganancia_global = metricas_globales.get('ganancia_semana', 0.0)
                drawdown_global = metricas_globales.get('drawdown', 0.0)
                ajuste_global = 1 + ganancia_global + drawdown_global
                peso *= max(0.5, min(1.5, ajuste_global))
            peso = max(1 - limite, min(1 + limite, peso))
            pesos[symbol] = peso
        suma = sum(pesos.values()) or 1
        for symbol in self.capital_por_simbolo:
            self.capital_por_simbolo[symbol] = round(total * pesos[symbol] /
                suma, 2)
        for symbol in self.capital_por_simbolo:
            orden = self.orders.obtener(symbol)
            reserva = 0.0
            if orden and orden.cantidad_abierta > 0 and self.estado[symbol
                ].buffer:
                precio_actual = float(self.estado[symbol].buffer[-1].get(
                    'close', 0))
                if precio_actual > orden.precio_entrada:
                    reserva = self.capital_por_simbolo[symbol
                        ] * self.reserva_piramide
            self.capital_por_simbolo[symbol] -= reserva
            self.reservas_piramide[symbol] = round(reserva, 2)
        self.capital_inicial_diario = self.capital_por_simbolo.copy()
        self.fecha_actual = fecha or datetime.utcnow().date()
        log.info(f'💰 Capital redistribuido: {self.capital_por_simbolo}')

    async def _precargar_historico(self, velas: int=12) ->None:
        log.info('➡️ Entrando en _precargar_historico()')
        """Carga datos recientes para todos los símbolos antes de iniciar."""
        if not self.modo_real or not self.cliente:
            log.info(
                '📈 Modo simulado: se omite precarga de histórico desde Binance'
                )
            return
        for symbol in self.estado.keys():
            try:
                datos = await fetch_ohlcv_async(self.cliente, symbol, self.
                    config.intervalo_velas, limit=velas)
            except BaseError as e:
                log.warning(f'⚠️ Error cargando histórico para {symbol}: {e}')
                continue
            except Exception as e:
                log.warning(
                    f'⚠️ Error inesperado cargando histórico para {symbol}: {e}'
                    )
                continue
            for ts, open_, high_, low_, close_, vol in datos:
                self.estado[symbol].buffer.append({'symbol': symbol,
                    'timestamp': ts, 'open': float(open_), 'high': float(
                    high_), 'low': float(low_), 'close': float(close_),
                    'volume': float(vol)})
            if datos:
                self.estado[symbol].ultimo_timestamp = datos[-1][0]
        log.info('📈 Histórico inicial cargado')

    async def _ciclo_aprendizaje(self, intervalo: int = 86400, max_fallos: int = 5) -> None:
        log.info('➡️ Entrando en _ciclo_aprendizaje()')
        """Ejecuta el proceso de aprendizaje continuo periódicamente.
        max_fallos: detiene el ciclo tras N errores consecutivos
        """
        await asyncio.sleep(1)
        fallos_consecutivos = 0
        # intervalo en el que se reporta actividad al supervisor
        intervalo_tick = max(1, self.heartbeat_interval // 2)
        while True:
            try:
                loop = asyncio.get_running_loop()
                futuro = loop.run_in_executor(None, ciclo_aprendizaje)
                # mientras el entrenamiento está en curso, marcamos actividad periódica
                while not futuro.done():
                    tick('aprendizaje')
                    await asyncio.sleep(intervalo_tick)
                await futuro
                log.info('🧠 Ciclo de aprendizaje completado')
                fallos_consecutivos = 0
            except Exception as e:
                fallos_consecutivos += 1
                log.warning(
                    f'⚠️ Error en ciclo de aprendizaje (fallos consecutivos: {fallos_consecutivos}): {e}'
                )
                if fallos_consecutivos >= max_fallos:
                    log.error(
                        '🛑 Deteniendo _ciclo_aprendizaje tras demasiados fallos seguidos.'
                    )
                    break
            # esperamos hasta la siguiente iteración, emitiendo ticks para evitar reinicios
            restante = intervalo
            while restante > 0:
                tick('aprendizaje')
                espera = min(restante, intervalo_tick)
                await asyncio.sleep(espera)
                restante -= espera

    async def _calcular_cantidad_async(self, symbol: str, precio: float
        ) ->float:
        log.info('➡️ Entrando en _calcular_cantidad_async()')
        """Solicita la cantidad al servicio de capital mediante eventos."""
        exposicion = sum(o.cantidad_abierta * o.precio_entrada for o in
            self.orders.ordenes.values())
        fut = asyncio.get_running_loop().create_future()
        await self.bus.publish('calcular_cantidad', {
            'symbol': symbol,
            'precio': precio,
            'exposicion_total': exposicion,
            'future': fut,
        })
        try:
            return await asyncio.wait_for(
                fut, timeout=self.config.timeout_bus_eventos
            )
        except asyncio.TimeoutError:
            log.error(f'⏰ Timeout calculando cantidad para {symbol}')
            return 0.0

    def _calcular_cantidad(self, symbol: str, precio: float) ->float:
        log.info('➡️ Entrando en _calcular_cantidad()')
        """Versión síncrona de :meth:`_calcular_cantidad_async`."""
        return asyncio.run(self._calcular_cantidad_async(symbol, precio))

    def _metricas_recientes(self, dias: int=7) ->dict:
        log.info('➡️ Entrando en _metricas_recientes()')
        """Calcula ganancia acumulada y drawdown de los últimos ``dias``."""
        carpeta = reporter_diario.carpeta
        if not os.path.isdir(carpeta):
            return {'ganancia_semana': 0.0, 'drawdown': 0.0, 'winrate': 0.0,
                'capital_actual': sum(self.capital_por_simbolo.values()),
                'capital_inicial': sum(self.capital_inicial_diario.values())}
        fecha_limite = datetime.utcnow().date() - timedelta(days=dias)
        retornos: list[float] = []
        archivos = sorted([f for f in os.listdir(carpeta) if f.endswith(
            '.csv')], reverse=True)[:20]
        for archivo in archivos:
            try:
                fecha = datetime.fromisoformat(archivo.replace('.csv', '')
                    ).date()
            except ValueError:
                continue
            if fecha < fecha_limite:
                continue
            ruta_archivo = os.path.join(carpeta, archivo)
            df = leer_reporte_seguro(ruta_archivo, columnas_esperadas=20)
            if df.empty:
                continue
            if 'retorno_total' in df.columns:
                retornos.extend(df['retorno_total'].dropna().tolist())
        if not retornos:
            return {'ganancia_semana': 0.0, 'drawdown': 0.0, 'winrate': 0.0,
                'capital_actual': sum(self.capital_por_simbolo.values()),
                'capital_inicial': sum(self.capital_inicial_diario.values())}
        serie = pd.Series(retornos).cumsum()
        drawdown = float((serie - serie.cummax()).min())
        ganancia = float(serie.iloc[-1])
        return {'ganancia_semana': ganancia, 'drawdown': drawdown}

    def _contar_senales(self, symbol: str, minutos: int=60) ->int:
        log.info('➡️ Entrando en _contar_senales()')
        """Cuenta señales válidas recientes para ``symbol``."""
        estado = self.estado.get(symbol)
        if not estado:
            return 0
        limite = datetime.utcnow().timestamp() * 1000 - minutos * 60 * 1000
        return sum(1 for v in estado.buffer if pd.to_datetime(v.get(
            'timestamp')).timestamp() * 1000 >= limite and v.get(
            'estrategias_activas'))

    def _obtener_historico(self, symbol: str,
        max_rows: int | None=None) ->(pd.DataFrame | None):
        log.info('➡️ Entrando en _obtener_historico()')
        """Devuelve el DataFrame de histórico para ``symbol`` usando caché.

        ``max_rows`` limita el número de filas conservadas en memoria.
        """
        df = self.historicos.get(symbol)
        if df is None:
            archivo = f"datos/{symbol.replace('/', '_').lower()}_1m.parquet"
            try:
                df = pd.read_parquet(archivo)
                limite = max_rows or self.max_filas_historico
                if limite:
                    df = df.tail(limite).copy()
                self.historicos[symbol] = df
                if len(self.historicos) > self.max_historicos_cache:
                    self.historicos.popitem(last=False)
            except Exception as e:
                log.debug(f'No se pudo cargar histórico para {symbol}: {e}')
                self.historicos[symbol] = None
                return None
        return df

    def _calcular_correlaciones(self, periodos: int=1440) ->pd.DataFrame:
        log.info('➡️ Entrando en _calcular_correlaciones()')
        """Calcula correlación histórica de cierres entre símbolos."""
        precios = {}
        for symbol in self.capital_por_simbolo:
            df = self._obtener_historico(symbol, max_rows=periodos)
            if df is not None and 'close' in df:
                precios[symbol] = df['close'].astype(float).tail(periodos
                    ).reset_index(drop=True)
        if len(precios) < 2:
            return pd.DataFrame()
        df_precios = pd.DataFrame(precios)
        return df_precios.corr()

    def _iniciar_tarea(self, nombre: str, factory: Callable[[], Awaitable]
        ) ->None:
        log.info('➡️ Entrando en _iniciar_tarea()')
        """Crea una tarea a partir de ``factory`` y la registra."""
        self._factories[nombre] = factory
        self._tareas[nombre] = supervised_task(factory, nombre)
        log.info(f'🚀 Tarea {nombre} iniciada')

    async def _vigilancia_tareas(self, intervalo: int=60) ->None:
        log.info('➡️ Entrando en _vigilancia_tareas()')
        while not self._cerrado:
            activos = 0
            ahora = datetime.utcnow()
            for nombre, task in list(self._tareas.items()):
                if task.done():
                    if task.cancelled():
                        log.info(
                            f'🟡 Heartbeat: tarea {nombre} fue cancelada manualmente'
                        )
                    else:
                        exc = task.exception()
                        if exc:
                            log.warning(
                                f'⚠️ Heartbeat: tarea {nombre} terminó con error: {exc}'
                            )
                        else:
                            log.warning(
                                f'⚠️ Heartbeat: tarea {nombre} finalizó inesperadamente'
                            )
                    self._iniciar_tarea(nombre, self._factories[nombre])
                    log.info(f'🔄 Tarea {nombre} reiniciada tras finalizar')
                    if nombre == 'data_feed':
                        log.debug('Data feed terminado y reiniciado por vigilancia')
                else:
                    hb = task_heartbeat.get(nombre, last_alive)
                    if (ahora - hb).total_seconds() > intervalo:
                        log.warning(
                            f'⏰ Tarea {nombre} sin actividad. Reiniciando.'
                        )
                        task.cancel()
                        self._iniciar_tarea(nombre, self._factories[nombre])
                        log.info(f'🔄 Tarea {nombre} reiniciada por inactividad')
                    else:
                        activos += 1
            for sym, ts in data_heartbeat.items():
                sin_datos = (ahora - ts).total_seconds()
                registro_metrico.registrar('sin_datos', {'symbol': sym, 'tiempo_s': sin_datos})
            log.info(
                f'🟢 Heartbeat: tareas activas {activos}/{len(self._tareas)}')
            tick('heartbeat')
            await asyncio.sleep(intervalo)

    def _flush_rechazos(self) ->None:
        log.info('➡️ Entrando en _flush_rechazos()')
        if not self._rechazos_buffer:
            return
        fecha = datetime.utcnow().strftime('%Y%m%d')
        archivo = os.path.join(LOG_DIR, 'rechazos', f'{fecha}.csv')
        df = pd.DataFrame(self._rechazos_buffer)
        modo = 'a' if os.path.exists(archivo) else 'w'
        df.to_csv(
            archivo,
            mode=modo,
            header=not os.path.exists(archivo),
            index=False,
        )
        self._rechazos_buffer.clear()

    async def _flush_rechazos_periodicamente(self, intervalo: int) ->None:
        log.info('➡️ Entrando en _flush_rechazos_periodicamente()')
        while not self._stop_event.is_set():
            await asyncio.sleep(intervalo)
            self._flush_rechazos()

    def _rechazo(self, symbol: str, motivo: str, puntaje: (float | None)=
        None, peso_total: (float | None)=None, estrategias: (list[str] |
        dict | None)=None) ->None:
        log.info('➡️ Entrando en _rechazo()')
        """Centraliza los mensajes de descartes de entrada."""
        mensaje = f'🔴 RECHAZO: {symbol} | Causa: {motivo}'
        if puntaje is not None:
            mensaje += f' | Puntaje: {puntaje:.2f}'
        if peso_total is not None:
            mensaje += f' | Peso: {peso_total:.2f}'
        if estrategias:
            estr = estrategias
            if isinstance(estr, dict):
                estr = list(estr.keys())
            mensaje += f' | Estrategias: {estr}'
        log.info(mensaje)
        registro = {
            'symbol': symbol,
            'motivo': motivo,
            'puntaje': puntaje,
            'peso_total': peso_total,
            'estrategias': ','.join(estrategias.keys() if isinstance(estrategias, dict) else estrategias) if estrategias else ''
        }
        self._rechazos_buffer.append(registro)
        if len(self._rechazos_buffer) >= self._rechazos_batch_size:
            self._flush_rechazos()
        registro_metrico.registrar('rechazo', registro)
        try:
            registrar_auditoria(symbol=symbol, evento='Entrada rechazada',
                resultado='rechazo', estrategias_activas=estrategias, score
                =puntaje, razon=motivo, capital_actual=self.
                capital_por_simbolo.get(symbol, 0.0), config_usada=self.
                config_por_simbolo.get(symbol, {}))
        except Exception as e:
            log.debug(f'No se pudo registrar auditoría de rechazo: {e}')

    def _validar_puntaje(self, symbol: str, puntaje: float, umbral: float,
        modo_agresivo: bool=False) -> bool:
        log.info('➡️ Entrando en _validar_puntaje()')
        """Comprueba si ``puntaje`` supera ``umbral``.

        Si ``modo_agresivo`` es ``True`` permite continuar incluso cuando el
        puntaje no alcanza el umbral establecido.
        """
        diferencia = umbral - puntaje
        metricas_tracker.registrar_diferencia_umbral(diferencia)
        if puntaje < umbral:
            if not modo_agresivo:
                log.debug(
                    f'🚫 {symbol}: puntaje {puntaje:.2f} < umbral {umbral:.2f}')
                metricas_tracker.registrar_filtro('umbral')
                return False
            log.debug(
                f'⚠️ {symbol}: Puntaje bajo {puntaje:.2f} < umbral {umbral:.2f} '
                'en modo agresivo')
        return True

    async def _validar_diversidad(self, symbol: str, peso_total: float,
        peso_min_total: float, estrategias_activas: Dict[str, float],
        diversidad_min: int, estrategias_disponibles: dict, df: pd.DataFrame,
        modo_agresivo: bool=False) -> bool:
        log.info('➡️ Entrando en _validar_diversidad()')
        """Verifica que la diversidad y el peso total sean suficientes.

        Cuando ``modo_agresivo`` es ``True`` tolera valores por debajo del
        mínimo requerido sin marcar la operación como rechazada.
        """
        diversidad = len(estrategias_activas)
        vol_factor = 1.0
        volatilidad = 0.0
        if df is not None and len(df) >= 30:
            vol_act = float(df['volume'].iloc[-1])
            vol_med = float(df['volume'].tail(30).mean())
            if vol_med > 0:
                vol_factor = vol_act / vol_med
            ventana = df['close'].tail(30)
            media = float(ventana.mean())
            if media:
                volatilidad = float(ventana.std()) / media
        peso_min_total *= max(0.5, min(2.0, vol_factor * (1 + volatilidad)))
        diversidad_min = max(1, int(round(diversidad_min * max(0.5, min(1.5,
            vol_factor)))))
        if self.modo_capital_bajo:
            euros = 0
            if self.modo_real and self.cliente:
                try:
                    balance = await fetch_balance_async(self.cliente)
                    euros = balance['total'].get('EUR', 0)
                except BaseError:
                    euros = 0
            if euros < 500:
                diversidad_min = min(diversidad_min, 2)
                peso_min_total *= 0.7
        if diversidad < diversidad_min or peso_total < peso_min_total:
            if not modo_agresivo:
                self._rechazo(symbol,
                    f'Diversidad {diversidad} < {diversidad_min} o peso {peso_total:.2f} < {peso_min_total:.2f}'
                    , peso_total=peso_total)
                metricas_tracker.registrar_filtro('diversidad')
                return False
            log.debug(
                f'⚠️ {symbol}: Diversidad {diversidad}/{diversidad_min} o peso {peso_total:.2f}/{peso_min_total:.2f} en modo agresivo'
                )
        return True

    def _validar_estrategia(self, symbol: str, df: pd.DataFrame,
        estrategias: Dict) ->bool:
        log.info('➡️ Entrando en _validar_estrategia()')
        """Aplica el filtro estratégico de entradas."""
        if not evaluar_validez_estrategica(symbol, df, estrategias):
            log.debug(
                f'❌ Entrada rechazada por filtro estratégico en {symbol}.')
            return False
        return True

    def _evaluar_persistencia(self, symbol: str, estado: EstadoSimbolo, df:
        pd.DataFrame, pesos_symbol: Dict[str, float], tendencia_actual: str,
        puntaje: float, umbral: float, estrategias: Dict[str, bool]) ->tuple[
        bool, float, float]:
        log.info('➡️ Entrando en _evaluar_persistencia()')
        """Evalúa si las señales persistentes son suficientes para entrar."""
        ventana_close = df['close'].tail(10)
        media_close = np.mean(ventana_close)
        if np.isnan(media_close) or media_close == 0:
            log.debug(
                f'⚠️ {symbol}: Media de cierre inválida para persistencia')
            return False
        repetidas = coincidencia_parcial(estado.estrategias_buffer,
            pesos_symbol, ventanas=5)
        minimo = calcular_persistencia_minima(symbol, df, tendencia_actual,
            base_minimo=self.persistencia.minimo)
        log.info(
            f'Persistencia detectada {repetidas:.2f} | Mínimo requerido {minimo:.2f}'
            )
        if repetidas < minimo:
            self._rechazo(symbol,
                f'Persistencia {repetidas:.2f} < {minimo}', puntaje=puntaje,
                estrategias=list(estrategias.keys()))
            metricas_tracker.registrar_filtro('persistencia')
            return False, repetidas, minimo
        if repetidas < 1 and puntaje < 1.2 * umbral:
            self._rechazo(symbol,
                f'{repetidas:.2f} coincidencia y puntaje débil ({puntaje:.2f})'
                , puntaje=puntaje, estrategias=list(estrategias.keys()))
            return False, repetidas, minimo
        elif repetidas < 1:
            log.info(
                f'⚠️ Entrada débil en {symbol}: Coincidencia {repetidas:.2f} insuficiente pero puntaje alto ({puntaje}) > Umbral {umbral} — Permitida.'
                )
            metricas_tracker.registrar_filtro('persistencia')
        return True, repetidas, minimo

    def _tendencia_persistente(self, symbol: str, df: pd.DataFrame,
        tendencia: str, velas: int=3) ->bool:
        log.info('➡️ Entrando en _tendencia_persistente()')
        if len(df) < 30 + velas:
            return False
        for i in range(velas):
            sub_df = df.iloc[:-(velas - 1 - i)] if velas - 1 - i > 0 else df
            t, _ = detectar_tendencia(symbol, sub_df)
            if t != tendencia:
                return False
        return True

    def _validar_reentrada_tendencia(self, symbol: str, df: pd.DataFrame,
        cierre: dict, precio: float) ->bool:
        log.info('➡️ Entrando en _validar_reentrada_tendencia()')
        if cierre.get('motivo') != 'cambio de tendencia':
            return True
        tendencia = cierre.get('tendencia')
        if not tendencia:
            return False
        cierre_dt = pd.to_datetime(cierre.get('timestamp'), errors='coerce')
        if pd.isna(cierre_dt):
            log.warning(f'⚠️ {symbol}: Timestamp de cierre inválido')
            return False
        duracion = cierre.get('duracion', 0)
        retorno = abs(cierre.get('retorno_total', 0))
        velas_requeridas = 3 + min(int(duracion // 30), 3)
        if retorno > 0.05:
            velas_requeridas += 1
        df_post = df[pd.to_datetime(df['timestamp']) > cierre_dt]
        if len(df_post) < velas_requeridas:
            log.info(
                f'⏳ {symbol}: esperando confirmación de tendencia {len(df_post)}/{velas_requeridas}'
                )
            return False
        if not self._tendencia_persistente(symbol, df_post, tendencia,
            velas=velas_requeridas):
            log.info(
                f'⏳ {symbol}: tendencia {tendencia} no persistente tras cierre'
                )
            return False
        precio_salida = cierre.get('precio')
        if precio_salida is not None and abs(precio - precio_salida
            ) <= precio * 0.001:
            log.info(
                f'🚫 {symbol}: precio de entrada similar al de salida anterior')
            return False
        return True
    
    def _validar_sl_tp(self, symbol: str, precio: float, sl: float,
        tp: float) ->bool:
        log.info('➡️ Entrando en _validar_sl_tp()')
        df = self._obtener_historico(symbol)
        if df is None or len(df) < 30:
            return True
        atr = calcular_atr(df.tail(100))
        if not atr:
            return True
        min_sl = atr * getattr(self.config, 'factor_sl_atr', 0.5)
        min_tp = atr * getattr(self.config, 'factor_tp_atr', 0.5)
        if abs(precio - sl) < min_sl or abs(tp - precio) < min_tp:
            log.warning(
                f'⛔ {symbol}: SL/TP demasiado cercanos al precio para ATR '
                f'{atr:.4f}')
            registro_metrico.registrar('entrada_rechazada_sl_tp',
                {'symbol': symbol})
            return False
        return True

    def _calcular_score_tecnico(self, df: pd.DataFrame, rsi: (float | None),
        momentum: (float | None), tendencia: str, direccion: str) ->tuple[
        float, dict]:
        log.info('➡️ Entrando en _calcular_score_tecnico()')
        """Calcula un puntaje técnico simple a partir de varios indicadores."""
        slope = calcular_slope(df)
        score_indicadores = calcular_score_tecnico(df, rsi, momentum, slope,
            tendencia)
        resultados = {'RSI': False, 'Momentum': False, 'Slope': False,
            'Tendencia': False}
        if rsi is not None:
            if direccion == 'long':
                resultados['RSI'] = rsi > 50 or 45 <= rsi <= 55
            else:
                resultados['RSI'] = rsi < 50 or 45 <= rsi <= 55
        if momentum is not None:
            resultados['Momentum'] = momentum > 0
        if slope is not None:
            if direccion == 'long':
                resultados['Slope'] = slope > 0
            else:
                resultados['Slope'] = slope < 0
        if direccion == 'long':
            resultados['Tendencia'] = tendencia in {'alcista', 'lateral'}
        else:
            resultados['Tendencia'] = tendencia in {'bajista', 'lateral'}
        score_total = score_indicadores
        if resultados['Tendencia']:
            score_total += PESOS_SCORE_TECNICO.get('Tendencia', 1.0)
        log.info(
            '📊 Score técnico: %.2f | RSI: %s (%.2f), Momentum: %s (%.4f), Slope: %s (%.4f), Tendencia: %s'
            , score_total, '✅' if resultados['RSI'] else '❌', rsi if rsi is not
            None else 0.0, '✅' if resultados['Momentum'] else '❌', momentum if
            momentum is not None else 0.0, '✅' if resultados['Slope'] else
            '❌', slope, '✅' if resultados['Tendencia'] else '❌')
        return float(score_total), resultados

    def _hay_contradicciones(self, df: pd.DataFrame, rsi: (float | None),
        momentum: (float | None), direccion: str, score: float) ->bool:
        log.info('➡️ Entrando en _hay_contradicciones()')
        """Detecta si existen contradicciones fuertes en las señales."""
        if direccion == 'long':
            if rsi is not None and rsi > 70:
                return True
            if df['close'].iloc[-1] >= df['close'].iloc[-10] * 1.05:
                return True
            if (momentum is not None and momentum < 0 and score >= self.
                umbral_score_tecnico):
                return True
        else:
            if rsi is not None and rsi < 30:
                return True
            if df['close'].iloc[-1] <= df['close'].iloc[-10] * 0.95:
                return True
            if (momentum is not None and momentum > 0 and score >= self.
                umbral_score_tecnico):
                return True
        return False

    def _validar_temporalidad(self, df: pd.DataFrame, direccion: str) ->bool:
        log.info('➡️ Entrando en _validar_temporalidad()')
        """Verifica que las señales no estén perdiendo fuerza."""
        rsi_series = calcular_rsi(df, serie_completa=True)
        if rsi_series is None or len(rsi_series) < 3:
            return True
        r = rsi_series.iloc[-3:]
        if direccion == 'long' and not r.iloc[-1] > r.iloc[-2] > r.iloc[-3]:
            return False
        if direccion == 'short' and not r.iloc[-1] < r.iloc[-2] < r.iloc[-3]:
            return False
        slope3 = calcular_slope(df, periodo=3)
        slope5 = calcular_slope(df, periodo=5)
        if direccion == 'long' and not slope3 > slope5:
            return False
        if direccion == 'short' and not slope3 < slope5:
            return False
        return True

    def _registrar_rechazo_tecnico(self, symbol: str, score: float, puntos:
        dict, tendencia: str, precio: float, motivo: str, estrategias: (
        dict | None)=None) ->None:
        log.info('➡️ Entrando en _registrar_rechazo_tecnico()')
        """Guarda detalles de rechazos técnicos en un CSV."""
        if not self.registro_tecnico_csv:
            return
        fila = {'timestamp': datetime.utcnow().isoformat(), 'symbol':
            symbol, 'puntaje_total': score, 'indicadores_fallidos': ','.
            join([k for k, v in puntos.items() if not v]), 'estado_mercado':
            tendencia, 'precio': precio, 'motivo': motivo, 'estrategias': 
            ','.join(estrategias.keys()) if estrategias else ''}
        df = pd.DataFrame([fila])
        modo = 'a' if os.path.exists(self.registro_tecnico_csv) else 'w'
        df.to_csv(self.registro_tecnico_csv, mode=modo, header=not os.path.
            exists(self.registro_tecnico_csv), index=False)

    async def evaluar_condiciones_entrada(self, symbol: str, df: pd.DataFrame
        ) ->None:
        log.info('➡️ Entrando en evaluar_condiciones_entrada()')
        """
        Evalúa y ejecuta una entrada si todas las condiciones se cumplen,
        con protección de timeout sobre el motor sincrónico.
        """
        estado = self.estado[symbol]
        config_actual = self.config_por_simbolo.get(symbol, {})
        config_actual = adaptar_configuracion(symbol, df, config_actual)
        self.config_por_simbolo[symbol] = config_actual
        tendencia_actual = self.estado_tendencia.get(symbol)
        if not tendencia_actual:
            tendencia_actual, _ = detectar_tendencia(symbol, df)
            self.estado_tendencia[symbol] = tendencia_actual
        loop = asyncio.get_running_loop()
        try:
            resultado = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self.engine.evaluar_entrada(
                        symbol,
                        df,
                        tendencia=tendencia_actual,
                        config=config_actual,
                        pesos_symbol=self.pesos_por_simbolo.get(symbol, {}),
                    ),
                ),
                timeout=self.config.timeout_evaluar_condiciones,
            )
        except asyncio.TimeoutError:
            log.warning(f'⚠️ Timeout en evaluar_entrada para {symbol}')
            self._rechazo(symbol, 'timeout_engine', estrategias=[])
            return
        estrategias = resultado.get('estrategias_activas', {})
        estado.estrategias_buffer[-1] = estrategias
        self.persistencia.actualizar(symbol, estrategias)
        precio_actual = float(df['close'].iloc[-1])
        if not resultado.get('permitido'):
            if self.usar_score_tecnico:
                rsi = resultado.get('rsi')
                mom = resultado.get('momentum')
                score, puntos = self._calcular_score_tecnico(df, rsi, mom,
                    tendencia_actual, 'short' if tendencia_actual ==
                    'bajista' else 'long')
                self._registrar_rechazo_tecnico(symbol, score, puntos,
                    tendencia_actual, precio_actual, resultado.get(
                    'motivo_rechazo', 'desconocido'), estrategias)
            self._rechazo(symbol, resultado.get('motivo_rechazo',
                'desconocido'), puntaje=resultado.get('score_total'),
                estrategias=list(estrategias.keys()))
            return
        info = await self.evaluar_condiciones_de_entrada(symbol, df, estado)
        if not info:
            self._rechazo(symbol, 'filtros_post_engine', puntaje=resultado.
                get('score_total'), estrategias=list(estrategias.keys()))
            return
        await self._abrir_operacion_real(**info)

    async def _abrir_operacion_real(self, symbol: str, precio: float, sl:
        float, tp: float, estrategias: (Dict | List), tendencia: str,
        direccion: str, puntaje: float=0.0, umbral: float=0.0,
        detalles_tecnicos: (dict | None)=None, **kwargs) ->None:
        log.info('➡️ Entrando en _abrir_operacion_real()')
        cantidad_total = await self._calcular_cantidad_async(symbol, precio)
        if cantidad_total <= 0:
            capital_disp = self.capital_manager.capital_por_simbolo.get(
                symbol, 0.0)
            log.warning(
                f'⛔ No se abre posición en {symbol} por capital insuficiente. '
                f'Disponible: {capital_disp:.2f}€')
            return
        fracciones = self.piramide_fracciones
        cantidad = cantidad_total / fracciones
        if self.capital_manager.cliente:
            try:
                ticker = await fetch_ticker_async(self.capital_manager.cliente, symbol)
                precio_actual = float(ticker.get('last') or ticker.get('close') or precio)
                spread = abs(precio_actual - precio) / precio if precio else 0.0
                if spread > self.config.max_spread_ratio:
                    log.warning(
                        f'⛔ Spread {spread:.2%} supera límite en {symbol}. Orden cancelada.'
                    )
                    return
            except Exception as e:
                log.error(f'❌ No se pudo verificar spread para {symbol}: {e}')
        if isinstance(estrategias, dict):
            estrategias_dict = estrategias
        else:
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            estrategias_dict = {e: pesos_symbol.get(e, 0.0) for e in
                estrategias}
        try:
            if not self._validar_sl_tp(symbol, precio, sl, tp):
                self._rechazo(symbol, 'sl_tp_invalidos', puntaje=puntaje,
                    estrategias=list(estrategias_dict.keys()))
                return
        except Exception as e:
            log.error(f'❌ Error validando SL/TP para {symbol}: {e}')
        await self.orders.abrir_async(
            symbol=symbol,
            precio=precio,
            sl=sl,
            tp=tp,
            estrategias=estrategias_dict,
            tendencia=tendencia,
            direccion=direccion,
            cantidad=cantidad,
            puntaje=puntaje,
            umbral=umbral,
            objetivo=cantidad_total,
            fracciones=fracciones,
            detalles_tecnicos=detalles_tecnicos or {},
        )
        estrategias_list = list(estrategias_dict.keys())
        log.info(
            f'🟢 ENTRADA: {symbol} | Puntaje: {puntaje:.2f} / Umbral: {umbral:.2f} | Estrategias: {estrategias_list}'
            )
        registro_metrico.registrar('entrada', {'symbol': symbol, 'puntaje':
            puntaje, 'umbral': umbral, 'estrategias': ','.join(
            estrategias_list), 'precio': precio})
        try:
            registrar_auditoria(symbol=symbol, evento='Entrada', resultado=
                'ejecutada', estrategias_activas=estrategias_dict, score=
                puntaje, tendencia=tendencia, capital_actual=self.
                capital_por_simbolo.get(symbol, 0.0), config_usada=self.
                config_por_simbolo.get(symbol, {}))
        except Exception as e:
            log.debug(f'No se pudo registrar auditoría de entrada: {e}')

    async def _verificar_salidas(self, symbol: str, df: pd.DataFrame) ->None:
        log.info('➡️ Entrando en _verificar_salidas()')
        await verificar_salidas(self, symbol, df)

    async def evaluar_condiciones_de_entrada(self, symbol: str, df: pd.
        DataFrame, estado: EstadoSimbolo) ->(dict | None):
        log.info('➡️ Entrando en evaluar_condiciones_de_entrada()')
        if not self._validar_config(symbol):
            return None
        return await verificar_entrada(self, symbol, df, estado)

    async def ejecutar(self) ->None:
        log.info('➡️ Entrando en ejecutar()')
        """Inicia el procesamiento de todos los símbolos."""

        async def handle(candle: dict) ->None:
            log.info('➡️ Entrando en handle()')
            await self._procesar_vela(candle)

        async def handle_context(symbol: str, score: float) ->None:
            log.info('➡️ Entrando en handle_context()')
            log.debug(f'🔁 Contexto actualizado {symbol}: {score:.2f}')
        symbols = list(self.estado.keys())
        await self._precargar_historico(velas=60)
        tareas: dict[str, Callable[[], Awaitable]] = {
            'data_feed': lambda: self.data_feed.escuchar(
                symbols,
                handle,
                cliente=self.cliente if self.modo_real else None,
            ),
            'estado': lambda: monitorear_estado_periodicamente(self),
            'context_stream': lambda: self.context_stream.escuchar(
                symbols, handle_context
            ),
            'flush': lambda: real_orders.flush_periodico(),
            'rechazos_flush': lambda: self._flush_rechazos_periodicamente(self._rechazos_intervalo_flush),
        }
        if 'PYTEST_CURRENT_TEST' not in os.environ:
            tareas['aprendizaje'] = lambda : self._ciclo_aprendizaje()
        for nombre, factory in tareas.items():
            self._iniciar_tarea(nombre, factory)
        self._iniciar_tarea('heartbeat', lambda : self._vigilancia_tareas(self.heartbeat_interval))
        await self._stop_event.wait()

    async def _procesar_vela(self, vela: dict) ->None:
        log.info('➡️ Entrando en _procesar_vela()')
        symbol = vela.get('symbol')
        if not self._validar_config(symbol):
            return
        await procesar_vela(self, vela)
        return

    async def cerrar(self) ->None:
        log.info('➡️ Entrando en cerrar()')
        self._cerrado = True
        self._stop_event.set()
        for nombre, tarea in list(self._tareas.items()):
            if nombre == 'data_feed':
                await self.data_feed.detener()
            if nombre == 'context_stream':
                await self.context_stream.detener()
            tarea.cancel()
        await asyncio.gather(*self._tareas.values(), return_exceptions=True)
        await self.bus.close()
        self._flush_rechazos()
        self._guardar_estado_persistente()

    def _guardar_estado_persistente(self) ->None:
        log.info('➡️ Entrando en _guardar_estado_persistente()')
        """Guarda historial de cierres y capital en ``estado/``."""
        try:
            os.makedirs('estado', exist_ok=True)
            self._save_json_file('estado/historial_cierres.json', self.
                historial_cierres)
            self._save_json_file('estado/capital.json', self.
                capital_por_simbolo)
        except Exception as e:
            log.warning(f'⚠️ Error guardando estado persistente: {e}')

    def _cargar_estado_persistente(self) ->None:
        log.info('➡️ Entrando en _cargar_estado_persistente()')
        """Carga el estado previo de ``estado/`` si existe."""
        try:
            data = self._load_json_file('estado/historial_cierres.json')
            if data:
                self.historial_cierres.update(data)
            data = self._load_json_file('estado/capital.json')
            if data:
                self.capital_por_simbolo.update({k: float(v) for k, v in
                    data.items()})
        except Exception as e:
            log.warning(f'⚠️ Error cargando estado persistente: {e}')

    def _validar_config(self, symbol: str) ->bool:
        log.info('➡️ Entrando en _validar_config()')
        """Valida que exista configuración para ``symbol``."""
        cfg = self.config_por_simbolo.get(symbol)
        if not isinstance(cfg, dict):
            log.error(f'⚠️ Configuración no encontrada para {symbol}')
            return False
        return True
