"""Controlador principal del bot modular."""
from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass, replace, field
from typing import Dict, Callable, Awaitable, Any, List
from collections import OrderedDict, deque, defaultdict
from core.streams.candle_filter import CandleFilter
from datetime import datetime, timedelta, timezone
import json
import os
import math
from math import isclose
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
from core.rejection_handler import RejectionHandler
from binance_api.cliente import (
    crear_cliente,
    fetch_balance_async,
    fetch_ohlcv_async,
    filtrar_simbolos_activos,
)
from core.utils.utils import (
    leer_reporte_seguro,
    DATOS_DIR,
    ESTADO_DIR,
    intervalo_a_segundos,
    round_decimal,
)
from core.strategies import cargar_pesos_estrategias
from core.risk import calcular_fraccion_kelly
from core.data import PersistenciaTecnica, coincidencia_parcial, calcular_persistencia_minima
from core.metricas_semanales import metricas_tracker, metricas_semanales
from core.ajustador_riesgo import RIESGO_MAXIMO_DIARIO_BASE
from learning.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from core.utils.utils import configurar_logger
from core.monitor_estado_bot import monitorear_estado_periodicamente
from core.contexto_externo import StreamContexto
from core.data.external_feeds import ExternalFeeds
from core.orders import real_orders
from core.config_manager.dinamica import adaptar_configuracion
from ccxt.base.errors import BaseError
from core.reporting import reporter_diario
from core.registro_metrico import registro_metrico
from core.metrics import registrar_decision, registrar_correlacion_btc
from core.supervisor import (
    supervised_task,
    task_heartbeat,
    tick,
    get_last_alive,
    data_heartbeat,
)
from learning.aprendizaje_en_linea import registrar_resultado_trade
from learning.aprendizaje_continuo import ejecutar_ciclo as ciclo_aprendizaje
from core.strategies.exit.gestor_salidas import verificar_filtro_tecnico
from core.strategies.tendencia import detectar_tendencia
from core.strategies.entry.validador_entradas import evaluar_validez_estrategica
from indicators.helpers import get_rsi, get_momentum, get_atr, get_slope
from core.strategies.evaluador_tecnico import actualizar_pesos_tecnicos
from core.auditoria import registrar_auditoria
from core.strategies.exit.verificar_salidas import verificar_salidas
from core.strategies.entry.verificar_entradas import verificar_entrada
from core.strategies.entry.gestor_entradas import requiere_ajuste_diversificacion
from core.procesar_vela import (
    procesar_vela,
    MAX_BUFFER_VELAS,
    MAX_ESTRATEGIAS_BUFFER,
)
from indicators.rsi import calcular_rsi
from indicators.correlacion import correlacion_series
from core.scoring import calcular_score_tecnico, PESOS_SCORE_TECNICO, ScoreBreakdown
from binance_api.cliente import fetch_ticker_async
from core import adaptador_umbral
from core.data.bootstrap import warmup_symbol
log = configurar_logger('trader')
LOG_DIR = os.getenv('LOG_DIR', 'logs')
UTC = timezone.utc


def _ts_ms(v: object) -> int:
    """Convertir cualquier marca de tiempo a milisegundos UTC."""
    if isinstance(v, (int, float)):
        return int(v)
    return int(pd.to_datetime(v, utc=True).view('int64') // 1_000_000)


async def safe_notify(coro: Awaitable, timeout: int = 3):
    """Ejecuta el envío de notificaciones con timeout y captura de errores."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except Exception as e:
        log.error(f'❌ Error enviando notificación: {e}')
        return None


@dataclass
class EstadoSimbolo:
    buffer: deque = field(default_factory=lambda: deque(maxlen=MAX_BUFFER_VELAS))
    estrategias_buffer: deque = field(
        default_factory=lambda: deque(maxlen=MAX_ESTRATEGIAS_BUFFER)
    )
    ultimo_umbral: float = 0.0
    ultimo_timestamp: int | None = None
    candle_filter: CandleFilter = field(default_factory=CandleFilter)
    tendencia_detectada: str | None = None
    timeouts_salidas: int = 0
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_idx: int = 0
    contador_tendencia: int = 0
    indicadores_cache: dict = field(default_factory=dict)
    indicadores_calls: int = 0
    indicadores_wait_ms: float = 0.0
    entradas_timeouts: int = 0
    cierres_timeouts: int = 0


class Trader:
    """Orquesta el flujo de datos y las operaciones de trading."""

    def __init__(self, config: Config) ->None:
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
            getattr(config, 'inactivity_intervals', 3),
            handler_timeout=getattr(config, 'handler_timeout', 5),
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
        self.persistencia = PersistenciaTecnica(
            config.persistencia_minima, config.peso_extra_persistencia
        )
        self.rejection_handler = RejectionHandler(
            LOG_DIR, config.registro_tecnico_csv
        )
        self.umbral_score_tecnico = config.umbral_score_tecnico
        self.usar_score_tecnico = getattr(config, 'usar_score_tecnico', True)
        self.contradicciones_bloquean_entrada = (
            config.contradicciones_bloquean_entrada
        )
        self._rechazos_intervalo_flush = 5
        # Cache limitada para históricos de velas
        self.historicos: OrderedDict[str, pd.DataFrame] = OrderedDict()
        self.max_historicos_cache = getattr(config, 'max_historicos_cache', 20)
        self.max_filas_historico = getattr(config, 'max_filas_historico', 1440)
        self._correlaciones_cache: pd.DataFrame | None = None
        self._correlaciones_ts: float = 0.0
        self.frecuencia_correlaciones = getattr(config, 'frecuencia_correlaciones', 300)
        self.frecuencia_recursos = getattr(config, 'frecuencia_recursos', 60)
        self._recursos_ts: float = 0.0
        self._ultimo_cpu: float = 0.0
        self._ultimo_mem: float = 0.0
        self.fraccion_kelly = calcular_fraccion_kelly()
        factor_kelly = self.risk.multiplicador_kelly()
        self.fraccion_kelly *= factor_kelly
        self.series_precio: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=MAX_BUFFER_VELAS)
        )
        # Históricos adicionales para BTC y otros índices macro si se dispone.
        self.historicos_macro: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.max_filas_historico)
        )
        self.umbral_correlacion_btc = getattr(config, "umbral_correlacion_btc", 0.9)
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
        self.riesgo_maximo_diario = RIESGO_MAXIMO_DIARIO_BASE
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
        self.config_por_simbolo: Dict[str, dict] = {
            s: {'diversidad_minima': getattr(config, 'diversidad_minima', 2)}
            for s in config.symbols
        }
        try:
            self.pesos_por_simbolo: Dict[str, Dict[str, float]
                ] = cargar_pesos_estrategias()
        except ValueError as e:
            log.error(f'❌ {e}')
            raise
        self.historial_cierres: Dict[str, dict] = {}
        self.capital_inicial_orden: Dict[str, float] = {}
        self._tareas: dict[str, asyncio.Task] = {}
        self._restart_stats: defaultdict[str, deque] = defaultdict(deque)
        self._sin_datos_alertado: set[str] = set()
        self._factories: dict[str, Callable[[], Awaitable]] = {}
        self._stop_event = asyncio.Event()
        self._limite_riesgo_notificado = False
        self._cerrado = False
        self._cpu_high_cycles = 0
        self._mem_high_cycles = 0
        self.context_stream = StreamContexto()
        self.external_feeds = ExternalFeeds(self.context_stream)
        self.puntajes_contexto: Dict[str, float] = {}
        self.estrategias_habilitadas = False
        self.warmup_completed = False
        try:
            simbolos = config.symbols if self.modo_real else None
            self.orders.ordenes = real_orders.reconciliar_ordenes(simbolos)
        except Exception as e:
            log.warning(
                f'⚠️ Error cargando órdenes previas desde la base de datos: {e}'
                )
            raise
        if self.orders.ordenes:
            for s in self.orders.ordenes:
                self.capital_inicial_orden[s] = self.capital_por_simbolo.get(s, 0.0)
            log.warning(
                '⚠️ Órdenes abiertas encontradas al iniciar. Serán monitoreadas.'
                )
        if 'PYTEST_CURRENT_TEST' not in os.environ:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(self._cargar_estado_persistente())
            else:
                loop.create_task(self._cargar_estado_persistente())
        else:
            log.debug('🔍 Modo prueba: se omite carga de estado persistente')

    def _load_json_file(self, path: str) ->dict[str, Any]:
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
        """Guardar ``data`` en ``path`` silenciosamente."""
        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        except OSError as e:
            log.warning(f'⚠️ Error guardando {path}: {e}')

    def habilitar_estrategias(self) -> None:
        """Permite que las estrategias comiencen a evaluarse."""
        self.estrategias_habilitadas = True
        log.info('🟢 Estrategias habilitadas')

    def _registrar_rechazo(self, symbol: str, motivo: str, **datos: Any) -> None:
        """Registrar rechazos con campos comunes.

        Envuelve :meth:`RejectionHandler.registrar` asegurando que todos los
        registros incluyan la información de ``capital`` y ``config`` del
        símbolo.  Parámetros adicionales se pasan directamente al handler.
        """

        datos_comunes = {
            'capital': self.capital_por_simbolo.get(symbol, 0.0),
            'config': self.config_por_simbolo.get(symbol, {}),
        }
        datos_comunes.update(datos)
        self.rejection_handler.registrar(symbol, motivo, **datos_comunes)

    def _log_impacto_sl(self, orden, precio: float) ->None:
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
        """Cierra una orden y actualiza los pesos si corresponden."""
        orden = self.orders.obtener(symbol)
        if not orden or getattr(orden, 'cerrando', False):
            log.warning(f'[{symbol}] Cierre ya en progreso o sin orden activa')
            return
        orden.cerrando = True
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
            log.warning(
                f'🔁 Cierre no confirmado para {symbol}; pesos no actualizados'
            )
            return
        actualizar_pesos_estrategias_symbol(symbol)
        try:
            self.pesos_por_simbolo = cargar_pesos_estrategias()
        except ValueError as e:
            log.error(f'❌ {e}')
            return
        log.info(f"✅ Orden cerrada: {symbol} a {precio:.2f}€ por '{motivo}'")
        registrar_decision(symbol, 'exit')

    async def _cerrar_y_reportar(self, orden, precio: float, motivo: str,
        tendencia: (str | None)=None, df: (pd.DataFrame | None)=None) ->None:
        """Cierra ``orden`` y registra la operación para el reporte diario."""
        if getattr(orden, 'cerrando', False):
            log.warning(f'[{orden.symbol}] Cierre en progreso, se ignora reintento')
            return False
        orden.cerrando = True
        retorno_unitario = (precio - orden.precio_entrada
            ) / orden.precio_entrada if orden.precio_entrada else 0.0
        fraccion = orden.cantidad_abierta / orden.cantidad if orden.cantidad else 0.0
        retorno_parcial = retorno_unitario * fraccion
        retorno_total = (orden.retorno_total or 0.0) + retorno_parcial
        capital_base = self.capital_inicial_orden.get(orden.symbol,
            self.capital_por_simbolo.get(orden.symbol, 0.0))
        info = orden.to_dict()
        info.update({
            'precio_cierre': precio,
            'fecha_cierre': datetime.now(UTC).isoformat(),
            'motivo_cierre': motivo,
            'retorno_total': retorno_total,
            'capital_inicial': capital_base,
        })
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
            log.error(
                f'❌ No se pudo confirmar el cierre de {orden.symbol}. Se omitirá el registro.'
            )
            return False
        capital_actual = self.capital_por_simbolo.get(orden.symbol, 0.0)
        ganancia_parcial = capital_base * retorno_parcial
        self.capital_por_simbolo[orden.symbol] = capital_actual + ganancia_parcial
        capital_final = self.capital_por_simbolo[orden.symbol]
        ganancia_total = capital_base * retorno_total
        info['capital_final'] = capital_final
        if getattr(orden, 'sl_evitar_info', None):
            self._log_impacto_sl(orden, precio)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, reporter_diario.registrar_operacion, info)
        await loop.run_in_executor(None, registrar_resultado_trade, orden.symbol, info, retorno_total)
        try:
            if orden.detalles_tecnicos:
                await actualizar_pesos_tecnicos(orden.symbol, orden.
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
            duracion = (datetime.now(UTC) - apertura).total_seconds() / 60
        except Exception:
            pass
        prev = self.historial_cierres.get(orden.symbol, {})
        self.historial_cierres[orden.symbol] = {
                'timestamp': datetime.now(UTC).isoformat(),
                'motivo': motivo.lower().strip(),
                'velas': 0,
                'precio': precio,
                'tendencia': tendencia,
                'duracion': duracion,
                'retorno_total': retorno_total,
            }
        if retorno_total < 0:
            fecha_hoy = datetime.now(UTC).date().isoformat()
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
            f'✅ CIERRE {motivo.upper()}: {orden.symbol} | Beneficio: {ganancia_total:.2f} €'
            )
        registro_metrico.registrar('cierre', {'symbol': orden.symbol,
            'motivo': motivo, 'retorno': retorno_total, 'beneficio': ganancia_total})
        rsi_val = get_rsi(df) if df is not None else None
        mom_val = get_momentum(df) if df is not None else None
        score_val, _ = self._calcular_score_tecnico(
            df, rsi_val, mom_val, tendencia or '', orden.direccion
        ) if df is not None else (0.0, {})
        await self._registrar_salida_profesional(
            orden.symbol,
            {
                'tipo_salida': motivo,
                'estrategias_activas': orden.estrategias_activas,
                'score_tecnico_al_cierre': score_val,
                'capital_final': capital_final,
                'configuracion_usada': self.config_por_simbolo.get(orden.symbol, {}),
                'tiempo_operacion': duracion,
                'beneficio_relativo': retorno_total,
            },
        )
        metricas = self._metricas_recientes()
        await self.bus.publish('ajustar_riesgo', metricas)
        try:
            registrar_auditoria(
                symbol=orden.symbol,
                evento=motivo,
                resultado='ganancia' if retorno_total > 0 else 'pérdida',
                estrategias_activas=orden.estrategias_activas,
                score=score_val,
                rsi=rsi_val,
                tendencia=tendencia,
                capital_actual=capital_final,
                config_usada=self.config_por_simbolo.get(orden.symbol, {}),
            )
        except Exception as e:
            log.debug(f'No se pudo registrar auditoría de cierre: {e}')
        self.capital_inicial_orden.pop(orden.symbol, None)
        return True

    async def _registrar_salida_profesional(self, symbol: str, info: dict) -> None:
        archivo = 'reportes_diarios/registro_salidas.parquet'
        os.makedirs(os.path.dirname(archivo), exist_ok=True)
        data = info.copy()
        data['symbol'] = symbol
        data['timestamp'] = datetime.now(UTC).isoformat()
        if isinstance(data.get('estrategias_activas'), dict):
            data['estrategias_activas'] = json.dumps(data['estrategias_activas'])
        try:
            if os.path.exists(archivo):
                df = await asyncio.to_thread(pd.read_parquet, archivo)
                df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
            else:
                df = pd.DataFrame([data])
            await asyncio.to_thread(df.to_parquet, archivo, index=False)
        except Exception as e:
            log.warning(f'⚠️ Error registrando salida en {archivo}: {e}')

    async def _cerrar_posiciones_por_riesgo(self) ->None:
        """Cierra todas las posiciones abiertas cuando se supera el riesgo."""
        ordenes = list(self.orders.ordenes.items())
        for symbol, orden in ordenes:
            try:
                df = self._obtener_historico(symbol)
                precio = orden.precio_entrada
                if self.modo_real:
                    try:
                        ticker = await fetch_ticker_async(self.cliente, symbol)
                        precio = float(ticker.get('last', precio))
                    except Exception as e:
                        log.error(f'❌ Error obteniendo precio actual para {symbol}: {e}')
                elif df is not None and 'close' in df:
                    precio = float(df['close'].iloc[-1])
                await self._cerrar_y_reportar(orden, precio, 'Límite riesgo diario', df=df)
            except Exception as e:
                log.error(f'❌ Error cerrando {symbol} por riesgo: {e}')

    async def _cerrar_parcial_y_reportar(
        self,
        orden,
        cantidad: float,
        precio: float,
        motivo: str,
        df: (pd.DataFrame | None) = None,
    ) -> bool:
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
        orden.retorno_total = (orden.retorno_total or 0.0) + retorno_total
        capital_base = self.capital_inicial_orden.get(orden.symbol,
            self.capital_por_simbolo.get(orden.symbol, 0.0))
        info = orden.to_dict()
        info.update(
            {
                'precio_cierre': precio,
                'fecha_cierre': datetime.now(UTC).isoformat(),
                'motivo_cierre': motivo,
                'retorno_total': retorno_total,
                'cantidad_cerrada': cantidad,
                'capital_inicial': capital_base,
            }
        )
        loop = asyncio.get_running_loop()
        tareas = [
            loop.run_in_executor(None, reporter_diario.registrar_operacion, info),
            loop.run_in_executor(
                None, registrar_resultado_trade, orden.symbol, info, retorno_total
            ),
        ]
        resultados = await asyncio.gather(*tareas, return_exceptions=True)
        for res, nombre in zip(resultados, ['reporte', 'historico']):
            if isinstance(res, Exception):
                log.warning(f'⚠️ Error registrando {nombre}: {res}')
        ganancia = capital_base * retorno_total
        self.capital_por_simbolo[orden.symbol] = self.capital_por_simbolo.get(
            orden.symbol, 0.0
        ) + ganancia
        capital_final = self.capital_por_simbolo[orden.symbol]
        info['capital_final'] = capital_final
        log.info(
            f'✅ CIERRE PARCIAL: {orden.symbol} | Beneficio: {ganancia:.2f} €')
        registro_metrico.registrar('cierre_parcial', {'symbol': orden.symbol,
            'retorno': retorno_total, 'beneficio': ganancia})
        rsi_val = get_rsi(df) if df is not None else None
        mom_val = get_momentum(df) if df is not None else None
        score_val, _ = self._calcular_score_tecnico(
            df, rsi_val, mom_val, orden.tendencia, orden.direccion
        ) if df is not None else (0.0, {})
        await self._registrar_salida_profesional(
            orden.symbol,
            {
                'tipo_salida': 'parcial',
                'estrategias_activas': orden.estrategias_activas,
                'score_tecnico_al_cierre': score_val,
                'configuracion_usada': self.config_por_simbolo.get(orden.symbol, {}),
                'tiempo_operacion': 0.0,
                'beneficio_relativo': retorno_total,
            },
        )
        try:
            registrar_auditoria(
                symbol=orden.symbol,
                evento=motivo,
                resultado='ganancia' if retorno_total > 0 else 'pérdida',
                estrategias_activas=orden.estrategias_activas,
                score=score_val,
                rsi=rsi_val,
                tendencia=orden.tendencia,
                capital_actual=capital_final,
                config_usada=self.config_por_simbolo.get(orden.symbol, {}),
            )
        except Exception as e:
            log.debug(f'No se pudo registrar auditoría de cierre parcial: {e}')
        return True

    async def es_salida_parcial_valida(self, orden, precio_tp: float, config:
        dict, df: pd.DataFrame) ->bool:
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
        if not await verificar_filtro_tecnico(orden.symbol, df, orden.
            estrategias_activas, pesos_symbol, config=config):
            return False
        return True

    async def _piramidar(self, symbol: str, orden, df: pd.DataFrame) ->None:
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
        """Compatibilidad con ``monitorear_estado_periodicamente``."""
        return self.orders.ordenes

    def ajustar_capital_diario(self, factor: float=0.2, limite: float=0.3,
        penalizacion_corr: float=0.2, umbral_corr: float=0.8, fecha: (
        datetime.date | None)=None) ->None:
        """Redistribuye el capital según múltiples métricas adaptativas."""
        if self._limite_riesgo_notificado:
            if self.notificador:
                try:
                    asyncio.create_task(
                        safe_notify(
                            self.notificador.enviar_async(
                                '✅ Riesgo diario restablecido. Bot reanudado.',
                                'INFO',
                            )
                        )
                    )
                except Exception as e:
                    log.error(f'❌ Error enviando notificación: {e}')
            self._limite_riesgo_notificado = False
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
            self.capital_por_simbolo[symbol] = float(round_decimal(total * pesos[symbol] /
                suma, 2))
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
            reserva = float(round_decimal(reserva, 2))
            self.capital_por_simbolo[symbol] -= reserva
            self.reservas_piramide[symbol] = reserva
        self.capital_inicial_diario = self.capital_por_simbolo.copy()
        self.fecha_actual = fecha or datetime.now(UTC).date()
        log.info(f'💰 Capital redistribuido: {self.capital_por_simbolo}')

    async def _precargar_historico(self, velas: int = 12) -> None:
        """Carga datos recientes para todos los símbolos antes de iniciar."""
        if self.warmup_completed:
            log.info('📈 Histórico ya precargado, omitiendo')
            return
        cliente_temp = None
        cliente = self.cliente
        if cliente is None:
            try:
                cfg_tmp = replace(self.config, modo_real=True)
                cliente_temp = crear_cliente(cfg_tmp)
                cliente = cliente_temp
                log.info('📈 Precargando histórico usando cliente temporal')
            except Exception as e:
                log.info(f'📈 No se pudo precargar histórico desde Binance: {e}')
        for symbol in self.estado.keys():
            try:
                df = await warmup_symbol(symbol, self.config.intervalo_velas, cliente)
            except BaseError as e:
                log.warning(f'⚠️ Error cargando histórico para {symbol}: {e}')
                continue
            except Exception as e:
                log.warning(f'⚠️ Error inesperado cargando histórico para {symbol}: {e}')
                continue
            for fila in df.to_dict('records'):
                self.estado[symbol].buffer.append(fila)
            if not df.empty:
                self.estado[symbol].ultimo_timestamp = int(df['timestamp'].iloc[-1])
                self.estado[symbol].df = df.drop(columns=['symbol'], errors='ignore')
                self.config_por_simbolo[symbol] = adaptar_configuracion(
                    symbol,
                    self.estado[symbol].df,
                    self.config_por_simbolo.get(symbol, {}),
                )
                tendencia, _ = detectar_tendencia(symbol, self.estado[symbol].df)
                self.estado[symbol].tendencia_detectada = tendencia
                self.estado_tendencia[symbol] = tendencia
        if cliente_temp is not None:
            try:
                cliente_temp.close()
            except Exception:
                pass
        self.warmup_completed = True
        log.info('📈 Histórico inicial cargado')

    async def _ciclo_aprendizaje(self, intervalo: int = 86400, max_fallos: int = 5) -> None:
        """Ejecuta el proceso de aprendizaje continuo periódicamente.
        max_fallos: detiene el ciclo tras N errores consecutivos
        """
        await asyncio.sleep(1)
        fallos_consecutivos = 0
        # intervalo en el que se reporta actividad al supervisor
        intervalo_tick = max(1, self.heartbeat_interval // 2)
        while True:
            inicio = time.time()
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
            restante = max(0, intervalo - (time.time() - inicio))
            while restante > 0:
                tick('aprendizaje')
                espera = min(restante, intervalo_tick)
                await asyncio.sleep(espera)
                restante -= espera

    async def _calcular_cantidad_async(self, symbol: str, precio: float, stop_loss: float) -> tuple[float, float]:
        """Solicita precio y cantidad al servicio de capital mediante eventos."""
        exposicion = sum(
            o.cantidad_abierta * o.precio_entrada for o in self.orders.ordenes.values()
        )
        fut = asyncio.get_running_loop().create_future()
        fee_pct = getattr(self.config, 'fee_pct', 0.0)
        slippage_pct = getattr(self.config, 'slippage_pct', 0.0)
        await self.bus.publish(
            'calcular_cantidad',
            {
                'symbol': symbol,
                'precio': precio,
                'stop_loss': stop_loss,
                'exposicion_total': exposicion,
                'fee_pct': fee_pct,
                'slippage_pct': slippage_pct,
                'future': fut,
            },
        )
        try:
            return await asyncio.wait_for(
                fut, timeout=self.config.timeout_bus_eventos
            )
        except asyncio.TimeoutError:
            log.error(f'⏰ Timeout calculando cantidad para {symbol}')
            return precio, 0.0

    def _metricas_recientes(self, dias: int=7) ->dict:
        """Calcula ganancia acumulada y drawdown de los últimos ``dias``."""
        carpeta = reporter_diario.carpeta
        if not os.path.isdir(carpeta):
            return {'ganancia_semana': 0.0, 'drawdown': 0.0, 'winrate': 0.0,
                'capital_actual': sum(self.capital_por_simbolo.values()),
                'capital_inicial': sum(self.capital_inicial_diario.values())}
        fecha_limite = datetime.now(UTC).date() - timedelta(days=dias)
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
        """Cuenta señales válidas recientes para ``symbol``."""
        estado = self.estado.get(symbol)
        if not estado:
            return 0
        limite = datetime.now(UTC).timestamp() * 1000 - minutos * 60 * 1000
        return sum(
            1
            for v in estado.buffer
            if _ts_ms(v.get('timestamp')) >= limite and v.get('estrategias_activas')
        )

    def _obtener_historico(self, symbol: str,
        max_rows: int | None=None) ->(pd.DataFrame | None):
        """Devuelve el DataFrame de histórico para ``symbol`` usando caché.

        ``max_rows`` limita el número de filas conservadas en memoria.
        """
        df = self.historicos.get(symbol)
        if df is None:
            archivo = os.path.join(
                DATOS_DIR,
                f"{symbol.replace('/', '_').lower()}_{self.config.intervalo_velas}.parquet",
            )
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

    def _calcular_correlaciones(self, periodos: int = 1440, symbols: list[str] | None = None) -> pd.DataFrame:
        """Calcula correlación histórica de cierres entre símbolos.

        Usa una caché para evitar recalcular en cada señal y permite
        limitar los símbolos evaluados.
        """
        intervalo = getattr(self, 'frecuencia_correlaciones', 300)
        ahora = time.time()
        if symbols is None and self._correlaciones_cache is not None and ahora - self._correlaciones_ts < intervalo:
            return self._correlaciones_cache
        if symbols is not None and self._correlaciones_cache is not None and ahora - self._correlaciones_ts < intervalo:
            cols = self._correlaciones_cache.columns
            if set(symbols).issubset(cols):
                return self._correlaciones_cache.loc[symbols, symbols]
        precios: dict[str, pd.Series] = {}
        base_symbols = symbols or self.capital_por_simbolo.keys()
        for symbol in base_symbols:
            df = self._obtener_historico(symbol, max_rows=periodos)
            if df is not None and 'close' in df:
                precios[symbol] = df['close'].astype(float).tail(periodos).reset_index(drop=True)
        if len(precios) < 2:
            return pd.DataFrame()
        df_precios = pd.DataFrame(precios)
        correlaciones = df_precios.corr()
        if symbols is None:
            self._correlaciones_cache = correlaciones
            self._correlaciones_ts = ahora
        return correlaciones
    
    def _es_correlacion_btc_alta(
        self, symbol: str, ventana: int = 60
    ) -> tuple[bool, float | None]:
        """Evalúa si ``symbol`` está demasiado correlacionado con BTC.

        Devuelve una tupla ``(rechazar, rho)`` donde ``rechazar`` indica si la
        entrada debería rechazarse y ``rho`` es la correlación calculada o
        ``None`` si no pudo obtenerse.
        """

        df_symbol = self._obtener_historico(symbol, max_rows=ventana)
        df_btc = self._obtener_historico("BTC/EUR", max_rows=ventana)
        if df_symbol is None or df_btc is None:
            return False, None
        if "close" not in df_symbol or "close" not in df_btc:
            return False, None
        rho = correlacion_series(df_symbol["close"], df_btc["close"], ventana)
        if rho is None:
            return False, None
        registrar_correlacion_btc(symbol, rho)
        return abs(rho) >= self.umbral_correlacion_btc, rho

    def actualizar_indice_macro(self, nombre: str, valor: float) -> None:
        """Añade un nuevo valor de cierre al histórico del índice ``nombre``."""

        serie = self.historicos_macro.setdefault(
            nombre, deque(maxlen=self.max_filas_historico)
        )
        serie.append(float(valor))

    def _iniciar_tarea(self, nombre: str, factory: Callable[[], Awaitable]
        ) ->None:
        """Crea una tarea a partir de ``factory`` y la registra."""
        self._factories[nombre] = factory
        self._tareas[nombre] = supervised_task(factory, nombre)
        log.info(f'🚀 Tarea {nombre} iniciada')

    async def _vigilancia_tareas(self, intervalo: int = 60) -> None:
        while not self._cerrado:
            activos = 0
            ahora = datetime.now(UTC)
            for nombre, task in list(self._tareas.items()):
                if task.done():
                    if task.cancelled():
                        log.debug(
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
                    if nombre == 'flush':
                        mensaje = (
                            '⚠️ Heartbeat: tarea flush finalizada; no se reiniciará automáticamente'
                        )
                        log.error(mensaje)
                        if self.notificador:
                            await safe_notify(
                                self.notificador.enviar_async(mensaje, 'CRITICAL')
                            )
                        del self._tareas[nombre]
                        continue
                    stats = self._restart_stats[nombre]
                    ts_now = time.time()
                    stats.append(ts_now)
                    while stats and ts_now - stats[0] > 300:
                        stats.popleft()
                    if len(stats) > 5 and ts_now - stats[0] < 60:
                        log.error(f'🚨 Circuit breaker: demasiados reinicios para {nombre}')
                        continue
                    backoff = min(60, 2 ** (len(stats) - 1))
                    await asyncio.sleep(backoff)
                    otra_activa = any(
                        t.get_name() == nombre for t in asyncio.all_tasks()
                    )
                    self._iniciar_tarea(nombre, self._factories[nombre])
                    log.debug(
                        f'🔄 Tarea {nombre} reiniciada tras finalizar (otra instancia activa: {otra_activa})'
                    )
                    if nombre == 'data_feed':
                        log.debug('Data feed terminado y reiniciado por vigilancia')
                else:
                    hb = task_heartbeat.get(nombre, get_last_alive())
                    elapsed = (ahora - hb).total_seconds()
                    if elapsed > intervalo:
                        log.warning(
                            f'⏰ Tarea {nombre} sin actividad desde {hb.isoformat()} ({elapsed:.1f}s). Reiniciando.'
                        )
                        task.cancel()
                        try:
                            await task
                        except Exception:
                            pass
                        stats = self._restart_stats[nombre]
                        ts_now = time.time()
                        stats.append(ts_now)
                        while stats and ts_now - stats[0] > 300:
                            stats.popleft()
                        if len(stats) > 5 and ts_now - stats[0] < 60:
                            log.error(
                                f'🚨 Circuit breaker: demasiados reinicios para {nombre}'
                            )
                            continue
                        backoff = min(60, 2 ** (len(stats) - 1))
                        await asyncio.sleep(backoff)
                        otra_activa = any(
                            t.get_name() == nombre for t in asyncio.all_tasks()
                        )
                        self._iniciar_tarea(nombre, self._factories[nombre])
                        log.debug(
                            f'🔄 Tarea {nombre} reiniciada por inactividad (otra instancia activa: {otra_activa})'
                        )
                    else:
                        activos += 1
            for sym, ts in data_heartbeat.items():
                sin_datos = (ahora - ts).total_seconds()
                registro_metrico.registrar('sin_datos', {'symbol': sym, 'tiempo_s': sin_datos})
                if sin_datos > intervalo * 3:
                    if sym not in self._sin_datos_alertado and self.notificador:
                        asyncio.create_task(
                            safe_notify(
                                self.notificador.enviar_async(
                                    f'⚠️ Sin datos recientes para {sym}', 'WARNING'
                                )
                            )
                        )
                        self._sin_datos_alertado.add(sym)
                elif sym in self._sin_datos_alertado:
                    self._sin_datos_alertado.remove(sym)
            log.debug(
                f'🟢 Heartbeat: tareas activas {activos}/{len(self._tareas)}'
            )
            tick('heartbeat')
            await asyncio.sleep(intervalo)
    
    async def _sincronizar_ordenes_periodicamente(self, intervalo: int = 60) -> None:
        while not self._cerrado:
            try:
                await asyncio.to_thread(
                    real_orders.sincronizar_ordenes_binance,
                    self.config.symbols,
                    self.config,
                )
                log.debug('🔄 Sincronización de órdenes completada')
            except Exception as e:
                log.warning(f'⚠️ Error al sincronizar órdenes: {e}')
            await asyncio.sleep(intervalo)

    def _validar_puntaje(self, symbol: str, puntaje: float, umbral: float,
        modo_agresivo: bool=False) -> bool:
        """Comprueba si ``puntaje`` supera ``umbral``.

        Si ``modo_agresivo`` es ``True`` permite continuar incluso cuando el
        puntaje no alcanza el umbral establecido.
        """
        log.info(
            f"[{symbol}] Puntaje={puntaje:.2f} | Umbral adaptativo={umbral:.2f}"
        )
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
        diversidad_min: int, df: pd.DataFrame, modo_agresivo: bool=False
        ) -> bool:
        """Verifica que la diversidad y el peso total sean suficientes.

        Cuando ``modo_agresivo`` es ``True`` tolera valores por debajo del
        mínimo requerido sin marcar la operación como rechazada.
        """
        estrategias_activas = estrategias_activas or {}
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
                self._registrar_rechazo(
                    symbol,
                    f'Diversidad {diversidad} < {diversidad_min} o peso {peso_total:.2f} < {peso_min_total:.2f}',
                    peso_total=peso_total,
                )
                metricas_tracker.registrar_filtro('diversidad')
                return False
            log.debug(
                f'⚠️ {symbol}: Diversidad {diversidad}/{diversidad_min} o peso {peso_total:.2f}/{peso_min_total:.2f} en modo agresivo'
                )
        return True

    def _validar_estrategia(self, symbol: str, df: pd.DataFrame,
        estrategias: Dict, config: Dict | None=None) ->bool:
        """Aplica el filtro estratégico de entradas respetando la configuración."""
        config = config or {}
        min_div = config.get('diversidad_minima', 2)
        peso_min = config.get('peso_minimo_total', 0.5)
        pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
        if not evaluar_validez_estrategica(symbol, df, estrategias,
            pesos=pesos_symbol, minimo_peso_total=peso_min,
            min_diversidad=min_div):
            log.debug(
                f'❌ Entrada rechazada por filtro estratégico en {symbol}.')
            return False
        return True

    def _evaluar_persistencia(self, symbol: str, estado: EstadoSimbolo, df:
        pd.DataFrame, pesos_symbol: Dict[str, float], tendencia_actual: str,
        puntaje: float, umbral: float, estrategias: Dict[str, bool]) ->tuple[
        bool, float, float]:
        """Evalúa si las señales persistentes son suficientes para entrar."""
        ventana_close = df['close'].tail(10)
        media_close = np.mean(ventana_close)
        minimo = calcular_persistencia_minima(symbol, df, tendencia_actual,
            base_minimo=self.persistencia.minimo)
        if np.isnan(media_close) or isclose(media_close, 0.0, rel_tol=1e-12, abs_tol=1e-12):
            log.debug(
                f'⚠️ {symbol}: Media de cierre inválida para persistencia')
            return False, 0.0, minimo
        repetidas = coincidencia_parcial(estado.estrategias_buffer,
            pesos_symbol, ventanas=5)
        log.info(
            f'Persistencia detectada {repetidas:.2f} | Mínimo requerido {minimo:.2f}'
            )
        if repetidas < minimo:
            self._registrar_rechazo(
                symbol,
                f'Persistencia {repetidas:.2f} < {minimo}',
                puntaje=puntaje,
                estrategias=list(estrategias.keys()),
            )
            metricas_tracker.registrar_filtro('persistencia')
            return False, repetidas, minimo
        if repetidas < 1 and puntaje < 1.2 * umbral:
            self._registrar_rechazo(
                symbol,
                f'{repetidas:.2f} coincidencia y puntaje débil ({puntaje:.2f})',
                puntaje=puntaje,
                estrategias=list(estrategias.keys()),
            )
            return False, repetidas, minimo
        elif repetidas < 1:
            log.info(
                f'⚠️ Entrada débil en {symbol}: Coincidencia {repetidas:.2f} insuficiente pero puntaje alto ({puntaje}) > Umbral {umbral} — Permitida.'
                )
            metricas_tracker.registrar_filtro('persistencia')
        return True, repetidas, minimo

    def _tendencia_persistente(self, symbol: str, df: pd.DataFrame,
        tendencia: str, velas: int=3) ->bool:
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
        try:
            df = self._obtener_historico(symbol)
        except Exception as e:
            log.debug(f'No se pudo obtener histórico para {symbol}: {e}')
            return True
        if df is None or len(df) < 30:
            return True
        try:
            atr = get_atr(df)
        except Exception as e:
            log.debug(f'No se pudo calcular ATR para {symbol}: {e}')
            return True
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

    def _calcular_score_tecnico(
        self,
        df: pd.DataFrame,
        rsi: float | None,
        momentum: float | None,
        tendencia: str,
        direccion: str,
    ) -> tuple[float, ScoreBreakdown]:
        """Calcula un puntaje técnico simple a partir de varios indicadores."""
        slope = get_slope(df)
        score_total, breakdown = calcular_score_tecnico(
            df, rsi, momentum, slope, tendencia, direccion
        )
        log.info(
            '📊 Score técnico: %.2f | RSI: %.2f, Momentum: %.4f, Slope: %.4f, Tendencia: %.2f',
            score_total,
            breakdown.rsi,
            breakdown.momentum,
            breakdown.slope,
            breakdown.tendencia,
        )
        return float(score_total), breakdown

    def _hay_contradicciones(self, df: pd.DataFrame, rsi: (float | None),
        momentum: (float | None), direccion: str, score: float) ->bool:
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
        """Verifica que las señales no estén perdiendo fuerza."""
        rsi_series = get_rsi(df, serie_completa=True)
        if rsi_series is None or len(rsi_series) < 3:
            return True
        r = rsi_series.iloc[-3:]
        if direccion == 'long' and not r.iloc[-1] > r.iloc[-2] > r.iloc[-3]:
            return False
        if direccion == 'short' and not r.iloc[-1] < r.iloc[-2] < r.iloc[-3]:
            return False
        slope3 = get_slope(df, periodo=3)
        slope5 = get_slope(df, periodo=5)
        if direccion == 'long' and not slope3 > slope5:
            return False
        if direccion == 'short' and not slope3 < slope5:
            return False
        return True


    async def evaluar_condiciones_entrada(self, symbol: str, df: pd.DataFrame
        ) ->None:
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
        try:
            resultado = await asyncio.wait_for(
                self.engine.evaluar_entrada(
                    symbol,
                    df,
                    tendencia=tendencia_actual,
                    config={
                        **config_actual,
                        "contradicciones_bloquean_entrada": self.contradicciones_bloquean_entrada,
                        "usar_score_tecnico": self.usar_score_tecnico,
                    },
                    pesos_symbol=self.pesos_por_simbolo.get(symbol, {}),
                ),
                timeout=self.config.timeout_evaluar_condiciones,
            )
        except asyncio.TimeoutError:
            log.warning(f'⚠️ Timeout en evaluar_entrada para {symbol}')
            self._registrar_rechazo(
                symbol,
                'timeout_engine',
                estrategias=[],
            )
            return
        estrategias = resultado.get('estrategias_activas', {})
        if estado.estrategias_buffer:
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
                self.rejection_handler.registrar_tecnico(symbol, score, puntos,
                    tendencia_actual, precio_actual, resultado.get(
                    'motivo_rechazo', 'desconocido'), estrategias)
            self._registrar_rechazo(
                symbol,
                resultado.get('motivo_rechazo', 'desconocido'),
                puntaje=resultado.get('score_total'),
                estrategias=list(estrategias.keys()),
            )
            return
        info = await self.evaluar_condiciones_de_entrada(symbol, df, estado)
        if not info:
            self._registrar_rechazo(
                symbol,
                'filtros_post_engine',
                puntaje=resultado.get('score_total'),
                estrategias=list(estrategias.keys()),
            )
            return
        await asyncio.wait_for(
            self._abrir_operacion_real(**info),
            timeout=self.config.timeout_abrir_operacion,
        )

    async def _abrir_operacion_real(
        self,
        symbol: str,
        precio: float,
        sl: float,
        tp: float,
        estrategias: Dict[str, float] | List[str],
        tendencia: str,
        direccion: str,
        candle_close_ts: int,
        strategy_version: str,
        puntaje: float = 0.0,
        umbral: float = 0.0,
        score_tecnico: float = 0.0,
        detalles_tecnicos: dict | None = None,
        **kwargs,
    ) -> None:
        capital_total = sum(
            getattr(self, "capital_inicial_diario", self.capital_manager.capital_por_simbolo).values()
        )
        if getattr(self, 'risk', None) and self.risk.riesgo_superado(capital_total):
            log.warning(
                f'⛔ No se abre posición en {symbol}: límite de riesgo diario superado.'
            )
            if not self._limite_riesgo_notificado and self.notificador:
                await safe_notify(
                    self.notificador.enviar_async(
                        '🚨 Límite de riesgo diario superado. Bot pausado.',
                        'CRITICAL',
                    )
                )
                self._limite_riesgo_notificado = True
                await self._cerrar_posiciones_por_riesgo()
            self._stop_event.set()
            registrar_decision(symbol, 'risk_limit')
            return
        precio, cantidad_total = await self._calcular_cantidad_async(symbol, precio, sl)
        if cantidad_total <= 0:
            capital_disp = self.capital_manager.capital_por_simbolo.get(
                symbol, 0.0)
            log.warning(
                f'⛔ No se abre posición en {symbol} por capital insuficiente. '
                f'Disponible: {capital_disp:.2f}€')
            if self.notificador:
                await safe_notify(
                    self.notificador.enviar_async(
                        f'⚠️ No se abre posición en {symbol}: capital insuficiente ('
                        f'{capital_disp:.2f}€ disponible)'
                    )
                )
            registrar_decision(symbol, 'no_capital')
            return
        fracciones = self.piramide_fracciones
        market = await self.capital_manager.info_mercado(symbol)
        cantidad = cantidad_total / fracciones
        if market.step_size > 0:
            cantidad = math.floor(cantidad / market.step_size) * market.step_size
        if market.min_notional and precio * cantidad < market.min_notional:
            log.warning(
                f'⛔ Orden fraccionaria para {symbol} inferior al mínimo Binance {market.min_notional}'
            )
            return
        if self.capital_manager.cliente:
            try:
                ticker = await asyncio.wait_for(
                    fetch_ticker_async(self.capital_manager.cliente, symbol),
                    timeout=3,
                )
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
                self._registrar_rechazo(
                    symbol,
                    'sl_tp_invalidos',
                    puntaje=puntaje,
                    estrategias=list(estrategias_dict.keys()),
                )
                return
        except Exception as e:
            log.error(f'❌ Error validando SL/TP para {symbol}: {e}')
            self._registrar_rechazo(
                symbol,
                'error_validacion_sl_tp',
                puntaje=puntaje,
                estrategias=list(estrategias_dict.keys()),
            )
            if self.notificador:
                await safe_notify(
                    self.notificador.enviar_async(
                        f'⚠️ Error validando SL/TP para {symbol}: {e}'
                    )
                )
            return
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
            score_tecnico,
            objetivo=cantidad_total,
            fracciones=fracciones,
            detalles_tecnicos=detalles_tecnicos or {},
            candle_close_ts=candle_close_ts,
            strategy_version=strategy_version,
        )
        registrar_decision(symbol, 'entry')
        self.capital_inicial_orden[symbol] = self.capital_por_simbolo.get(symbol, 0.0)
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
        await verificar_salidas(self, symbol, df)

    async def evaluar_condiciones_de_entrada(self, symbol: str, df: pd.
        DataFrame, estado: EstadoSimbolo) ->(dict | None):
        capital_total = sum(self.capital_inicial_diario.values())
        if self.risk.riesgo_superado(capital_total):
            log.warning('⛔ Límite de riesgo diario superado. Bloqueando nuevas entradas.')
            if not self._limite_riesgo_notificado and self.notificador:
                await safe_notify(
                    self.notificador.enviar_async(
                        '🚨 Límite de riesgo diario superado. Bot pausado.',
                        'CRITICAL',
                    )
                )
                self._limite_riesgo_notificado = True
                await self._cerrar_posiciones_por_riesgo()
            self._stop_event.set()
            return None
        if not self._validar_config(symbol):
            return None
        operacion = await verificar_entrada(self, symbol, df, estado)
        if not operacion:
            return None
        abiertas_scores = {
            s: getattr(o, 'score_tecnico', 0.0) for s, o in self.orders.ordenes.items()
        }
        correlaciones = self._calcular_correlaciones(
            symbols=[symbol, *abiertas_scores.keys()]
        )
        if requiere_ajuste_diversificacion(
            symbol,
            operacion.get('score_tecnico', 0.0),
            abiertas_scores,
            correlaciones,
            getattr(self.config, 'umbral_corr_diversificacion', 0.8),
        ):
            incremento = getattr(
                self.config, 'incremento_umbral_diversificacion', 0.1
            )
            operacion['umbral'] *= 1 + incremento
            if operacion['puntaje'] < operacion['umbral']:
                log.info(f'[{symbol}] Rechazo por diversificación correlacionada')
                metricas_tracker.registrar_filtro('diversificacion')
                return None
        rechazado, rho = self._es_correlacion_btc_alta(symbol)
        if rechazado:
            log.info(f'[{symbol}] Rechazo por correlación con BTC: {rho:.2f}')
            metricas_tracker.registrar_filtro('correlacion_btc')
            return None
        return operacion

    async def ejecutar(self) ->None:
        """Inicia el procesamiento de todos los símbolos."""

        async def handle(candle: dict) ->None:
            await self._procesar_vela(candle)

        async def handle_context(symbol: str, score: float) -> None:
            self.puntajes_contexto[symbol] = score
            log.debug(f'🔁 Contexto actualizado {symbol}: {score:.2f}')
        symbols = list(self.estado.keys())
        base_segundos = intervalo_a_segundos(self.config.intervalo_velas)
        velas_precarga = math.ceil(
            60 * intervalo_a_segundos('5m') / base_segundos
        )
        await self._precargar_historico(velas=velas_precarga)
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
            'external_feeds': lambda: self.external_feeds.escuchar(symbols),
            'flush': lambda: real_orders.flush_periodico(),
            'rechazos_flush': lambda: self.rejection_handler.flush_periodically(
                self._rechazos_intervalo_flush, self._stop_event
            ),
        }
        if self.modo_real:
            tareas['sincronizar_ordenes'] = (
                lambda: self._sincronizar_ordenes_periodicamente()
            )
        if 'PYTEST_CURRENT_TEST' not in os.environ:
            tareas['aprendizaje'] = lambda : self._ciclo_aprendizaje()
        for nombre, factory in tareas.items():
            self._iniciar_tarea(nombre, factory)
        self._iniciar_tarea('heartbeat', lambda : self._vigilancia_tareas(self.heartbeat_interval))
        await self._stop_event.wait()

    async def _procesar_vela(self, vela: dict) ->None:
        symbol = vela.get('symbol')
        if not self._validar_config(symbol):
            return
        serie = self.series_precio[symbol]
        serie.append(vela.get('close'))
        serie_pd = pd.Series(serie)
        # Cálculos rolling sobre la serie
        vela['sma_20'] = serie_pd.rolling(window=20).mean().iloc[-1]
        vela['rsi'] = calcular_rsi(serie_pd, 14)
        await procesar_vela(self, vela)
        return

    async def cerrar(self) ->None:
        self._cerrado = True
        self._stop_event.set()
        for nombre, tarea in list(self._tareas.items()):
            if nombre == 'data_feed':
                await self.data_feed.detener()
            if nombre == 'context_stream':
                await self.context_stream.detener()
            if nombre == 'external_feeds':
                await self.external_feeds.detener()
            tarea.cancel()
        await asyncio.gather(*self._tareas.values(), return_exceptions=True)
        await self.bus.close()
        real_orders.flush_operaciones()
        self.rejection_handler.flush()
        registro_metrico.exportar()
        await self._guardar_estado_persistente()

    async def _guardar_estado_persistente(self) -> None:
        """Guarda historial de cierres y capital en ``ESTADO_DIR``."""
        try:
            os.makedirs(ESTADO_DIR, exist_ok=True)
            await asyncio.to_thread(
                self._save_json_file,
                os.path.join(ESTADO_DIR, 'historial_cierres.json'),
                self.historial_cierres,
            )
            await asyncio.to_thread(
                self._save_json_file,
                os.path.join(ESTADO_DIR, 'capital.json'),
                self.capital_por_simbolo,
            )
            await asyncio.to_thread(adaptador_umbral.guardar_estado)
            buffers = {
                s: list(e.estrategias_buffer)[-100:]
                for s, e in self.estado.items()
                if e.estrategias_buffer
            }
            await asyncio.to_thread(
                self._save_json_file,
                os.path.join(ESTADO_DIR, 'estrategias_buffer.json'),
                buffers,
            )
            await asyncio.to_thread(
                self._save_json_file,
                os.path.join(ESTADO_DIR, 'persistencia_conteo.json'),
                self.persistencia.conteo,
            )
        except Exception as e:
            log.warning(f'⚠️ Error guardando estado persistente: {e}')

    async def _cargar_estado_persistente(self) -> None:
        """Carga el estado previo de ``ESTADO_DIR`` si existe."""
        try:
            data = await asyncio.to_thread(
                self._load_json_file,
                os.path.join(ESTADO_DIR, 'historial_cierres.json'),
            )
            if data:
                self.historial_cierres.update(data)
            data = await asyncio.to_thread(
                self._load_json_file, os.path.join(ESTADO_DIR, 'capital.json')
            )
            if data:
                self.capital_por_simbolo.update(
                    {k: float(v) for k, v in data.items()}
                )
            await asyncio.to_thread(adaptador_umbral.cargar_estado)
            data = await asyncio.to_thread(
                self._load_json_file,
                os.path.join(ESTADO_DIR, 'estrategias_buffer.json'),
            )
            for sym, buf in data.items():
                if sym in self.estado and isinstance(buf, list):
                    self.estado[sym].estrategias_buffer = deque(
                        buf, maxlen=MAX_ESTRATEGIAS_BUFFER
                    )
            data = await asyncio.to_thread(
                self._load_json_file,
                os.path.join(ESTADO_DIR, 'persistencia_conteo.json'),
            )
            if data:
                self.persistencia.conteo.update(
                    {s: {k: int(v) for k, v in d.items()} for s, d in data.items()}
                )
        except Exception as e:
            log.warning(f'⚠️ Error cargando estado persistente: {e}')

    def _puede_evaluar_entradas(self, symbol: str) -> bool:
        """Determina si existen condiciones para evaluar nuevas compras."""
        if not self.capital_manager.tiene_capital(symbol):
            log.info(f'[{symbol}] Entrada ignorada por capital agotado')
            registrar_decision(symbol, 'skip_no_capital')
            return False
        if self.orders.tiene_posicion(symbol):
            registrar_decision(symbol, 'skip_open_position')
            return False
        return True
        

    def _validar_config(self, symbol: str) ->bool:
        """Valida que exista configuración para ``symbol``."""
        cfg = self.config_por_simbolo.get(symbol)
        if not isinstance(cfg, dict):
            log.error(f'⚠️ Configuración no encontrada para {symbol}')
            return False
        return True
