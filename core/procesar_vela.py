from __future__ import annotations
import asyncio
import logging
import time
import os
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import deque, defaultdict
from itertools import islice
from types import MappingProxyType
import numpy as np
import pandas as pd
from core.utils.utils import (
    configurar_logger,
    obtener_uso_recursos,
    is_valid_number,
    intervalo_a_segundos,
)
from core.strategies.tendencia import obtener_tendencia
from indicators.helpers import clear_cache
from core.indicadores import get_rsi, get_momentum, get_atr
from core.registro_metrico import registro_metrico
from core.utils.perf import CandleProfiler
from core.metrics import (
    registrar_vela_recibida,
    registrar_vela_rechazada,
)
from observability import metrics as obs_metrics
from observability.metrics import (
    EVALUAR_ENTRADA_LATENCY_MS,
    EVALUAR_ENTRADA_TIMEOUTS,
)
from prometheus_client import Counter

"""Procesa una vela de mercado y actualiza indicadores.

Las funciones ``clear_cache`` y las actualizaciones incrementales de
indicadores modifican un estado compartido, por lo que se utiliza un
``asyncio.Lock`` para evitar condiciones de carrera cuando múltiples velas se
procesan en paralelo.
"""

log = configurar_logger('procesar_vela')
UTC = timezone.utc

AJUSTE_CAPITAL_SALTADO = obs_metrics._get_metric(
    Counter,
    "ajuste_capital_saltado_total",
    "Ajustes de capital diarios omitidos",
    ["symbol", "motivo"],
)

# Protege el acceso a funciones de indicadores que no son thread-safe por símbolo
_indicadores_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

# Lock por (símbolo, intervalo) para serializar el procesamiento de velas
_vela_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

MAX_BUFFER_VELAS = int(os.getenv('MAX_BUFFER_VELAS', 180))
MAX_ESTRATEGIAS_BUFFER = MAX_BUFFER_VELAS


@dataclass(slots=True)
class _IndicatorCacheEntry:
    window_hash: int
    timestamp: int
    values: dict[str, float | None]


_indicator_cache: dict[tuple[str, str], _IndicatorCacheEntry] = {}


def _compute_buffer_hash(buffer: deque, sample: int = 120) -> int:
    if not buffer:
        return 0
    subset = list(buffer)[-sample:]
    return hash(tuple(int(c.get('timestamp', 0)) for c in subset))


def _get_cached_indicators(
    symbol: str, intervalo: str, window_hash: int, timestamp: int
) -> dict[str, float | None] | None:
    entry = _indicator_cache.get((symbol, intervalo))
    if entry and entry.window_hash == window_hash and entry.timestamp == timestamp:
        return entry.values
    return None


def _store_indicator_cache(
    symbol: str,
    intervalo: str,
    window_hash: int,
    timestamp: int,
    values: dict[str, float | None],
) -> None:
    _indicator_cache[(symbol, intervalo)] = _IndicatorCacheEntry(
        window_hash=window_hash,
        timestamp=timestamp,
        values=values,
    )


def _invalidate_indicator_cache(symbol: str, intervalo: str) -> None:
    _indicator_cache.pop((symbol, intervalo), None)


def _approximate_spread(snapshot: MappingProxyType | dict[str, float]) -> float:
    close = float(snapshot.get('close', 0.0) or 0.0)
    if close <= 0:
        return 0.0
    high = float(snapshot.get('high', close) or close)
    low = float(snapshot.get('low', close) or close)
    return abs(high - low) / close


def _volume_gate(df: pd.DataFrame, minimo_relativo: float) -> tuple[bool, dict[str, float]]:
    if 'volume' not in df.columns or df.empty:
        return True, {'avg': 0.0, 'ultimo': 0.0}
    ventana = min(len(df), 20)
    if ventana < 5:
        return True, {'avg': 0.0, 'ultimo': float(df['volume'].iloc[-1])}
    vol_avg = float(df['volume'].tail(ventana).mean())
    ultimo = float(df['volume'].iloc[-1])
    if vol_avg <= 0:
        return True, {'avg': vol_avg, 'ultimo': ultimo}
    permitido = ultimo >= vol_avg * max(minimo_relativo, 0.0)
    return permitido, {'avg': vol_avg, 'ultimo': ultimo}


def _spread_gate(trader, symbol: str, snapshot: MappingProxyType | dict[str, float]) -> tuple[bool, float, float]:
    spread_ratio = _approximate_spread(snapshot)
    if trader.spread_guard:
        permitido = trader.spread_guard.allows(symbol, spread_ratio)
        limite = trader.spread_guard.current_limit(symbol)
    else:
        limite = getattr(trader.config, 'max_spread_ratio', 0.0) or 0.0
        permitido = spread_ratio <= limite if limite > 0 else True
    return permitido, spread_ratio, limite


async def _procesar_candle(
    trader,
    symbol: str,
    intervalo: str,
    estado,
    vela: dict,
    *,
    modo_degradado: bool = False,
    skip_notifications: bool = False,
    omitir_entradas: bool = False,
    omitir_tendencia: bool = False,
) -> None:
    ts = vela['timestamp']
    snapshot = {
        'symbol': symbol,
        'timestamp': ts,
        'open': float(vela['open']),
        'high': float(vela['high']),
        'low': float(vela['low']),
        'close': float(vela['close']),
        'volume': float(vela['volume']),
    }
    vela_inmutable = MappingProxyType(snapshot)

    notificador = (
        trader.notificador if not (modo_degradado and skip_notifications) else None
    )

    profiler = CandleProfiler(symbol, intervalo, getattr(trader, 'profile_candles', False))

    inicio = time.perf_counter()
    durations: dict[str, float] = {}

    def _notify(message: str, level: str = 'INFO') -> None:
        if notificador:
            with profiler.stage('notifications_io'):
                trader.enqueue_notification(message, level)

    def _persist(tipo: str, datos: dict, *, immediate: bool = False) -> None:
        with profiler.stage('persistence_io'):
            trader.enqueue_persistence(tipo, datos, immediate=immediate)

    recien_lleno = False
    df: pd.DataFrame

    with profiler.stage('state_update'):
        t_state = time.perf_counter()
        for attr, default in [
            ("df", pd.DataFrame()),
            ("df_idx", 0),
            ("_ring_ready", False),
            ("indicadores_wait_ms", 0.0),
            ("indicadores_calls", 0),
            ("indicadores_cache", {}),
            ("contador_tendencia", 0),
            ("timeouts_salidas", 0),
            ("cierres_timeouts", 0),
            ("entradas_timeouts", 0),
            ("tendencia_detectada", None),
            ("buffer", deque(maxlen=MAX_BUFFER_VELAS)),
            ("estrategias_buffer", deque(maxlen=MAX_ESTRATEGIAS_BUFFER)),
        ]:
            if not hasattr(estado, attr):
                setattr(estado, attr, default)

        if not isinstance(getattr(estado, "indicadores_cache", None), dict):
            estado.indicadores_cache = {}

        if datetime.now(UTC).date() != trader.fecha_actual:
            buffers_ready = len(getattr(trader, 'estado', {})) >= 2 and all(
                len(st.buffer) >= 2 for st in getattr(trader, 'estado', {}).values()
            )
            if not buffers_ready:
                msg = {
                    "evento": "ajuste_capital_skip",
                    "symbol": "*",
                    "corr_media": None,
                    "umbral_corr": 0.8,
                    "motivo_salto": "datos_insuficientes",
                }
                log.warning(json.dumps(msg))
                AJUSTE_CAPITAL_SALTADO.labels(symbol="*", motivo="datos_insuficientes").inc()
            else:
                trader.ajustar_capital_diario()

        if not isinstance(estado.buffer, deque):
            estado.buffer = deque(maxlen=MAX_BUFFER_VELAS)
        if not isinstance(estado.estrategias_buffer, deque):
            estado.estrategias_buffer = deque(maxlen=MAX_ESTRATEGIAS_BUFFER)

        estado.buffer.append(vela_inmutable)
        estado.estrategias_buffer.append({})
        estado.ultimo_timestamp = ts

        if not isinstance(estado.df.index, pd.RangeIndex):
            estado.df.reset_index(drop=True, inplace=True)

        if estado.df.empty:
            estado.df = pd.DataFrame([snapshot])
            estado.df_idx = len(estado.df) % MAX_BUFFER_VELAS
        elif len(estado.df) < MAX_BUFFER_VELAS:
            estado.df.loc[len(estado.df)] = snapshot
            estado.df_idx = len(estado.df) % MAX_BUFFER_VELAS
        else:
            estado.df.loc[estado.df_idx] = snapshot
            estado.df_idx = (estado.df_idx + 1) % MAX_BUFFER_VELAS

        recien_lleno = (
            len(estado.df) == MAX_BUFFER_VELAS
            and not getattr(estado, "_ring_ready", False)
        )
        if recien_lleno:
            estado._ring_ready = True

        if len(estado.df) < MAX_BUFFER_VELAS:
            base = estado.df.copy()
        else:
            idx = estado.df_idx
            orden = np.r_[idx:MAX_BUFFER_VELAS, 0:idx]
            base = estado.df.iloc[orden].reset_index(drop=True)

        if 'estrategias_activas' in base.columns:
            df = base.drop(columns=['estrategias_activas'])
        else:
            df = base.copy()

        durations['state_ms'] = (time.perf_counter() - t_state) * 1000

    if df.empty or 'close' not in df.columns:
        log.error(f"❌ DataFrame inválido para {symbol}: {df}")
        return
    
    # Indicadores (sección protegida por lock por símbolo)
    with profiler.stage('indicators'):
        lock_ind = _indicadores_locks[symbol]
        window_hash = _compute_buffer_hash(estado.buffer)
        ts_df = int(df['timestamp'].iloc[-1]) if 'timestamp' in df.columns else ts
        cached = _get_cached_indicators(symbol, intervalo, window_hash, ts_df)
        if cached is None:
            espera = time.perf_counter()
            async with lock_ind:
                estado.indicadores_wait_ms += (time.perf_counter() - espera) * 1000
                estado.indicadores_calls += 1
                if recien_lleno:
                    estado.indicadores_cache.clear()
                    await asyncio.to_thread(clear_cache, estado.df)
                    _invalidate_indicator_cache(symbol, intervalo)
                t_ind = time.perf_counter()
                valores = await asyncio.gather(
                    asyncio.to_thread(get_rsi, estado),
                    asyncio.to_thread(get_momentum, estado),
                    asyncio.to_thread(get_atr, estado),
                )
                durations["indicadores_ms"] = (time.perf_counter() - t_ind) * 1000
            indicadores = {
                'rsi': valores[0],
                'momentum': valores[1],
                'atr': valores[2],
            }
            _store_indicator_cache(symbol, intervalo, window_hash, ts_df, indicadores)
        else:
            indicadores = cached
            durations.setdefault("indicadores_ms", 0.0)
        estado.indicadores_hash = window_hash

    # Detección de tendencia: cada N velas (configurable)
    if getattr(estado, 'contador_tendencia', 0) == 0 and not (
        modo_degradado and omitir_tendencia
    ):
        with profiler.stage('trend'):
            t_trend = time.perf_counter()
            estado.tendencia_detectada = await asyncio.to_thread(
                obtener_tendencia, symbol, df
            )
            trader.estado_tendencia[symbol] = estado.tendencia_detectada
            dur_trend = time.perf_counter() - t_trend
            durations["tendencia_ms"] = dur_trend * 1000
            log.debug(
                f'obtener_tendencia tardó {dur_trend:.2f}s para {symbol}',
                extra={'symbol': symbol, 'timeframe': intervalo, 'timestamp': ts},
            )
    estado.contador_tendencia = (getattr(estado, 'contador_tendencia', 0) + 1) % max(getattr(trader.config, 'frecuencia_tendencia', 1), 1)

    log.debug(
        f"Procesando vela {symbol} | Precio: {vela_inmutable.get('close')}",
        extra={'symbol': symbol, 'timeframe': intervalo, 'timestamp': ts},
    )

    # Si las estrategias están deshabilitadas, terminar aquí
    if not getattr(trader, 'estrategias_habilitadas', True):
        log.debug(f'Estrategias deshabilitadas para {symbol}')
        return

    try:
        orden_existente = trader.orders.obtener(symbol)
        cancelar_por_salidas = False
        if orden_existente is not None:
            with profiler.stage('position_manager'):
                t_pm = time.perf_counter()
                try:
                    t_salidas = time.perf_counter()
                    await asyncio.wait_for(
                        trader._verificar_salidas(symbol, df),
                        timeout=trader.config.timeout_verificar_salidas,
                    )
                    dur_salidas = time.perf_counter() - t_salidas
                    durations["salidas_ms"] = dur_salidas * 1000
                    log.debug(
                        f'_verificar_salidas tardó {dur_salidas:.2f}s para {symbol}',
                        extra={'symbol': symbol, 'timeframe': intervalo, 'timestamp': ts},
                    )
                    estado.timeouts_salidas = 0
                except asyncio.TimeoutError:
                    log.error(f'⏰ Timeout verificando salidas de {symbol}')
                    estado.timeouts_salidas += 1
                    _notify(f'⚠️ Timeout verificando salidas de {symbol}', 'WARNING')
                if estado.timeouts_salidas >= trader.config.max_timeouts_salidas:
                    log.error(
                        f'🚨 Forzando cierre de {symbol} tras {estado.timeouts_salidas} timeouts'
                    )
                    precio_cierre = float(df['close'].iloc[-1])
                    try:
                        await asyncio.wait_for(
                            trader.cerrar_operacion(
                                symbol, precio_cierre, 'timeout_salidas'
                            ),
                            timeout=trader.config.timeout_cerrar_operacion,
                        )
                    except asyncio.TimeoutError:
                        estado.cierres_timeouts += 1
                        log.error(f'⏰ Timeout cerrando operación de {symbol}')
                        _notify(f'⚠️ Timeout cerrando operación de {symbol}', 'WARNING')
                    except Exception as cierre_err:
                        log.error(f'❌ Error forzando cierre de {symbol}: {cierre_err}')
                    estado.timeouts_salidas = 0
                    cancelar_por_salidas = True
                durations['position_manager_ms'] = durations.get(
                    'position_manager_ms', 0.0
                ) + (time.perf_counter() - t_pm) * 1000
            if cancelar_por_salidas:
                return

        with profiler.stage('risk_checks'):
            t_risk = time.perf_counter()
            try:
                capital_total = float(
                    sum(getattr(trader, 'capital_inicial_diario', {}).values())
                )
                if getattr(trader, 'risk', None) and trader.risk.riesgo_superado(
                    capital_total
                ):
                    log.warning(
                        f'[{symbol}] Límite de riesgo diario superado. Entrada bloqueada.'
                    )
                    _notify(
                        '🚨 Límite de riesgo diario superado. Bot pausado.', 'CRITICAL'
                    )
                    return
                if not trader._puede_evaluar_entradas(symbol):
                    return
                if modo_degradado and omitir_entradas:
                    log.debug(
                        f'[{symbol}] Entrada omitida por modo degradado (backlog alto)'
                    )
                    return
                permitido_vol, vol_stats = _volume_gate(
                    df, getattr(trader.config, 'volumen_min_relativo', 1.0)
                )
                if not permitido_vol:
                    log.debug(
                        f'[{symbol}] Volumen insuficiente: último={vol_stats["ultimo"]:.2f}, '
                        f'promedio={vol_stats["avg"]:.2f}'
                    )
                    return
                with profiler.stage('spread_guard'):
                    t_spread = time.perf_counter()
                    spread_ok, spread_ratio, spread_limit = _spread_gate(
                        trader, symbol, snapshot
                    )
                    durations['spread_guard_ms'] = (
                        time.perf_counter() - t_spread
                    ) * 1000
                if not spread_ok:
                    log.debug(
                        f'[{symbol}] Entrada omitida por spread {spread_ratio:.4f} '
                        f'> límite {spread_limit:.4f}'
                    )
                    return
            finally:
                durations['risk_ms'] = (time.perf_counter() - t_risk) * 1000

        with profiler.stage('strategy_engine'):
            t_entrada = time.perf_counter()
            timeout_eval = getattr(
                trader.config,
                "timeout_evaluar_condiciones_por_symbol",
                {},
            ).get(symbol, trader.config.timeout_evaluar_condiciones)
            info = await asyncio.wait_for(
                trader.evaluar_condiciones_de_entrada(symbol, df, estado),
                timeout=timeout_eval,
            )
            dur_entry = time.perf_counter() - t_entrada
            durations["entrada_ms"] = dur_entry * 1000
        EVALUAR_ENTRADA_LATENCY_MS.labels(symbol=symbol).observe(durations["entrada_ms"])
        log.debug(
            f'evaluar_condiciones_de_entrada tardó {dur_entry:.2f}s para {symbol}',
            extra={'symbol': symbol, 'timeframe': intervalo, 'timestamp': ts},
        )
        if isinstance(info, dict) and info:
            try:
                with profiler.stage('position_manager'):
                    t_pm = time.perf_counter()
                    await asyncio.wait_for(
                        trader._abrir_operacion_real(**info),
                        timeout=trader.config.timeout_abrir_operacion,
                    )
                    durations['position_manager_ms'] = durations.get(
                        'position_manager_ms', 0.0
                    ) + (time.perf_counter() - t_pm) * 1000
            except asyncio.TimeoutError:
                estado.entradas_timeouts += 1
                log.error(f'⏰ Timeout abriendo operación de {symbol}')
                _notify(f'⚠️ Timeout abriendo operación de {symbol}', 'WARNING')
        elif info is not None:
            log.warning(
                f'⚠️ Resultado inesperado al evaluar entrada para {symbol}: {type(info)}'
            )
    except asyncio.TimeoutError:
        EVALUAR_ENTRADA_TIMEOUTS.labels(symbol=symbol).inc()
        log.error(f'⏰ Timeout en evaluar_condiciones_de_entrada para {symbol}')
        estado.entradas_timeouts += 1
        _notify(
            f'⚠️ Timeout en evaluar condiciones de entrada para {symbol}', 'WARNING'
        )
    except Exception as e:
        log.exception(f'❌ Error procesando vela de {symbol}: {e}')
        _notify(f'❌ Error procesando vela de {symbol}: {e}', 'ERROR')
    finally:
        duracion = time.perf_counter() - inicio
        if durations:
            payload = {
                "evento": "timing_procesar_vela",
                "symbol": symbol,
                "timeframe": intervalo,
                "durations_ms": {k: round(v, 2) for k, v in durations.items()},
            }
            log.info(json.dumps(payload))
            _persist('timing_procesar_vela', payload)
        # Métricas de recursos (con suavizado)
        ahora = time.perf_counter()
        if ahora - getattr(trader, '_recursos_ts', 0) >= getattr(trader, 'frecuencia_recursos', 60):
            cpu, mem = obtener_uso_recursos()
            trader._recursos_ts = ahora
            trader._ultimo_cpu = cpu
            trader._ultimo_mem = mem
        else:
            cpu = getattr(trader, '_ultimo_cpu', 0.0)
            mem = getattr(trader, '_ultimo_mem', 0.0)

        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                f'✅ procesar_vela completado en {duracion:.2f}s para {symbol} | CPU: {cpu:.1f}% | Memoria: {mem:.1f}%',
                extra={'symbol': symbol, 'timeframe': intervalo, 'timestamp': ts},
            )
        profiler.finalize(ts)

        if cpu > trader.config.umbral_alerta_cpu:
            trader._cpu_high_cycles += 1
        else:
            trader._cpu_high_cycles = 0
        if mem > trader.config.umbral_alerta_mem:
            trader._mem_high_cycles += 1
        else:
            trader._mem_high_cycles = 0


async def _procesar_candle_con_lock(
    trader,
    symbol: str,
    intervalo: str,
    estado,
    vela: dict,
    *,
    modo_degradado: bool = False,
    skip_notifications: bool = False,
    omitir_entradas: bool = False,
    omitir_tendencia: bool = False,
) -> None:
    """Procesa una vela asegurando el lock por símbolo e intervalo."""
    lock = _vela_locks[f'{symbol}:{intervalo}']
    async with lock:
        await _procesar_candle(
            trader,
            symbol,
            intervalo,
            estado,
            vela,
            modo_degradado=modo_degradado,
            skip_notifications=skip_notifications,
            omitir_entradas=omitir_entradas,
            omitir_tendencia=omitir_tendencia,
        )


async def procesar_vela(
    trader,
    vela: dict,
    *,
    modo_degradado: bool = False,
    skip_notifications: bool = False,
    omitir_entradas: bool = False,
    omitir_tendencia: bool = False,
) -> None:
    """Entrada principal para procesar una vela cerrada."""
    if not isinstance(vela, dict):
        log.error(f"❌ Formato de vela inválido: {vela}")
        registrar_vela_rechazada('desconocido', 'formato_invalido')
        return

    symbol = vela.get('symbol')
    if symbol is None:
        log.error(f"❌ Vela sin símbolo: {vela}")
        registrar_vela_rechazada('desconocido', 'sin_simbolo')
        return

    registrar_vela_recibida(symbol)

    intervalo = getattr(trader.config, 'intervalo_velas', '')
    if not intervalo:
        log.error("❌ intervalo_velas no configurado")
        registrar_vela_rechazada(symbol, 'intervalo_no_configurado')
        return

    # Estado debe existir (evitamos crear estructuras no verificadas)
    estado = trader.estado.get(symbol)
    if estado is None:
        log.error(f"❌ Estado no inicializado para {symbol}")
        registrar_vela_rechazada(symbol, 'estado_no_inicializado')
        return

    lock = _vela_locks[f'{symbol}:{intervalo}']
    async with lock:
        campos_requeridos = {'timestamp', 'close'}
        if not campos_requeridos.issubset(vela):
            log.error(f"❌ Vela incompleta para {symbol}: {vela}")
            registrar_vela_rechazada(symbol, 'incompleta')
            return

        # Normalización de campos opcionales
        vela.setdefault('open', vela['close'])
        vela.setdefault('high', vela['close'])
        vela.setdefault('low', vela['close'])
        vela.setdefault('volume', 0)

        # Validación de tipos/valores
        for campo in ('timestamp', 'open', 'high', 'low', 'close', 'volume'):
            if not is_valid_number(vela.get(campo)):
                log.error(f"❌ Valor inválido en campo {campo} para {symbol}: {vela.get(campo)}")
                registrar_vela_rechazada(symbol, f'valor_invalido_{campo}')
                return

        intervalo_ms = intervalo_a_segundos(intervalo) * 1000

        # Normalización/creación de buffers si vinieran desinicializados
        if not isinstance(estado.buffer, deque):
            estado.buffer = deque(maxlen=MAX_BUFFER_VELAS)
        if not isinstance(estado.estrategias_buffer, deque):
            estado.estrategias_buffer = deque(maxlen=MAX_ESTRATEGIAS_BUFFER)

        # Filtro de velas (dup/out-of-order/parcial) — requiere que estado.candle_filter exista
        if not hasattr(estado, 'candle_filter') or estado.candle_filter is None:
            log.error(f"❌ Falta candle_filter para {symbol}")
            registrar_vela_rechazada(symbol, 'sin_candle_filter')
            return

        ready, status, warn = estado.candle_filter.push(vela, intervalo_ms)

        if status == 'duplicate':
            log.info(f"Vela duplicada para {symbol}: {vela['timestamp']}")
            registrar_vela_rechazada(symbol, 'duplicada')
            if warn:
                log.warning(f'Alto ratio de velas descartadas para {symbol}')
                estado.candle_filter.reset()
            return

        if status == 'out_of_order':
            log.debug(f"Vela fuera de orden para {symbol}: {vela['timestamp']}")
            registrar_vela_rechazada(symbol, 'fuera_de_orden')
            if warn:
                log.warning(f'Alto ratio de velas descartadas para {symbol}')
                estado.candle_filter.reset()
            return

        if status == 'partial':
            log.debug(f"Ignorando kline parcial para {symbol}")
            return

        if warn:
            log.warning(f'Alto ratio de velas descartadas para {symbol}')
            estado.candle_filter.reset()

        if not ready:
            return

        # Procesa la primera de la lista bajo el lock actual
        primera, *resto = ready
        await _procesar_candle(
            trader,
            symbol,
            intervalo,
            estado,
            primera,
            modo_degradado=modo_degradado,
            skip_notifications=skip_notifications,
            omitir_entradas=omitir_entradas,
            omitir_tendencia=omitir_tendencia,
        )

        # El resto se procesa en tareas separadas; cada una adquirirá el lock interno
        for vela_proc in resto:
            asyncio.create_task(
                _procesar_candle_con_lock(
                    trader,
                    symbol,
                    intervalo,
                    estado,
                    vela_proc,
                    modo_degradado=modo_degradado,
                    skip_notifications=skip_notifications,
                    omitir_entradas=omitir_entradas,
                    omitir_tendencia=omitir_tendencia,
                )
            )
