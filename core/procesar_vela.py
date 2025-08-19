from __future__ import annotations
import asyncio
import logging
import time
from datetime import datetime, timezone
from collections import deque, defaultdict
import numpy as np
import pandas as pd
from core.utils.utils import configurar_logger, obtener_uso_recursos
from core.strategies.tendencia import obtener_tendencia
from indicators.helpers import clear_cache
from core.indicadores import get_rsi, get_momentum, get_atr

"""Procesa una vela de mercado y actualiza indicadores.

Las funciones ``clear_cache`` y las actualizaciones incrementales de
indicadores modifican un estado compartido, por lo que se utiliza un
``asyncio.Lock`` para evitar condiciones de carrera cuando múltiples velas se
procesan en paralelo.
"""

log = configurar_logger('procesar_vela')
UTC = timezone.utc

# Protege el acceso a funciones de indicadores que no son thread-safe por símbolo
_indicadores_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

MAX_BUFFER_VELAS = 120
MAX_ESTRATEGIAS_BUFFER = MAX_BUFFER_VELAS


async def procesar_vela(trader, vela: dict) -> None:
    log.debug('➡️ Entrando en procesar_vela()')
    if not isinstance(vela, dict):
        log.error(f"❌ Formato de vela inválido: {vela}")
        return
    symbol = vela.get('symbol')
    if symbol is None:
        log.error(f"❌ Vela sin símbolo: {vela}")
        return
    campos_requeridos = {'timestamp', 'close'}
    if not campos_requeridos.issubset(vela):
        log.error(f"❌ Vela incompleta para {symbol}: {vela}")
        return
    # Rellenar campos opcionales ausentes con valores por defecto para evitar errores
    vela.setdefault('open', vela['close'])
    vela.setdefault('high', vela['close'])
    vela.setdefault('low', vela['close'])
    vela.setdefault('volume', 0)
    estado = trader.estado[symbol]

    # Asegurar que los buffers existen como deques para evitar errores
    if not isinstance(estado.buffer, deque):
        estado.buffer = deque(maxlen=MAX_BUFFER_VELAS)
    if not isinstance(estado.estrategias_buffer, deque):
        estado.estrategias_buffer = deque(maxlen=MAX_ESTRATEGIAS_BUFFER)

    inicio = time.perf_counter()  # ⏱️ para medir cuánto tarda

    # Ajustar capital diario si es nuevo día (UTC)
    if datetime.now(UTC).date() != trader.fecha_actual:
        trader.ajustar_capital_diario()

    # Ignorar vela duplicada antes de agregarla al buffer
    if vela.get('timestamp') == estado.ultimo_timestamp:
        return

    # Agregar vela al buffer (deque maneja el tamaño automáticamente)
    estado.buffer.append(vela)
    estado.estrategias_buffer.append({})

    estado.ultimo_timestamp = vela.get('timestamp')

    # Actualizar DataFrame incrementalmente usando índice rotatorio
    if estado.df.empty:
        estado.df = pd.DataFrame([vela])
        estado.df_idx = len(estado.df) % MAX_BUFFER_VELAS
    elif len(estado.df) < MAX_BUFFER_VELAS:
        estado.df.loc[len(estado.df)] = vela
        estado.df_idx = len(estado.df) % MAX_BUFFER_VELAS
    else:
        estado.df.loc[estado.df_idx] = vela
        estado.df_idx = (estado.df_idx + 1) % MAX_BUFFER_VELAS
    # invalidar cache de indicadores porque los datos cambiaron
    # Estas funciones modifican estado compartido, por lo que se requiere
    # exclusión mutua para evitar condiciones de carrera.
    lock = _indicadores_locks[symbol]
    espera = time.perf_counter()
    async with lock:
        estado.indicadores_wait_ms += (time.perf_counter() - espera) * 1000
        estado.indicadores_calls += 1
        await asyncio.to_thread(clear_cache, estado.df)
        await asyncio.to_thread(get_rsi, estado)
        await asyncio.to_thread(get_momentum, estado)
        await asyncio.to_thread(get_atr, estado)

    if len(estado.df) < MAX_BUFFER_VELAS:
        df = await asyncio.to_thread(
            lambda: estado.df.drop(columns=['estrategias_activas'], errors='ignore')
        )
    else:
        idx = estado.df_idx
        orden = np.r_[idx:MAX_BUFFER_VELAS, 0:idx]
        df = await asyncio.to_thread(
            lambda: (
                estado.df.iloc[orden]
                .reset_index(drop=True)
                .drop(columns=['estrategias_activas'], errors='ignore')
            )
        )
    if df.empty or 'close' not in df.columns:
        log.error(f"❌ DataFrame inválido para {symbol}: {df}")
        return
    # calcular tendencia solo cada ``frecuencia_tendencia`` velas
    if getattr(estado, 'contador_tendencia', 0) == 0:
        t_trend = time.perf_counter()
        estado.tendencia_detectada = await asyncio.to_thread(
            obtener_tendencia, symbol, df
        )
        trader.estado_tendencia[symbol] = estado.tendencia_detectada
        log.debug(
            f'obtener_tendencia tardó {time.perf_counter() - t_trend:.2f}s para {symbol}'
        )
    estado.contador_tendencia = (
        getattr(estado, 'contador_tendencia', 0) + 1
    ) % max(getattr(trader.config, 'frecuencia_tendencia', 1), 1)

    log.debug(f"Procesando vela {symbol} | Precio: {vela.get('close')}")

    try:
        orden_existente = trader.orders.obtener(symbol)
        if orden_existente is not None:
            # ⚠️ Validar salidas activas con timeout
            try:
                t_salidas = time.perf_counter()
                await asyncio.wait_for(
                    trader._verificar_salidas(symbol, df),
                    timeout=trader.config.timeout_verificar_salidas,
                )
                log.debug(
                    f'_verificar_salidas tardó {time.perf_counter() - t_salidas:.2f}s para {symbol}'
                )
                estado.timeouts_salidas = 0
            except asyncio.TimeoutError:
                log.error(f'⏰ Timeout verificando salidas de {symbol}')
                estado.timeouts_salidas += 1
                if trader.notificador:
                    try:
                        await trader.notificador.enviar_async(
                            f'⚠️ Timeout verificando salidas de {symbol}'
                        )
                    except Exception as e:
                        log.error(f'❌ Error enviando notificación: {e}')
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
                        if trader.notificador:
                            try:
                                await trader.notificador.enviar_async(
                                    f'⚠️ Timeout cerrando operación de {symbol}'
                                )
                            except Exception as e:
                                log.error(f'❌ Error enviando notificación: {e}')
                    except Exception as e:
                        log.error(f'❌ Error forzando cierre de {symbol}: {e}')
                    estado.timeouts_salidas = 0
                return
            return
        
        if not trader._puede_evaluar_entradas(symbol):
            return

        # ⚠️ Validar condiciones de entrada con timeout
        try:
            t_entrada = time.perf_counter()
            info = await asyncio.wait_for(
                trader.evaluar_condiciones_de_entrada(symbol, df, estado),
                timeout=trader.config.timeout_evaluar_condiciones,
            )
            log.debug(
                f'evaluar_condiciones_de_entrada tardó {time.perf_counter() - t_entrada:.2f}s para {symbol}'
            )
            if isinstance(info, dict) and info:
                try:
                    await asyncio.wait_for(
                        trader._abrir_operacion_real(**info),
                        timeout=trader.config.timeout_abrir_operacion,
                    )
                except asyncio.TimeoutError:
                    estado.entradas_timeouts += 1
                    log.error(f'⏰ Timeout abriendo operación de {symbol}')
                    if trader.notificador:
                        try:
                            await trader.notificador.enviar_async(
                                f'⚠️ Timeout abriendo operación de {symbol}'
                            )
                        except Exception as e:
                            log.error(f'❌ Error enviando notificación: {e}')
            elif info is not None:
                log.warning(
                    f'⚠️ Resultado inesperado al evaluar entrada para {symbol}: {type(info)}'
                )
        except asyncio.TimeoutError:
            log.error(
                f'⏰ Timeout en evaluar_condiciones_de_entrada para {symbol}'
            )
            estado.entradas_timeouts += 1
            if trader.notificador:
                try:
                    await trader.notificador.enviar_async(
                        f'⚠️ Timeout en evaluar condiciones de entrada para {symbol}'
                    )
                except Exception as e:
                    log.error(f'❌ Error enviando notificación: {e}')
    except Exception as e:
        log.exception(f'❌ Error procesando vela de {symbol}: {e}')
        if trader.notificador:
            try:
                await trader.notificador.enviar_async(
                    f'❌ Error procesando vela de {symbol}: {e}'
                )
            except Exception as e2:
                log.error(f'❌ Error enviando notificación: {e2}')
    finally:
        duracion = time.perf_counter() - inicio
        ahora = time.perf_counter()
        if ahora - getattr(trader, '_recursos_ts', 0) >= getattr(
            trader, 'frecuencia_recursos', 60
        ):
            cpu, mem = obtener_uso_recursos()
            trader._recursos_ts = ahora
            trader._ultimo_cpu = cpu
            trader._ultimo_mem = mem
        else:
            cpu = getattr(trader, '_ultimo_cpu', 0.0)
            mem = getattr(trader, '_ultimo_mem', 0.0)
        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                f'✅ procesar_vela completado en {duracion:.2f}s para {symbol} | CPU: {cpu:.1f}% | Memoria: {mem:.1f}%'
            )
        if cpu > trader.config.umbral_alerta_cpu:
            trader._cpu_high_cycles += 1
        else:
            trader._cpu_high_cycles = 0
        if mem > trader.config.umbral_alerta_mem:
            trader._mem_high_cycles += 1
        else:
            trader._mem_high_cycles = 0
        if (
            trader._cpu_high_cycles >= trader.config.ciclos_alerta_recursos
            or trader._mem_high_cycles >= trader.config.ciclos_alerta_recursos
        ):
            if trader.notificador:
                try:
                    await trader.notificador.enviar_async(
                        f'⚠️ Uso de recursos elevado: CPU {cpu:.1f}% | Memoria {mem:.1f}%'
                    )
                except Exception as e:
                    log.error(f'❌ Error enviando notificación: {e}')
            trader._cpu_high_cycles = 0
            trader._mem_high_cycles = 0
