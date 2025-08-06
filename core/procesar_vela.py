from __future__ import annotations
import asyncio
import logging
import time
from datetime import datetime
import pandas as pd
from core.utils.utils import configurar_logger, obtener_uso_recursos
from core.strategies.tendencia import detectar_tendencia

log = configurar_logger('procesar_vela')

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

    # Asegurar que los buffers existen para evitar errores al usar len()
    if not isinstance(estado.buffer, list):
        estado.buffer = []
    if not isinstance(estado.estrategias_buffer, list):
        estado.estrategias_buffer = []

    inicio = time.time()  # ⏱️ para medir cuánto tarda

    # Ajustar capital diario si es nuevo día
    if datetime.utcnow().date() != trader.fecha_actual:
        trader.ajustar_capital_diario()

    # Ignorar vela duplicada antes de agregarla al buffer
    if vela.get('timestamp') == estado.ultimo_timestamp:
        return

    # Agregar vela al buffer y mantener tamaño
    estado.buffer.append(vela)
    estado.estrategias_buffer.append({})
    if len(estado.buffer) > MAX_BUFFER_VELAS:
        estado.buffer = estado.buffer[-MAX_BUFFER_VELAS:]
    estado.estrategias_buffer = estado.estrategias_buffer[-MAX_BUFFER_VELAS:]

    estado.ultimo_timestamp = vela.get('timestamp')

    # Actualizar DataFrame incrementalmente y detectar tendencia
    if estado.df.empty:
        estado.df = pd.DataFrame([vela])
    else:
        estado.df = pd.concat([estado.df, pd.DataFrame([vela])], ignore_index=True)
        if len(estado.df) > MAX_BUFFER_VELAS:
            estado.df = estado.df.tail(MAX_BUFFER_VELAS).reset_index(drop=True)

    df = estado.df.drop(columns=['estrategias_activas'], errors='ignore')
    if df.empty or 'close' not in df.columns:
        log.error(f"❌ DataFrame inválido para {symbol}: {df}")
        return
    estado.tendencia_detectada, _ = detectar_tendencia(symbol, df)
    trader.estado_tendencia[symbol] = estado.tendencia_detectada

    log.debug(f"Procesando vela {symbol} | Precio: {vela.get('close')}")

    try:
        orden_existente = trader.orders.obtener(symbol)
        if orden_existente is not None:
            # ⚠️ Validar salidas activas con timeout
            try:
                await asyncio.wait_for(
                    trader._verificar_salidas(symbol, df),
                    timeout=trader.config.timeout_verificar_salidas,
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
                        await trader.cerrar_operacion(
                            symbol, precio_cierre, 'timeout_salidas'
                        )
                    except Exception as e:
                        log.error(f'❌ Error forzando cierre de {symbol}: {e}')
                    estado.timeouts_salidas = 0
                return
            return

        # ⚠️ Validar condiciones de entrada con timeout
        try:
            info = await asyncio.wait_for(
                trader.evaluar_condiciones_de_entrada(symbol, df, estado),
                timeout=trader.config.timeout_evaluar_condiciones,
            )
            if isinstance(info, dict) and info:
                await trader._abrir_operacion_real(**info)
            elif info is not None:
                log.warning(
                    f'⚠️ Resultado inesperado al evaluar entrada para {symbol}: {type(info)}'
                )
        except asyncio.TimeoutError:
            log.error(
                f'⏰ Timeout en evaluar_condiciones_de_entrada para {symbol}'
            )
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
        duracion = time.time() - inicio
        if log.isEnabledFor(logging.DEBUG):
            cpu, mem = obtener_uso_recursos()
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
