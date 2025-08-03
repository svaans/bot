from __future__ import annotations
import asyncio
import logging
import time
from datetime import datetime
import pandas as pd
from core.utils.utils import configurar_logger, obtener_uso_recursos
from core.strategies.tendencia import detectar_tendencia

log = configurar_logger('procesar_vela')


async def procesar_vela(trader, vela: dict) -> None:
    log.debug('➡️ Entrando en procesar_vela()')
    symbol = vela['symbol']
    estado = trader.estado[symbol]

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
    if len(estado.buffer) > 120:
        estado.buffer = estado.buffer[-120:]
        estado.estrategias_buffer = estado.estrategias_buffer[-120:]

    estado.ultimo_timestamp = vela.get('timestamp')

    # Crear DataFrame y detectar tendencia
    df = pd.DataFrame(estado.buffer)
    df = df.drop(columns=['estrategias_activas'], errors='ignore')
    estado.tendencia_detectada, _ = detectar_tendencia(symbol, df)
    trader.estado_tendencia[symbol] = estado.tendencia_detectada

    log.debug(f"Procesando vela {symbol} | Precio: {vela.get('close')}")

    try:
        if trader.orders.obtener(symbol):
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
                    precio_cierre = float(df.iloc[-1].get('close'))
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
            if info:
                await trader._abrir_operacion_real(**info)
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
    finally:
        duracion = time.time() - inicio
        if log.isEnabledFor(logging.DEBUG):
            cpu, mem = obtener_uso_recursos()
            log.debug(
                f'✅ procesar_vela completado en {duracion:.2f}s para {symbol} | CPU: {cpu:.1f}% | Memoria: {mem:.1f}%'
            )
