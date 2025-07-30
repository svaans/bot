from __future__ import annotations
import asyncio
import time
from datetime import datetime
import pandas as pd
from core.utils.utils import configurar_logger
from core.strategies.tendencia import detectar_tendencia

log = configurar_logger('procesar_vela')


async def procesar_vela(trader, vela: dict) -> None:
    log.info('➡️ Entrando en procesar_vela()')
    symbol = vela['symbol']
    estado = trader.estado[symbol]

    inicio = time.time()  # ⏱️ para medir cuánto tarda

    # Ajustar capital diario si es nuevo día
    if datetime.utcnow().date() != trader.fecha_actual:
        trader.ajustar_capital_diario()

    # Agregar vela al buffer y mantener tamaño
    estado.buffer.append(vela)
    if len(estado.buffer) > 120:
        estado.buffer = estado.buffer[-120:]

    # Ignorar vela duplicada
    if vela.get('timestamp') == estado.ultimo_timestamp:
        return

    estado.ultimo_timestamp = vela.get('timestamp')

    # Crear DataFrame y detectar tendencia
    df = pd.DataFrame(estado.buffer)
    estado.tendencia_detectada, _ = detectar_tendencia(symbol, df)
    trader.estado_tendencia[symbol] = estado.tendencia_detectada

    log.info(f"Procesando vela {symbol} | Precio: {vela.get('close')}")

    try:
        if trader.orders.obtener(symbol):
            # ⚠️ Validar salidas activas con timeout
            try:
                await asyncio.wait_for(
                    trader._verificar_salidas(symbol, df),
                    timeout=trader.config.timeout_verificar_salidas,
                )
            except asyncio.TimeoutError:
                log.error(f'⏰ Timeout verificando salidas de {symbol}')
                if trader.notificador:
                    try:
                        await trader.notificador.enviar_async(
                            f'⚠️ Timeout verificando salidas de {symbol}'
                        )
                    except Exception as e:
                        log.error(f'❌ Error enviando notificación: {e}')
            return

        # ⚠️ Validar condiciones de entrada con timeout
        try:
            await asyncio.wait_for(
                trader.evaluar_condiciones_entrada(symbol, df),
                timeout=trader.config.timeout_evaluar_condiciones,
            )
        except asyncio.TimeoutError:
            log.error(f'⏰ Timeout en evaluar_condiciones_entrada para {symbol}')
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
        log.info(f'✅ procesar_vela completado en {duracion:.2f}s para {symbol}')

