from __future__ import annotations

import asyncio
from datetime import datetime
import pandas as pd

from core.utils.utils import configurar_logger
from core.strategies.tendencia import detectar_tendencia

log = configurar_logger("procesar_vela")

async def procesar_vela(trader, vela: dict) -> None:
    symbol = vela["symbol"]
    estado = trader.estado[symbol]
    if datetime.utcnow().date() != trader.fecha_actual:
        trader.ajustar_capital_diario()

    estado.buffer.append(vela)
    if len(estado.buffer) > 120:
        estado.buffer = estado.buffer[-120:]
    if vela.get("timestamp") == estado.ultimo_timestamp:
        return

    estado.ultimo_timestamp = vela.get("timestamp")
    df = pd.DataFrame(estado.buffer)
    estado.tendencia_detectada, _ = detectar_tendencia(symbol, df)
    trader.estado_tendencia[symbol] = estado.tendencia_detectada
    log.info(f"Procesando vela {symbol} | Precio: {vela.get('close')}")

    if trader.orders.obtener(symbol):
        try:
            await asyncio.wait_for(
                trader._verificar_salidas(symbol, df), timeout=20
            )
        except asyncio.TimeoutError:
            log.error(f"Timeout verificando salidas de {symbol}")
            if trader.notificador:
                try:
                    await trader.notificador.enviar_async(
                        f"⚠️ Timeout verificando salidas de {symbol}"
                    )
                except Exception as e:
                    log.error(f"❌ Error enviando notificación: {e}")
        return

    await trader.evaluar_condiciones_entrada(symbol, df)
    return