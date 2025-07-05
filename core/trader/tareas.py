"""Módulo de gestión de tareas asincrónicas y ciclo de aprendizaje"""

from __future__ import annotations
import asyncio
from core.utils.utils import configurar_logger
from learning.aprendizaje_continuo import ejecutar_ciclo as ciclo_aprendizaje
from ccxt.base.errors import BaseError
from binance_api.cliente import fetch_ohlcv_async
from core.trader.trader import Trader  # Asegúrate de que la ruta sea correcta

log = configurar_logger("trader")

async def ciclo_aprendizaje_periodico(trader: Trader, intervalo: int = 86400, max_fallos: int = 5) -> None:
    """
    Ejecuta el proceso de aprendizaje continuo periódicamente.
    """
    await asyncio.sleep(1)
    fallos_consecutivos = 0
    while True:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, ciclo_aprendizaje)
            log.info("🧠 Ciclo de aprendizaje completado")
            fallos_consecutivos = 0
        except Exception as e:
            fallos_consecutivos += 1
            log.warning(f"⚠️ Error en ciclo de aprendizaje ({fallos_consecutivos} fallos): {e}")
            if fallos_consecutivos >= max_fallos:
                log.error("🛑 Deteniendo ciclo_aprendizaje tras demasiados fallos consecutivos")
                break
        await asyncio.sleep(intervalo)


async def precargar_historico(trader: Trader, velas: int = 12) -> None:
    """
    Precarga velas históricas para todos los símbolos en modo real
    """
    if not trader.modo_real or not trader.cliente:
        log.info("📈 Modo simulado: se omite precarga de histórico")
        return
    for symbol in trader.estado.keys():
        try:
            datos = await fetch_ohlcv_async(trader.cliente, symbol, trader.config.intervalo_velas, limit=velas)
        except BaseError as e:
            log.warning(f"⚠️ Error cargando histórico para {symbol}: {e}")
            continue
        except Exception as e:
            log.warning(f"⚠️ Error inesperado cargando histórico para {symbol}: {e}")
            continue
        for ts, open_, high_, low_, close_, vol in datos:
            trader.estado[symbol].buffer.append({
                'symbol': symbol,
                'timestamp': ts,
                'open': float(open_),
                'high': float(high_),
                'low': float(low_),
                'close': float(close_),
                'volume': float(vol)
            })
        if datos:
            trader.estado[symbol].ultimo_timestamp = datos[-1][0]
    log.info("📈 Histórico inicial cargado")

