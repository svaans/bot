from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from time import perf_counter
import pandas as pd

from core.utils.utils import configurar_logger
from core.registro_metrico import registro_metrico
from core.strategies.tendencia import detectar_tendencia

log = configurar_logger("procesar_vela")

async def procesar_vela(trader, vela: dict) -> None:
    symbol = vela["symbol"]
    inicio = perf_counter()
    log.debug(f"Inicio procesamiento vela {symbol}")
    estado = trader.estado[symbol]
    trader.watchdog.ping("procesar_vela")
    if datetime.now(timezone.utc).date() != trader.fecha_actual:
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
        t_sal = perf_counter()
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
        finally:
            dur_sal = (perf_counter() - t_sal) * 1000.0
            log.debug(f"[{symbol}] verificar_salidas tomó {dur_sal:.2f} ms")
        return
    else:
        # Mantiene vivo el watchdog aunque no haya órdenes abiertas
        trader.watchdog.ping("verificar_salidas")

    t_ent = perf_counter()
    try:
        await asyncio.wait_for(
            trader.evaluar_condiciones_entrada(symbol, df), timeout=20
        )
    except asyncio.TimeoutError:
        log.error(f"Timeout evaluando entrada de {symbol}")
        if trader.notificador:
            try:
                await trader.notificador.enviar_async(
                    f"⚠️ Timeout evaluando entrada de {symbol}"
                )
            except Exception as e:
                log.error(f"❌ Error enviando notificación: {e}")
        trader.watchdog.ping("verificar_entrada")
    finally:
        dur_ent = (perf_counter() - t_ent) * 1000.0
        log.debug(f"[{symbol}] evaluar_entrada tomó {dur_ent:.2f} ms")
    t_kelly = perf_counter()
    try:
        trader.actualizar_fraccion_kelly()
    except Exception as e:  # noqa: BLE001
        log.exception("No se pudo actualizar fracción Kelly", exc_info=e)
    finally:
        dur_kelly = (perf_counter() - t_kelly) * 1000.0
        log.debug(f"[{symbol}] actualizar_fraccion_kelly tomó {dur_kelly:.2f} ms")
    log.debug(f"🔄 Vela procesada {symbol}")
    trader.watchdog.ping("procesar_vela")
    duracion = (perf_counter() - inicio) * 1000.0
    registro_metrico.registrar("proc_vela", {"symbol": symbol, "ms": duracion})
    log.debug(f"Fin procesamiento vela {symbol} ({duracion:.2f} ms)")
    return
