from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from time import perf_counter
import pandas as pd
import traceback

from core.async_utils import dump_tasks_stacktraces

from core.utils.utils import configurar_logger
from core.registro_metrico import registro_metrico
from core.strategies.tendencia import detectar_tendencia

log = configurar_logger("procesar_vela")

async def procesar_vela(trader, vela: dict) -> None:
    symbol = vela["symbol"]
    inicio = perf_counter()
    log.debug(f"Inicio procesamiento vela {symbol}")
    log.debug(f"Símbolos gestionados: {len(getattr(trader.config, 'symbols', []))}")
    estado = trader.estado[symbol]
    loop = asyncio.get_event_loop()
    trader.watchdog.ping("procesar_vela")
    log.debug(f"[{symbol}] watchdog ping procesar_vela @ {loop.time():.6f}")
    try:
        if datetime.now(timezone.utc).date() != trader.fecha_actual:
            trader.ajustar_capital_diario()

        t_load = perf_counter()
        estado.buffer.append(vela)
        if len(estado.buffer) > 120:
            estado.buffer = estado.buffer[-120:]
        if vela.get("timestamp") == estado.ultimo_timestamp:
            return
        estado.ultimo_timestamp = vela.get("timestamp")
        dur_load = (perf_counter() - t_load) * 1000.0
        log.debug(f"[{symbol}] cargar_vela tomó {dur_load:.2f} ms")

        t_trend = perf_counter()
        df_size_before = len(estado.buffer)
        df = await asyncio.to_thread(pd.DataFrame, estado.buffer)
        estado.tendencia_detectada, _ = await asyncio.to_thread(
            detectar_tendencia, symbol, df
        )
        log.debug(
            f"[{symbol}] DataFrame tamaño antes {df_size_before} después {len(df)}"
        )
        trader.estado_tendencia[symbol] = estado.tendencia_detectada
        dur_trend = (perf_counter() - t_trend) * 1000.0
        log.debug(f"[{symbol}] detectar_tendencia tomó {dur_trend:.2f} ms")
        log.info(f"Procesando vela {symbol} | Precio: {vela.get('close')}")

        if trader.orders.obtener(symbol):
            t_sal = perf_counter()
            try:
                sem = getattr(trader, "sem_global", None)
                if sem:
                    async with sem:  # limita concurrencia global
                        await asyncio.wait_for(
                            trader._verificar_salidas(symbol, df), timeout=20
                        )
                else:
                    await asyncio.wait_for(
                        trader._verificar_salidas(symbol, df), timeout=20
                    )
            except asyncio.TimeoutError:
                log.error(f"Timeout verificando salidas de {symbol}")
                log.error("Tareas:\n%s", dump_tasks_stacktraces(asyncio.all_tasks()))
                log.error("Stack actual:\n%s", "".join(traceback.format_stack()))
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
            sem = getattr(trader, "sem_global", None)
            if sem:
                async with sem:  # limita concurrencia global
                    await asyncio.wait_for(
                        trader.evaluar_condiciones_entrada(symbol, df), timeout=20
                    )
            else:
                await asyncio.wait_for(
                    trader.evaluar_condiciones_entrada(symbol, df), timeout=20
                )
        except asyncio.TimeoutError:
            log.error(f"Timeout evaluando entrada de {symbol}")
            log.error("Tareas:\n%s", dump_tasks_stacktraces(asyncio.all_tasks()))
            log.error("Stack actual:\n%s", "".join(traceback.format_stack()))
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
    finally:
        log.debug(f"🔄 Vela procesada {symbol}")
        trader.watchdog.ping("procesar_vela")
        log.debug(f"[{symbol}] watchdog ping procesar_vela @ {loop.time():.6f}")
        duracion = (perf_counter() - inicio) * 1000.0
        registro_metrico.registrar("proc_vela", {"symbol": symbol, "ms": duracion})
        log.debug(f"Fin procesamiento vela {symbol} ({duracion:.2f} ms)")
    return
