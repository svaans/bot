from __future__ import annotations

import asyncio
import concurrent.futures
import atexit
import time
from datetime import datetime, timezone
import pandas as pd


from core.async_utils import dump_tasks_stacktraces
from core.job_queue import Job, enqueue_job

from core.utils.utils import configurar_logger
from core.registro_metrico import registro_metrico
from core.strategies.tendencia import detectar_tendencia

log = configurar_logger("procesar_vela")

# Executor compartido para tareas CPU-bound. Permite aislar el procesamiento
# técnico y evitar que bloquee el event loop principal.
cpu_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)


def _shutdown_executor() -> None:
    cpu_executor.shutdown(wait=False, cancel_futures=True)


atexit.register(_shutdown_executor)


def calcular_puntuacion(symbol: str, buffer: list[dict]) -> tuple[pd.DataFrame, str]:
    """Calcula indicadores técnicos y tendencia para ``symbol``.

    Este cálculo se realiza en un ``ThreadPoolExecutor`` porque es una operación
    intensiva en CPU y no queremos bloquear el event loop mientras se procesa.
    """

    df = pd.DataFrame(buffer)
    tendencia, _ = detectar_tendencia(symbol, df)
    return df, tendencia

async def procesar_vela(trader, vela: dict) -> None:
    symbol = vela["symbol"]
    inicio = time.monotonic()
    log.debug(f"Inicio procesamiento vela {symbol}")
    log.debug(f"Símbolos gestionados: {len(getattr(trader.config, 'symbols', []))}")
    estado = trader.estado[symbol]
    loop = asyncio.get_event_loop()
    trader.watchdog.ping("procesar_vela")
    log.debug(f"[{symbol}] watchdog ping procesar_vela @ {loop.time():.6f}")
    try:
        if datetime.now(timezone.utc).date() != trader.fecha_actual:
            trader.ajustar_capital_diario()

        t_load = time.monotonic()
        estado.buffer.append(vela)
        if len(estado.buffer) > 120:
            estado.buffer = estado.buffer[-120:]
        if vela.get("timestamp") == estado.ultimo_timestamp:
            log.debug(
                f"[{symbol}] vela repetida ignorada {estado.ultimo_timestamp}"
            )
            return
        estado.ultimo_timestamp = vela.get("timestamp")
        dur_load = (time.monotonic() - t_load) * 1000.0
        log.debug(f"[{symbol}] cargar_vela tomó {dur_load:.2f} ms")

        t_trend = time.monotonic()
        df_size_before = len(estado.buffer)
        trader.watchdog.ping("procesar_vela")
        log.debug(f"[{symbol}] watchdog ping procesar_vela @ {loop.time():.6f}")
        try:
            df, tendencia = await asyncio.wait_for(
                loop.run_in_executor(
                    cpu_executor, calcular_puntuacion, symbol, list(estado.buffer)
                ),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            log.error(f"[{symbol}] Timeout calculando tendencia")
            return
        except Exception as exc:
            log.exception(f"[{symbol}] Error en cpu_executor", exc_info=exc)
            return
        estado.tendencia_detectada = tendencia
        log.debug(
            f"[{symbol}] DataFrame tamaño antes {df_size_before} después {len(df)}"
        )
        trader.estado_tendencia[symbol] = estado.tendencia_detectada
        dur_trend = (time.monotonic() - t_trend) * 1000.0
        log.debug(f"[{symbol}] detectar_tendencia tomó {dur_trend:.2f} ms")
        log.info(f"Procesando vela {symbol} | Precio: {vela.get('close')}")

        if trader.orders.obtener(symbol):
            if trader.job_queue.qsize() > 200:
                log.warning(
                    f"⚠️ job_queue saturada: {trader.job_queue.qsize()} elementos"
                )
            else:
                await enqueue_job(
                    trader.job_queue,
                    Job(0, "exit", symbol, df),
                    drop_policy=trader.config.job_drop_policy,
                )
            return
        else:
            # Mantiene vivo el watchdog aunque no haya órdenes abiertas
            trader.watchdog.ping("verificar_salidas")

        if trader.job_queue.qsize() > 200:
            log.warning(
                f"⚠️ job_queue saturada: {trader.job_queue.qsize()} elementos"
            )
        else:
            await enqueue_job(
                trader.job_queue,
                Job(1, "entry", symbol, df),
                drop_policy=trader.config.job_drop_policy,
            )
    finally:
        log.debug(f"🔄 Vela procesada {symbol}")
        trader.watchdog.ping("procesar_vela")
        log.debug(f"[{symbol}] watchdog ping procesar_vela @ {loop.time():.6f}")
        duracion = time.monotonic() - inicio
        registro_metrico.registrar("proc_vela", {"symbol": symbol, "ms": duracion * 1000.0})
        if duracion > 2.0:
            log.warning(f"⚠️ procesamiento de vela lento: {duracion:.2f}s")
        log.debug(f"Fin procesamiento vela {symbol} ({duracion*1000.0:.2f} ms)")
    return

async def consumir_velas(trader, queue: asyncio.Queue) -> None:
    """Consume velas desde ``queue`` y las procesa secuencialmente."""

    procesadas = 0
    while True:
        try:
            vela = await asyncio.wait_for(queue.get(), timeout=300)
        except asyncio.TimeoutError:
            log.warning("No llegaron velas en 5 minutos")
            continue
        try:
            await procesar_vela(trader, vela)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # pragma: no cover - log unexpected errors
            log.exception("Error procesando vela", exc_info=e)
        finally:
            queue.task_done()
            procesadas += 1
            if procesadas % 10 == 0 and queue.qsize() > 100:
                log.warning(
                    f"⚠️ cola de velas congestionada: {queue.qsize()} elementos"
                )
