"""Benchmark local para validar el control de backpressure del DataFeed."""
from __future__ import annotations

import asyncio
import contextlib
import os
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault('SUPERVISOR_NOTIFICATIONS', 'false')
os.environ.setdefault('DF_NOTIFICATIONS', 'false')

from core import supervisor
# Ajuste de importación tras mover DataFeed a data_feed.lite
from data_feed.lite import DataFeed


@dataclass
class BenchmarkResult:
    total_candles: int
    max_queue: int
    avg_wait_ms: float
    p95_wait_ms: float
    p99_wait_ms: float
    max_wait_ms: float
    avg_handler_ms: float
    duration_s: float
    throughput_cps: float


class DummyHandler:
    """Simula el procesamiento del trader con latencia controlada."""

    def __init__(self, process_time: float = 0.03) -> None:
        self.process_time = process_time
        self.waits_ms: List[float] = []
        self.handler_ms: List[float] = []

    async def __call__(self, candle: dict) -> None:
        start = time.perf_counter()
        wait = candle.get('_df_queue_wait_ms')
        if isinstance(wait, (int, float)):
            self.waits_ms.append(float(wait))
        await asyncio.sleep(self.process_time)
        self.handler_ms.append((time.perf_counter() - start) * 1000)


async def run_benchmark(
    *,
    bursts: int = 40,
    burst_factor: int = 8,
    base_interval: float = 0.25,
    handler_time: float = 0.035,
    queue_limit: int = 500,
    queue_policy: str = 'coalesce',
    coalesce_window_ms: int = 750,
    safety_policy: str = 'drop_oldest',
) -> BenchmarkResult:
    """Ejecuta un benchmark sintetizado con ráfagas ``burst_factor`` veces mayores."""

    handler = DummyHandler(process_time=handler_time)
    datafeed = DataFeed(
        '1s',
        backpressure=True,
        notifications_enabled=False,
    )
    datafeed.configure_queue_control(
        default_limit=queue_limit,
        default_policy=queue_policy,
        coalesce_window_ms=coalesce_window_ms,
        safety_policy=safety_policy,
        high_watermark=0.8,
        metrics_log_interval=10.0,
    )
    symbol = 'TEST/USDT'
    datafeed._symbols = [symbol]
    queue = asyncio.Queue(maxsize=datafeed._queue_maxsize_for(symbol))
    datafeed._queues = {symbol: queue}
    datafeed._running = True
    datafeed._consumer_tasks[symbol] = asyncio.create_task(
        datafeed._consumer(symbol, handler.__call__)
    )

    now = int(time.time() * 1000)
    interval_ms = int(datafeed.intervalo_segundos * 1000)
    aligned_start = (now // interval_ms) * interval_ms
    total_candles = bursts * burst_factor
    max_queue = 0
    start = time.perf_counter()
    for burst in range(bursts):
        # disparo de ráfaga ``burst_factor`` veces mayor que el ritmo base
        for i in range(burst_factor):
            ts = aligned_start + ((burst * burst_factor) + i) * interval_ms
            candle = {
                'symbol': symbol,
                'timestamp': ts,
                'open': 1.0,
                'high': 1.0,
                'low': 1.0,
                'close': 1.0,
                'volume': 1.0,
                'is_closed': True,
            }
            await datafeed._handle_candle(symbol, candle, force=True)
            max_queue = max(max_queue, queue.qsize())
        # pausa base para simular ritmo nominal
        await asyncio.sleep(base_interval)

    await queue.join()
    datafeed._running = False
    for task in datafeed._consumer_tasks.values():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await datafeed.detener()
    await supervisor.shutdown()

    waits = handler.waits_ms or [0.0]
    handler_lat = handler.handler_ms or [handler_time * 1000]
    duration = time.perf_counter() - start
    p95_wait = statistics.quantiles(waits, n=100, method='inclusive')[94] if len(waits) >= 100 else max(waits)
    p99_wait = statistics.quantiles(waits, n=100, method='inclusive')[98] if len(waits) >= 100 else max(waits)
    return BenchmarkResult(
        total_candles=total_candles,
        max_queue=max_queue,
        avg_wait_ms=sum(waits) / len(waits),
        p95_wait_ms=p95_wait,
        p99_wait_ms=p99_wait,
        max_wait_ms=max(waits),
        avg_handler_ms=sum(handler_lat) / len(handler_lat),
        duration_s=duration,
        throughput_cps=total_candles / duration if duration else float('inf'),
    )


async def main() -> None:
    result = await run_benchmark()
    print(
        (
            "Procesadas %d velas | cola máxima=%d | espera media=%.2fms | "
            "p95=%.2fms | p99=%.2fms | espera máx=%.2fms | handler medio=%.2fms | "
            "duración=%.2fs | throughput=%.2f velas/s"
        )
        % (
            result.total_candles,
            result.max_queue,
            result.avg_wait_ms,
            result.p95_wait_ms,
            result.p99_wait_ms,
            result.max_wait_ms,
            result.avg_handler_ms,
            result.duration_s,
            result.throughput_cps,
        )
    )
    if result.max_queue >= 500:
        raise AssertionError('La cola alcanzó el límite de 500 elementos')
    if result.p95_wait_ms > 2000:
        raise AssertionError(
            f'La espera p95 excede 2s (observado {result.p95_wait_ms:.2f}ms)'
        )


if __name__ == '__main__':
    asyncio.run(main())
