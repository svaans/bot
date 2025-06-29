"""Módulo para gestionar el flujo de datos desde Binance."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Dict, Iterable

import grpc
from core import candle_pb2, candle_pb2_grpc

from core.utils.logger import configurar_logger
from core.async_utils import log_exceptions_async

log = configurar_logger("datafeed", modo_silencioso=True)


class DataFeed:
    """Maneja la recepción de velas de Binance en tiempo real."""

    def __init__(
        self,
        intervalo: str,
        host: str = "localhost",
        puerto: int = 9000,
        timeout: float | None = None,
    ) -> None:
        self.intervalo = intervalo
        self.host = host
        self.puerto = puerto
        self.timeout = timeout
        self._tasks: Dict[str, asyncio.Task] = {}
        self._last_candle: Dict[str, float] = {}
        self._count: Dict[str, int] = {}

    @property
    def activos(self) -> list[str]:
        """Lista de símbolos con streams activos."""
        return list(self._tasks.keys())
    
    def metricas(self, symbol: str) -> dict[str, float | int | None]:
        """Devuelve información del último mensaje recibido de ``symbol``."""
        return {
            "cuenta": self._count.get(symbol, 0),
            "ultimo": self._last_candle.get(symbol),
        }
        
    @log_exceptions_async
    async def stream(
        self, symbol: str, handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Escucha las velas de ``symbol`` via gRPC y reintenta ante fallos."""
        address = f"{self.host}:{self.puerto}"
        while True:
            channel = grpc.aio.insecure_channel(address)
            stub = candle_pb2_grpc.CandleServiceStub(channel)
            try:
                req = candle_pb2.CandleRequest(symbol=symbol, interval=self.intervalo)
                call = stub.Subscribe(req, timeout=self.timeout)
                async for candle in call:
                    self._count[symbol] = self._count.get(symbol, 0) + 1
                    self._last_candle[symbol] = asyncio.get_event_loop().time()
                    data = {
                        "symbol": candle.symbol,
                        "timestamp": candle.timestamp,
                        "open": candle.open,
                        "high": candle.high,
                        "low": candle.low,
                        "close": candle.close,
                        "volume": candle.volume,
                    }
                    await handler(data)
            except (
                grpc.aio.AioRpcError,
                OSError,
                ConnectionError,
            ) as e:  # pragma: no cover - conexión externa
                log.warning(f"⚠️ Stream {symbol} falló: {e}. Reintentando en 5s")
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # pragma: no cover - inesperado
                log.exception(f"❌ Error inesperado en stream {symbol}: {e}")
                raise
            finally:
                await channel.close()
                
    @log_exceptions_async
    async def escuchar(
        self, symbols: Iterable[str], handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        """Inicia un stream por cada símbolo y espera a que todos finalicen."""
        for sym in symbols:
            if sym in self._tasks:
                log.warning(f"⚠️ Stream duplicado para {sym}. Ignorando.")
                continue
            self._tasks[sym] = asyncio.create_task(self.stream(sym, handler))

        while self._tasks:
            tareas_actuales = list(self._tasks.items())
            resultados = await asyncio.gather(
                *[t for _, t in tareas_actuales], return_exceptions=True
            )
            reiniciar = {}
            for (sym, task), resultado in zip(tareas_actuales, resultados):
                self._tasks.pop(sym, None)
                if isinstance(resultado, asyncio.CancelledError):
                    continue
                if isinstance(resultado, Exception):
                    log.error(
                        f"⚠️ Stream {sym} finalizó con excepción: {resultado}. Reiniciando en 5s"
                    )
                    await asyncio.sleep(5)
                    reiniciar[sym] = asyncio.create_task(self.stream(sym, handler))
                

            if reiniciar:
                self._tasks.update(reiniciar)
            elif not self._tasks:
                break

    async def detener(self) -> None:
        """Cancela todos los streams en ejecución."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

