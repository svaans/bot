"""Módulo para gestionar el flujo de datos desde Binance."""

from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable, Dict, Iterable

from core.utils.logger import configurar_logger
from core.async_utils import log_exceptions_async

log = configurar_logger("datafeed", modo_silencioso=True)


class DataFeed:
    """Maneja la recepción de velas de Binance en tiempo real."""

    def __init__(self, intervalo: str, host: str = "localhost", puerto: int = 9000) -> None:
        self.intervalo = intervalo
        self.host = host
        self.puerto = puerto
        self._tasks: Dict[str, asyncio.Task] = {}

    @property
    def activos(self) -> list[str]:
        """Lista de símbolos con streams activos."""
        return list(self._tasks.keys())
        
    @log_exceptions_async
    async def stream(self, symbol: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        """Escucha las velas de ``symbol`` y reintenta ante fallos de conexión."""
        while True:
            writer = None
            try:
                reader, writer = await asyncio.open_connection(self.host, self.puerto)
                req = json.dumps({"symbol": symbol, "interval": self.intervalo}) + "\n"
                writer.write(req.encode())
                await writer.drain()
                log.info(f"🔌 Conectado al servicio para {symbol}")
                while True:
                    line = await reader.readline()
                    if not line:
                        raise ConnectionError("socket cerrado")
                    data = json.loads(line.decode())
                    await handler(data)
            except (OSError, ConnectionError, json.JSONDecodeError) as e:  # pragma: no cover - conexión externa
                log.warning(f"⚠️ Stream {symbol} falló: {e}. Reintentando en 5s")
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception(f"❌ Error inesperado en stream {symbol}: {e}")
                raise
            finally:
                if writer:
                    writer.close()
                    await writer.wait_closed()
                
    @log_exceptions_async
    async def escuchar(self, symbols: Iterable[str], handler: Callable[[dict], Awaitable[None]]) -> None:
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

