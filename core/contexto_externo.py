"""Gestión de contexto fundamental recibido por streaming desde Binance."""

from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable, Dict, Iterable

import websockets
from core.logger import configurar_logger

log = configurar_logger("contexto_externo")

_PUNTAJES: Dict[str, float] = {}

CONTEXT_WS_URL = "wss://stream.binance.com:9443/ws/{symbol}@kline_1m"


def obtener_puntaje_contexto(symbol: str) -> float:
    """Devuelve el último puntaje conocido para ``symbol``."""
    valor = _PUNTAJES.get(symbol)
    try:
        return float(valor) if valor is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def obtener_todos_puntajes() -> dict:
    """Devuelve todos los puntajes actuales almacenados."""
    return dict(_PUNTAJES)


class StreamContexto:
    """Conecta con Binance y actualiza el contexto en tiempo real."""

    def __init__(self, url_template: str | None = None) -> None:
        self.url_template = url_template or CONTEXT_WS_URL
        self._tasks: Dict[str, asyncio.Task] = {}

    async def _stream(
        self, symbol: str, handler: Callable[[str, float], Awaitable[None]]
    ) -> None:
        symbol_norm = symbol.replace("/", "").lower()
        url = self.url_template.format(symbol=symbol_norm)
        while True:
            try:
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20
                ) as ws:
                    log.info(f"🔌 Contexto conectado para {symbol}")
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            vela = data.get("k")
                            if vela is None:
                                continue
                            close = float(vela.get("c", 0.0))
                            open_ = float(vela.get("o", 0.0))
                            vol = float(vela.get("v", 0.0))

                            if open_ == 0 or vol < 1e-8:
                                continue

                            # Versión normalizada del puntaje
                            variacion_pct = (close - open_) / open_
                            puntaje = variacion_pct * vol

                            _PUNTAJES[symbol] = puntaje
                            log.info(f"🔁 Contexto actualizado {symbol}: {puntaje:.4f}")

                            try:
                                await handler(symbol, puntaje)
                            except Exception as e:
                                log.warning(f"⚠️ Handler contexto {symbol} falló: {e}")
                        except asyncio.CancelledError:
                            log.info(f"🛑 Stream contexto {symbol} cancelado (mensaje).")
                            raise
                        except Exception as e:
                            log.warning(f"⚠️ Error procesando contexto de {symbol}: {e}")
                    break
            except asyncio.CancelledError:
                log.info(f"🛑 Conexión de contexto {symbol} cancelada")
                break
            except Exception as e:
                log.warning(f"⚠️ Stream de contexto {symbol} falló: {e}. Reintentando en 5s")
                await asyncio.sleep(5)

    async def escuchar(
        self, symbols: Iterable[str], handler: Callable[[str, float], Awaitable[None]]
    ) -> None:
        """Inicia un stream por cada símbolo."""
        for sym in symbols:
            self._tasks[sym] = asyncio.create_task(self._stream(sym, handler))

        while self._tasks:
            tareas = list(self._tasks.items())
            resultados = await asyncio.gather(
                *[t for _, t in tareas], return_exceptions=True
            )
            reiniciar = {}
            for (sym, task), resultado in zip(tareas, resultados):
                if isinstance(resultado, asyncio.CancelledError):
                    continue
                if isinstance(resultado, Exception):
                    log.warning(f"⚠️ Stream de contexto {sym} terminó con error: {resultado}")
                    await asyncio.sleep(5)
                    reiniciar[sym] = asyncio.create_task(self._stream(sym, handler))
                else:
                    self._tasks[sym] = task
            if reiniciar:
                self._tasks.update(reiniciar)
            else:
                break

    async def detener(self) -> None:
        """Cancela todos los streams en ejecución."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)


__all__ = ["StreamContexto", "obtener_puntaje_contexto", "obtener_todos_puntajes"]
