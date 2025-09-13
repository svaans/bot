"""Gestor simple de conexiones WebSocket con reconexión automática."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

import websockets
from websockets.client import WebSocketClientProtocol

from core.utils.logger import configurar_logger
from data_feed.reconexion import calcular_backoff
from observabilidad import metrics as obs_metrics

log = configurar_logger("websocket_manager")


class WebSocketManager:
    """Gestiona la conexión a un WebSocket aplicando backoff exponencial."""

    def __init__(self, url: str, handler: Callable[[str], Awaitable[None]], max_reintentos: int = 5) -> None:
        self.url = url
        self.handler = handler
        self.max_reintentos = max_reintentos
        self.ws: WebSocketClientProtocol | None = None

    async def close(self) -> None:
        """Cierra la conexión WebSocket de forma controlada."""
        if self.ws is None:
            return
        try:
            await self.ws.close(code=1000)
            await asyncio.wait_for(self.ws.wait_closed(), timeout=5)
        except asyncio.TimeoutError:
            log.warning("Timeout al cerrar WS; forzando cierre")
            await self.ws.close_transport()
        finally:
            self.ws = None

    async def conectar(self) -> None:
        """Inicia la conexión y maneja reconexiones automáticas."""
        intentos = 0
        while True:
            try:
                self.ws = await websockets.connect(self.url)
                intentos = 0
                try:
                    async for mensaje in self.ws:
                        await self.handler(mensaje)
                finally:
                    if self.ws and self.ws.close_code and self.ws.close_code != 1000:
                        obs_metrics.WS_UNEXPECTED_CLOSURES.inc()
                    await self.close()
            except Exception as exc:  # pragma: no cover - dependiente de la red
                intentos += 1
                obs_metrics.WS_RECONNECTIONS.inc()
                if intentos > self.max_reintentos:
                    log.error("Máximo de reintentos alcanzado", exc_info=exc)
                    raise
                espera = calcular_backoff(intentos)
                obs_metrics.BACKOFF_SECONDS.observe(espera)
                log.warning(
                    "Fallo de conexión WS; reintentando en %.2f s (intento %d)",
                    espera,
                    intentos,
                )
                await asyncio.sleep(espera)