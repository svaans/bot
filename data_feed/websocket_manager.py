"""Gestor simple de conexiones WebSocket con reconexión automática."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

import websockets

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

    async def conectar(self) -> None:
        """Inicia la conexión y maneja reconexiones automáticas."""
        intentos = 0
        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    intentos = 0
                    async for mensaje in ws:
                        await self.handler(mensaje)
            except Exception as exc:  # pragma: no cover - dependiente de la red
                intentos += 1
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