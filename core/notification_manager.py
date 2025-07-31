from __future__ import annotations
from core.notificador import Notificador

from typing import Any
from core.event_bus import EventBus

class NotificationManager:
    """Encapsula el sistema de notificaciones del bot."""

    def __init__(self, token: str = '', chat_id: str = '', modo_test: bool = False, bus: EventBus | None = None):
        self._notifier = Notificador(token, chat_id, modo_test)
        if bus:
            self.subscribe(bus)

    def subscribe(self, bus: EventBus) -> None:
        bus.subscribe('notify', self._on_notify)

    async def _on_notify(self, data: Any) -> None:
        mensaje = data.get('mensaje') or data.get('message')
        tipo = data.get('tipo', 'INFO')
        if mensaje:
            await self.enviar_async(mensaje, tipo)

    def enviar(self, mensaje: str, tipo: str='INFO') ->None:
        self._notifier.enviar(mensaje, tipo)

    async def enviar_async(self, mensaje: str, tipo: str='INFO') ->None:
        await self._notifier.enviar_async(mensaje, tipo)
