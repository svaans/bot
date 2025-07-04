from __future__ import annotations
from core.notificador import Notificador


class NotificationManager:
    """Encapsula el sistema de notificaciones del bot."""

    def __init__(self, token: str='', chat_id: str='', modo_test: bool=False):
        self._notifier = Notificador(token, chat_id, modo_test)

    def enviar(self, mensaje: str, tipo: str='INFO') ->None:
        self._notifier.enviar(mensaje, tipo)

    async def enviar_async(self, mensaje: str, tipo: str='INFO') ->None:
        await self._notifier.enviar_async(mensaje, tipo)
