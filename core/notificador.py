import os
import requests
from core.logger import configurar_logger

log = configurar_logger("notificador")

class Notificador:
    """Envía mensajes de texto mediante la API de Telegram."""

    def __init__(self, token: str | None, chat_id: str | None) -> None:
        self.token = token or ""
        self.chat_id = chat_id or ""

    def enviar(self, mensaje: str) -> None:
        if not self.token or not self.chat_id:
            return
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            resp = requests.post(url, json={"chat_id": self.chat_id, "text": mensaje})
            if resp.status_code != 200:
                log.warning(f"⚠️ Error al enviar notificación: {resp.text}")
        except requests.RequestException as e:
            log.warning(f"⚠️ No se pudo enviar notificación: {e}")
            