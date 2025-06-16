import os
import requests
import asyncio
from core.logger import configurar_logger
from dotenv import load_dotenv

log = configurar_logger("notificador")

class Notificador:
    def __init__(self, token: str = "", chat_id: str = "", modo_test: bool = False, parse_mode: str = "Markdown"):
        self.token = token
        self.chat_id = chat_id
        self.modo_test = modo_test
        self.parse_mode = parse_mode

        if not self.token or not self.chat_id:
            log.warning("❌ Token o Chat ID no configurados. Notificaciones deshabilitadas.")

    def enviar(self, mensaje: str, tipo: str = "INFO") -> None:
        if not self.token or not self.chat_id:
            return

        mensaje = f"[{tipo.upper()}] {mensaje}"

        if self.modo_test:
            print(f"🧪 [TEST] {mensaje}")
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": mensaje,
            "parse_mode": self.parse_mode
        }

        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code != 200:
                log.warning(f"⚠️ Error enviando notificación: {response.text}")
        except Exception as e:
            log.error(f"❌ Excepción al enviar notificación: {e}")

    async def enviar_async(self, mensaje: str, tipo: str = "INFO") -> None:
        """Envía la notificación sin bloquear el loop principal."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.enviar, mensaje, tipo)

def crear_notificador_desde_env() -> Notificador:
    load_dotenv("config/claves.env")
    token = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    modo_test = os.getenv("MODO_TEST_NOTIFICADOR", "false").lower() == "true"
    parse_mode = os.getenv("TELEGRAM_PARSE_MODE", "Markdown")
    return Notificador(token=token, chat_id=chat_id, modo_test=modo_test, parse_mode=parse_mode)
