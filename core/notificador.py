import os
import requests
import asyncio
from core.utils.utils import configurar_logger
from dotenv import load_dotenv
ESCAPE_CHARS = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-',
    '=', '|', '{', '}', '.', '!']


def escape_markdown(text: str) ->str:
    """Escapa caracteres problemáticos para Markdown/MarkdownV2 de Telegram."""
    for char in ESCAPE_CHARS:
        text = text.replace(char, f'\\{char}')
    return text


log = configurar_logger('notificador')


class Notificador:

    def __init__(self, token: str='', chat_id: str='', modo_test: bool=
        False, parse_mode: (str | None)='Markdown'):
        self.token = token
        self.chat_id = chat_id
        self.modo_test = modo_test
        self.parse_mode = parse_mode if parse_mode else None
        if not self.token or not self.chat_id:
            log.warning(
                '❌ Token o Chat ID no configurados. Notificaciones deshabilitadas.'
                )

    def enviar(self, mensaje: str, tipo: str='INFO') ->None:
        if not self.token or not self.chat_id:
            return
        mensaje = f'[{tipo.upper()}] {mensaje}'
        if self.parse_mode and self.parse_mode.startswith('Markdown'):
            mensaje = escape_markdown(mensaje)
        if self.modo_test:
            print(f'🧪 [TEST] {mensaje}')
            return
        url = f'https://api.telegram.org/bot{self.token}/sendMessage'
        payload = {'chat_id': self.chat_id, 'text': mensaje}
        if self.parse_mode:
            payload['parse_mode'] = self.parse_mode
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code != 200:
                log.warning(f'⚠️ Error enviando notificación: {response.text}')
                if self.parse_mode:
                    payload.pop('parse_mode', None)
                    retry = requests.post(url, json=payload, timeout=10)
                    if retry.status_code != 200:
                        log.warning(
                            f'⚠️ Reintento sin parse_mode: {retry.text}')
        except Exception as e:
            log.error(f'❌ Excepción al enviar notificación: {e}')
            if self.parse_mode:
                payload.pop('parse_mode', None)
                try:
                    retry = requests.post(url, json=payload, timeout=10)
                    if retry.status_code != 200:
                        log.warning(
                            f'⚠️ Reintento tras excepción: {retry.text}')
                except Exception as e2:
                    log.error(f'❌ Error en reintento de notificación: {e2}')

    async def enviar_async(self, mensaje: str, tipo: str='INFO') ->None:
        """Envía la notificación sin bloquear el loop principal."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.enviar, mensaje, tipo)


def crear_notificador_desde_env() ->Notificador:
    load_dotenv('config/claves.env')
    token = os.getenv('TELEGRAM_TOKEN', '')
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
    modo_test = os.getenv('MODO_TEST_NOTIFICADOR', 'false').lower() == 'true'
    parse_mode_env = os.getenv('TELEGRAM_PARSE_MODE', 'Markdown').strip()
    parse_mode = parse_mode_env if parse_mode_env else None
    return Notificador(token=token, chat_id=chat_id, modo_test=modo_test,
        parse_mode=parse_mode)
