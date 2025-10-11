import asyncio
import os
import time
from typing import Any, Tuple
import httpx
from core.utils.utils import configurar_logger
from observability.metrics import NOTIFICATIONS_RETRY, NOTIFICATIONS_TOTAL
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

    def __init__(
        self,
        token: str = '',
        chat_id: str = '',
        modo_test: bool = False,
        parse_mode: str | None = 'Markdown',
        *,
        timeout: float = 10.0,
        max_reintentos: int = 3,
        backoff_inicial: float = 0.5,
    ):
        self.token = token
        self.chat_id = chat_id
        self.modo_test = modo_test
        self.parse_mode = parse_mode if parse_mode else None
        self._timeout = max(timeout, 0.1)
        self._max_reintentos = max(0, int(max_reintentos))
        self._backoff_inicial = max(0.0, backoff_inicial)
        self._async_client: httpx.AsyncClient | None = None
        if not self.token or not self.chat_id:
            log.warning(
                '❌ Token o Chat ID no configurados. Notificaciones deshabilitadas.'
            )

    def enviar(self, mensaje: str, tipo: str = 'INFO', request_id: str | None = None) -> Tuple[bool, dict[str, Any]]:
        if not self.token or not self.chat_id:
            NOTIFICATIONS_TOTAL.labels(channel='telegram', result='skipped').inc()
            return False, {"status_code": None, "body": "credenciales faltantes"}
        mensaje = f'[{tipo.upper()}] {mensaje}'
        if self.parse_mode and self.parse_mode.startswith('Markdown'):
            mensaje = escape_markdown(mensaje)
        if self.modo_test:
            print(f'🧪 [TEST] {mensaje}')
            NOTIFICATIONS_TOTAL.labels(channel='telegram', result='success').inc()
            return True, {"status_code": 200, "body": "test"}
        url = f'https://api.telegram.org/bot{self.token}/sendMessage'
        payload = {'chat_id': self.chat_id, 'text': mensaje}
        if self.parse_mode:
            payload['parse_mode'] = self.parse_mode
        backoff = self._backoff_inicial or 0.0
        ultimo_error: Exception | None = None
        with httpx.Client(timeout=self._timeout) as client:
            for intento in range(self._max_reintentos + 1):
                try:
                    response = client.post(url, json=payload)
                    info = {"status_code": response.status_code, "body": response.text}
                    try:
                        ok = response.status_code == 200 and response.json().get('ok', False)
                    except Exception as exc:  # pragma: no cover - defensivo
                        ultimo_error = exc
                        ok = False
                    if ok:
                        NOTIFICATIONS_TOTAL.labels(channel='telegram', result='success').inc()
                        return True, info
                    log.error(
                        '⚠️ Error enviando notificación',
                        extra={"id": request_id, **info},
                    )
                    if intento >= self._max_reintentos:
                        NOTIFICATIONS_TOTAL.labels(channel='telegram', result='error').inc()
                        return False, info
                    NOTIFICATIONS_RETRY.labels(channel='telegram').inc()
                    if backoff > 0:
                        time.sleep(backoff)
                        backoff *= 2
                except httpx.HTTPError as exc:
                    ultimo_error = exc
                    log.warning(
                        '📡 Reintento %s/%s al enviar notificación: %s',
                        intento + 1,
                        self._max_reintentos,
                        exc,
                        extra={"id": request_id},
                    )
                    if intento >= self._max_reintentos:
                        break
                    NOTIFICATIONS_RETRY.labels(channel='telegram').inc()
                    if backoff > 0:
                        time.sleep(backoff)
                        backoff *= 2
        NOTIFICATIONS_TOTAL.labels(channel='telegram', result='exception').inc()
        log.error(
            '❌ Excepción al enviar notificación: %s',
            ultimo_error,
            extra={"id": request_id},
        )
        return False, {"exception": str(ultimo_error) if ultimo_error else "error desconocido"}

    async def enviar_async(
        self, mensaje: str, tipo: str = 'INFO', request_id: str | None = None
    ) -> Tuple[bool, dict[str, Any]]:
        """Envía la notificación sin bloquear el loop principal."""

        if not self.token or not self.chat_id:
            NOTIFICATIONS_TOTAL.labels(channel='telegram', result='skipped').inc()
            return False, {"status_code": None, "body": "credenciales faltantes"}

        mensaje = f'[{tipo.upper()}] {mensaje}'
        if self.parse_mode and self.parse_mode.startswith('Markdown'):
            mensaje = escape_markdown(mensaje)
        if self.modo_test:
            print(f'🧪 [TEST] {mensaje}')
            NOTIFICATIONS_TOTAL.labels(channel='telegram', result='success').inc()
            return True, {"status_code": 200, "body": "test"}

        payload = {'chat_id': self.chat_id, 'text': mensaje}
        if self.parse_mode:
            payload['parse_mode'] = self.parse_mode

        client = await self._ensure_async_client()
        backoff = self._backoff_inicial or 0.0
        ultimo_error: Exception | None = None
        for intento in range(self._max_reintentos + 1):
            try:
                response = await client.post(
                    f'https://api.telegram.org/bot{self.token}/sendMessage',
                    json=payload,
                )
                info = {"status_code": response.status_code, "body": response.text}
                try:
                    ok = response.status_code == 200 and response.json().get('ok', False)
                except Exception as exc:  # pragma: no cover - defensivo
                    ultimo_error = exc
                    ok = False
                if ok:
                    NOTIFICATIONS_TOTAL.labels(channel='telegram', result='success').inc()
                    return True, info
                log.error(
                    '⚠️ Error enviando notificación',
                    extra={"id": request_id, **info},
                )
                if intento >= self._max_reintentos:
                    NOTIFICATIONS_TOTAL.labels(channel='telegram', result='error').inc()
                    return False, info
                NOTIFICATIONS_RETRY.labels(channel='telegram').inc()
                if backoff > 0:
                    await asyncio.sleep(backoff)
                    backoff *= 2
            except httpx.HTTPError as exc:
                ultimo_error = exc
                log.warning(
                    '📡 Reintento %s/%s al enviar notificación: %s',
                    intento + 1,
                    self._max_reintentos,
                    exc,
                    extra={"id": request_id},
                )
                if intento >= self._max_reintentos:
                    break
                NOTIFICATIONS_RETRY.labels(channel='telegram').inc()
                if backoff > 0:
                    await asyncio.sleep(backoff)
                    backoff *= 2

        NOTIFICATIONS_TOTAL.labels(channel='telegram', result='exception').inc()
        log.error(
            '❌ Excepción al enviar notificación: %s',
            ultimo_error,
            extra={"id": request_id},
        )
        return False, {"exception": str(ultimo_error) if ultimo_error else "error desconocido"}

    async def escuchar_status(self, alert_manager, intervalo: int = 5) -> None:
        """Escucha el comando /status y responde con un resumen."""

        if not self.token or not self.chat_id:
            return
        offset = None
        url = f'https://api.telegram.org/bot{self.token}/getUpdates'
        client = await self._ensure_async_client()
        while True:
            try:
                params = {'timeout': 0}
                if offset is not None:
                    params['offset'] = offset
                resp = await client.get(url, params=params)
                datos = resp.json()
                for upd in datos.get('result', []):
                    offset = upd['update_id'] + 1
                    msg = upd.get('message') or {}
                    chat_id = str(msg.get('chat', {}).get('id'))
                    texto = (msg.get('text') or '').strip()
                    if chat_id == str(self.chat_id) and texto == '/status':
                        resumen = alert_manager.format_summary(900)
                        await self.enviar_async(resumen, 'INFO')
            except Exception as e:  # pragma: no cover - defensivo
                log.error(f'❌ Error escuchando comandos: {e}')
            await asyncio.sleep(intervalo)

    async def aclose(self) -> None:
        """Cierra el cliente HTTP reutilizado por el notificador."""

        if self._async_client is not None:
            await self._async_client.aclose()
            self._async_client = None

    async def _ensure_async_client(self) -> httpx.AsyncClient:
        if self._async_client is None:
            self._async_client = httpx.AsyncClient(timeout=self._timeout)
        return self._async_client

            
def crear_notificador_desde_env() ->Notificador:
    load_dotenv('config/claves.env')
    token = os.getenv('TELEGRAM_TOKEN', '')
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
    modo_test = os.getenv('MODO_TEST_NOTIFICADOR', 'false').lower() == 'true'
    parse_mode_env = os.getenv('TELEGRAM_PARSE_MODE', 'Markdown').strip()
    parse_mode = parse_mode_env if parse_mode_env else None
    return Notificador(token=token, chat_id=chat_id, modo_test=modo_test,
        parse_mode=parse_mode)
