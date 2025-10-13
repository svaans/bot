# core/notification_manager.py
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, Optional, Callable, Iterable, cast

import aiohttp

from core.utils.logger import configurar_logger
from core.utils.metrics_compat import Counter, Gauge, Histogram

log = configurar_logger("notification_manager")

# Métricas (no fallan si no hay Prometheus)
NOTIFY_ENQUEUED = Counter("notify_enqueued_total", "Notificaciones encoladas")
NOTIFY_SENT = Counter("notify_sent_total", "Notificaciones enviadas con éxito")
NOTIFY_FAILED = Counter("notify_failed_total", "Notificaciones fallidas")
NOTIFY_RETRY = Counter("notify_retry_total", "Reintentos de notificación")
NOTIFY_QUEUE_SIZE = Gauge("notify_queue_size", "Tamaño de la cola de notificaciones")
NOTIFY_LATENCY = Histogram(
    "notify_latency_seconds", "Latencia de envío de notificaciones",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5)
)

# --- Backend Telegram opcional ------------------------------------------------

class NotificationDeliveryError(RuntimeError):
    """Error específico para fallos al emitir una notificación."""
    
class _TelegramBackend:
    """Backend asíncrono para Telegram empleando ``aiohttp``."""

    def __init__(
        self,
        token: Optional[str],
        chat_id: Optional[str],
        *,
        session: Optional[aiohttp.ClientSession] = None,
        request_timeout: float = 10.0,
        base_url: str = "https://api.telegram.org",
    ) -> None:
        self.token = token
        self.chat_id = chat_id
        self._enabled = bool(token and chat_id)
        self._session = session
        self._request_timeout = request_timeout
        self._base_url = base_url.rstrip("/")

    async def send(self, text: str) -> bool:
        if not self._enabled:
            return True  # no romper el flujo cuando no hay credenciales
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }

        if self._session is not None:
            return await self._dispatch(self._session, payload)

        timeout = aiohttp.ClientTimeout(total=self._request_timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            return await self._dispatch(session, payload)

    async def _dispatch(
        self,
        session: aiohttp.ClientSession,
        payload: dict[str, Any],
    ) -> bool:
        if not self.token:
            raise NotificationDeliveryError("Token de Telegram no configurado")

        url = f"{self._base_url}/bot{self.token}/sendMessage"
        try:
            async with session.post(url, json=payload) as response:
                status = response.status
                try:
                    data = await response.json(content_type=None)
                except Exception as exc:  # pragma: no cover - json inválido
                    body = await response.text()
                    raise NotificationDeliveryError(
                        f"Respuesta no JSON de Telegram (HTTP {status}): {body!r}"
                    ) from exc
        except aiohttp.ClientError as exc:
            raise NotificationDeliveryError(f"Error de red enviando a Telegram: {exc}") from exc
        except asyncio.TimeoutError as exc:
            raise NotificationDeliveryError("Timeout enviando a Telegram") from exc

        if status != 200:
            raise NotificationDeliveryError(
                f"HTTP {status} al enviar mensaje: {data!r}"
            )
        if not data.get("ok", False):
            descripcion = data.get("description", "Respuesta desconocida")
            raise NotificationDeliveryError(
                f"Telegram rechazó la notificación: {descripcion}"
            )
        return True


@dataclass
class Notification:
    mensaje: str
    nivel: str = "INFO"
    meta: Optional[dict] = None


class NotificationManager:
    """Gestor de notificaciones con cola y workers asíncronos + reintentos."""

    def __init__(
        self,
        *,
        token: Optional[str] = None,
        chat_id: Optional[str] = None,
        max_queue: int = 1000,
        workers: int = 2,
        retries: Iterable[float] = (1.0, 2.0, 5.0),
        on_event: Optional[Callable[[str, dict], None]] = None,
    ) -> None:
        self._max_queue = max_queue
        self._q: asyncio.Queue[Notification | object] | None = None
        self._workers: list[asyncio.Task[None]] = []
        self._retries = list(retries)
        self._backend = _TelegramBackend(token, chat_id)
        self._on_event = on_event
        self._started = False
        self._workers_num = max(1, workers)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closing = False
        self._sentinel = object()

    # ---------------- API pública ----------------

    def start(self) -> None:
        if self._started:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:  # pragma: no cover - protección defensiva
            raise RuntimeError(
                "NotificationManager.start() debe ejecutarse dentro de un loop activo"
            ) from exc

        self._loop = loop
        self._q = asyncio.Queue(maxsize=self._max_queue)
        self._workers.clear()
        self._closing = False
        self._started = True
        for i in range(self._workers_num):
            t = loop.create_task(self._worker_loop(), name=f"notify_worker_{i}")
            t.add_done_callback(self._handle_worker_done)
            self._workers.append(t)

    def enviar(self, mensaje: str, nivel: str = "INFO", **meta: Any) -> None:
        """Método síncrono (no bloquea). Encola y retorna inmediatamente."""
        queue = self._ensure_started()
        if queue is None:
            return
        notif = Notification(mensaje=mensaje, nivel=nivel, meta=meta or None)
        try:
            queue.put_nowait(notif)
            NOTIFY_ENQUEUED.inc()
            NOTIFY_QUEUE_SIZE.set(queue.qsize())
        except asyncio.QueueFull:
            self._log_safe("warning", "⚠️ Cola de notificaciones llena; se descarta el mensaje")
            if self._on_event:
                try:
                    self._on_event("notify_drop", {"nivel": nivel})
                except Exception:
                    pass
        except RuntimeError:
            # Loop asociado ya no está disponible (p.ej. durante teardown de tests)
            self._log_safe(
                "debug",
                "Cola de notificaciones cerrada; se descarta el mensaje '%s'",
                mensaje,
            )

    async def enviar_async(self, mensaje: str, nivel: str = "INFO", **meta: Any) -> None:
        """Versión asíncrona: espera a encolar (si hay backpressure)."""
        queue = self._ensure_started()
        if queue is None:
            return
        notif = Notification(mensaje=mensaje, nivel=nivel, meta=meta or None)
        await queue.put(notif)
        NOTIFY_ENQUEUED.inc()
        NOTIFY_QUEUE_SIZE.set(queue.qsize())

    async def iniciar_comando_status(self) -> None:
        """Hook opcional para comandos (p. ej. /status); placeholder."""
        await asyncio.sleep(0)

    async def iniciar_resumen_periodico(self, cada_segundos: int = 600) -> None:
        """Resumen periódico (placeholder)."""
        while True:
            try:
                await asyncio.sleep(cada_segundos)
                # Aquí podrías volcar métricas/estado
            except asyncio.CancelledError:
                break

    async def close(self) -> None:
        """Cancela los workers asíncronos y limpia la cola interna."""
        if not self._started:
            return
        if self._loop and self._loop.is_closed():
            self._reset_state()
            return

        if self._closing:
            await asyncio.gather(*self._workers, return_exceptions=True)
            return
        self._closing = True
        queue = self._q
        if queue is not None:
            for _ in self._workers:
                await queue.put(self._sentinel)
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._reset_state()

    # ---------------- Workers internos ----------------

    async def _worker_loop(self) -> None:
        while True:
            try:
                queue = self._q
                if queue is None:
                    return
                item = await queue.get()
                if item is self._sentinel:
                    queue.task_done()
                    break
                notif = cast(Notification, item)
                NOTIFY_QUEUE_SIZE.set(queue.qsize())
                ok = await self._enviar_con_reintentos(notif)
                if ok:
                    NOTIFY_SENT.inc()
                else:
                    NOTIFY_FAILED.inc()
                queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self._log_safe("exception", "Error en worker de notificaciones")

    async def _enviar_con_reintentos(self, notif: Notification) -> bool:
        lat = NOTIFY_LATENCY.time()
        delays: list[float] = [0.0, *self._retries]
        mensaje = self._formatear(notif)
        try:
            for intento, delay in enumerate(delays, start=1):
                if delay:
                    NOTIFY_RETRY.inc()
                    await asyncio.sleep(delay)
                try:
                    if await self._backend.send(mensaje):
                        return True
                except NotificationDeliveryError as exc:
                    self._log_safe(
                        "warning",
                        "Error enviando notificación (intento %s/%s): %s",
                        intento,
                        len(delays),
                        exc,
                    )
                except Exception:
                    self._log_safe(
                        "exception",
                        "Fallo inesperado en backend de notificaciones",
                    )
            return False
        finally:
            try:
                lat.observe_duration()  # type: ignore[attr-defined]
            except Exception:
                pass

    def _ensure_started(self) -> asyncio.Queue[Notification | object] | None:
        if not self._started:
            try:
                self.start()
            except RuntimeError:
                return None
        if self._loop and self._loop.is_closed():
            self._reset_state()
            try:
                self.start()
            except RuntimeError:
                return None
        return self._q

    def _reset_state(self) -> None:
        self._started = False
        self._closing = False
        self._workers.clear()
        self._loop = None
        self._q = None
        NOTIFY_QUEUE_SIZE.set(0)

    def _handle_worker_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc is not None:
            self._log_safe("error", "Worker de notificaciones finalizado con error: %s", exc)

    @staticmethod
    def _log_safe(level: str, msg: str, *args: Any, **kwargs: Any) -> None:
        try:
            getattr(log, level)(msg, *args, **kwargs)
        except ValueError:
            # Puede ocurrir cuando el loop de pruebas cerró los capturers
            pass

    @staticmethod
    def _formatear(n: Notification) -> str:
        if not n.meta:
            return f"[{n.nivel}] {n.mensaje}"
        try:
            extra = json.dumps(n.meta, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            extra = str(n.meta)
        return f"[{n.nivel}] {n.mensaje} | {extra}"


# Factory desde entorno (como usa tu main)
def crear_notification_manager_desde_env(on_event: Optional[Callable[[str, dict], None]] = None) -> NotificationManager:
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    nm = NotificationManager(
        token=token,
        chat_id=chat_id,
        workers=int(os.getenv("NOTIFY_WORKERS", "2")),
        on_event=on_event,
    )
    try:
        nm.start()
    except RuntimeError as exc:
        mensaje = str(exc)
        if "loop activo" not in mensaje:
            raise
        log.debug(
            "NotificationManager diferido: %s. Se iniciará la primera vez que se use en un loop.",
            mensaje,
        )
    return nm

