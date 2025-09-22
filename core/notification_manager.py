# core/notification_manager.py
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, Optional, Callable, Iterable

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

class _TelegramBackend:
    """Backend mínimo para Telegram; ignora silenciosamente si faltan credenciales."""
    def __init__(self, token: Optional[str], chat_id: Optional[str]) -> None:
        self.token = token
        self.chat_id = chat_id
        self._enabled = bool(token and chat_id)

    async def send(self, text: str) -> bool:
        if not self._enabled:
            return True  # no romper el flujo cuando no hay credenciales
        # Evita dependencias en requests/aiohttp aquí; deja el hook para que lo inyectes si quieres.
        # Puedes cambiar esta parte por tu cliente real de Telegram si lo tienes en el proyecto.
        try:
            # Placeholder de éxito inmediato (hookéalo a tu implementacion real)
            await asyncio.sleep(0)
            return True
        except Exception:
            return False


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
        self._q: asyncio.Queue[Notification] = asyncio.Queue(maxsize=max_queue)
        self._workers: list[asyncio.Task] = []
        self._retries = list(retries)
        self._backend = _TelegramBackend(token, chat_id)
        self._on_event = on_event
        self._started = False
        self._workers_num = max(1, workers)

    # ---------------- API pública ----------------

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        for i in range(self._workers_num):
            t = asyncio.create_task(self._worker_loop(), name=f"notify_worker_{i}")
            self._workers.append(t)

    def enviar(self, mensaje: str, nivel: str = "INFO", **meta: Any) -> None:
        """Método síncrono (no bloquea). Encola y retorna inmediatamente."""
        if not self._started:
            self.start()
        notif = Notification(mensaje=mensaje, nivel=nivel, meta=meta or None)
        try:
            self._q.put_nowait(notif)
            NOTIFY_ENQUEUED.inc()
            NOTIFY_QUEUE_SIZE.set(self._q.qsize())
        except asyncio.QueueFull:
            log.warning("⚠️ Cola de notificaciones llena; se descarta el mensaje")
            if self._on_event:
                try:
                    self._on_event("notify_drop", {"nivel": nivel})
                except Exception:
                    pass

    async def enviar_async(self, mensaje: str, nivel: str = "INFO", **meta: Any) -> None:
        """Versión asíncrona: espera a encolar (si hay backpressure)."""
        if not self._started:
            self.start()
        notif = Notification(mensaje=mensaje, nivel=nivel, meta=meta or None)
        await self._q.put(notif)
        NOTIFY_ENQUEUED.inc()
        NOTIFY_QUEUE_SIZE.set(self._q.qsize())

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
        if not self._workers:
            return
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        # Vacía la cola
        while not self._q.empty():
            try:
                self._q.get_nowait()
                self._q.task_done()
            except asyncio.QueueEmpty:
                break

    # ---------------- Workers internos ----------------

    async def _worker_loop(self) -> None:
        while True:
            try:
                notif = await self._q.get()
                NOTIFY_QUEUE_SIZE.set(self._q.qsize())
                ok = await self._enviar_con_reintentos(notif)
                if ok:
                    NOTIFY_SENT.inc()
                else:
                    NOTIFY_FAILED.inc()
                self._q.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Error en worker de notificaciones")

    async def _enviar_con_reintentos(self, notif: Notification) -> bool:
        lat = NOTIFY_LATENCY.time()
        try:
            if await self._backend.send(self._formatear(notif)):
                return True
            for delay in self._retries:
                NOTIFY_RETRY.inc()
                await asyncio.sleep(delay)
                if await self._backend.send(self._formatear(notif)):
                    return True
            return False
        finally:
            try:
                lat.observe_duration()  # type: ignore[attr-defined]
            except Exception:
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
    nm.start()
    return nm

