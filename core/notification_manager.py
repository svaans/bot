# core/notification_manager.py
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Protocol, cast
from typing import Literal

import aiohttp

from core.utils.logger import configurar_logger
from core.utils.metrics_compat import Counter, Gauge, Histogram
from core.notificador import escape_markdown

log = configurar_logger("notification_manager")

# Métricas (no fallan si no hay Prometheus)
NOTIFY_ENQUEUED = Counter("notify_enqueued_total", "Notificaciones encoladas")
NOTIFY_SENT = Counter(
    "notify_sent_total", "Notificaciones enviadas con éxito", ("channel",)
)
NOTIFY_FAILED = Counter(
    "notify_failed_total", "Notificaciones fallidas", ("channel",)
)
NOTIFY_RETRY = Counter(
    "notify_retry_total", "Reintentos de notificación", ("channel",)
)
NOTIFY_SKIPPED = Counter(
    "notify_skipped_total", "Notificaciones omitidas", ("channel",)
)
NOTIFY_QUEUE_SIZE = Gauge("notify_queue_size", "Tamaño de la cola de notificaciones")
NOTIFY_LATENCY = Histogram(
    "notify_latency_seconds", "Latencia de envío de notificaciones",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5)
)

# --- Backend Telegram opcional ------------------------------------------------

class NotificationDeliveryError(RuntimeError):
    """Error específico para fallos al emitir una notificación."""


@dataclass(frozen=True)
class DeliveryReport:
    """Resultado homogéneo que describe el estado de la entrega."""

    status: Literal["success", "skipped"]
    details: dict[str, Any] | None = None


class NotificationBackend(Protocol):
    async def send(self, text: str, *, meta: dict[str, Any] | None = None) -> DeliveryReport:
        """Envía ``text`` y retorna el resultado normalizado."""
    
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
        parse_mode: Optional[str] = "Markdown",
        escape_markdown_text: bool = True,
    ) -> None:
        self.token = token
        self.chat_id = chat_id
        self._enabled = bool(token and chat_id)
        self._session = session
        self._request_timeout = request_timeout
        self._base_url = base_url.rstrip("/")
        self._parse_mode = parse_mode if parse_mode else None
        self._escape_markdown_text = escape_markdown_text

    async def send(self, text: str, *, meta: dict[str, Any] | None = None) -> DeliveryReport:
        if not self._enabled:
            return DeliveryReport(status="skipped", details={"reason": "telegram_disabled"})

        mensaje = text
        if self._parse_mode and self._parse_mode.startswith("Markdown"):
            if self._escape_markdown_text:
                mensaje = escape_markdown(mensaje)

        payload: dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": mensaje,
            "disable_web_page_preview": True,
        }
        if self._parse_mode:
            payload["parse_mode"] = self._parse_mode
        telegram_meta = (meta or {}).get("telegram") if meta else None
        if isinstance(telegram_meta, dict):
            payload.update(telegram_meta)

        if self._session is not None:
            return await self._dispatch(self._session, payload)

        timeout = aiohttp.ClientTimeout(total=self._request_timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            return await self._dispatch(session, payload)

    async def _dispatch(
        self,
        session: aiohttp.ClientSession,
        payload: dict[str, Any],
    ) -> DeliveryReport:
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
        return DeliveryReport(status="success", details={"response": data})


@dataclass
class Notification:
    mensaje: str
    nivel: str = "INFO"
    meta: Optional[dict] = None
    canales: tuple[str, ...] | None = None
    dedup_key: str | None = None


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
        backends: Optional[dict[str, NotificationBackend]] = None,
        dedup_ttl: float | None = 300.0,
    ) -> None:
        self._max_queue = max_queue
        self._q: asyncio.Queue[Notification | object] | None = None
        self._workers: list[asyncio.Task[None]] = []
        self._retries = list(retries)
        if backends is not None:
            self._backends: dict[str, NotificationBackend] = dict(backends)
        else:
            self._backends = {"telegram": _TelegramBackend(token, chat_id)}
        self._on_event = on_event
        self._started = False
        self._workers_num = max(1, workers)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closing = False
        self._sentinel = object()
        if dedup_ttl is None:
            self._dedup_ttl = 0.0
        else:
            self._dedup_ttl = max(0.0, float(dedup_ttl))
        self._dedup_cache: dict[str, float] = {}

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
        notif = self._build_notification(mensaje, nivel, meta)
        if notif is None:
            return
        try:
            queue.put_nowait(notif)
            NOTIFY_ENQUEUED.inc()
            NOTIFY_QUEUE_SIZE.set(queue.qsize())
        except asyncio.QueueFull:
            self._log_safe("warning", "⚠️ Cola de notificaciones llena; se descarta el mensaje")
            NOTIFY_FAILED.labels(channel="internal").inc()
            NOTIFY_SKIPPED.labels(channel="internal").inc()
            if self._on_event:
                try:
                    self._on_event(
                        "notify_drop",
                        {"nivel": nivel, "reason": "queue_full", "mensaje": mensaje},
                    )
                except Exception:
                    pass
        except RuntimeError:
            # Loop asociado ya no está disponible (p.ej. durante teardown de tests)
            self._log_safe(
                "debug",
                "Cola de notificaciones cerrada; se descarta el mensaje '%s'",
                mensaje,
            )
            NOTIFY_SKIPPED.labels(channel="internal").inc()

    async def enviar_async(self, mensaje: str, nivel: str = "INFO", **meta: Any) -> None:
        """Versión asíncrona: espera a encolar (si hay backpressure)."""
        queue = self._ensure_started()
        if queue is None:
            return
        notif = self._build_notification(mensaje, nivel, meta)
        if notif is None:
            return
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
                if not ok:
                    NOTIFY_FAILED.labels(channel="internal").inc()
                queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self._log_safe("exception", "Error en worker de notificaciones")

    async def _enviar_con_reintentos(self, notif: Notification) -> bool:
        lat = NOTIFY_LATENCY.time()
        canales = notif.canales or tuple(self._backends.keys())
        mensaje = self._formatear(notif)
        meta = notif.meta or None
        overall_ok = True
        try:
            for canal in canales:
                backend = self._backends.get(canal)
                if backend is None:
                    self._log_safe("warning", "Canal de notificación '%s' no configurado", canal)
                    NOTIFY_FAILED.labels(channel=canal).inc()
                    overall_ok = False
                    continue
                delivered = await self._emitir_con_reintentos_backend(
                    canal, backend, mensaje, meta
                )
                if not delivered:
                    overall_ok = False
            return overall_ok
        finally:
            try:
                lat.observe_duration()  # type: ignore[attr-defined]
            except Exception:
                pass


    async def _emitir_con_reintentos_backend(
        self,
        canal: str,
        backend: NotificationBackend,
        mensaje: str,
        meta: dict[str, Any] | None,
    ) -> bool:
        delays: list[float] = [0.0, *self._retries]
        for intento, delay in enumerate(delays, start=1):
            if delay > 0:
                NOTIFY_RETRY.labels(channel=canal).inc()
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    raise
            try:
                report = await backend.send(mensaje, meta=meta)
            except NotificationDeliveryError as exc:
                self._log_safe(
                    "warning",
                    "Error enviando notificación en canal %s (intento %s/%s): %s",
                    canal,
                    intento,
                    len(delays),
                    exc,
                )
            except Exception:
                self._log_safe(
                    "exception",
                    "Fallo inesperado en backend '%s' de notificaciones",
                    canal,
                )
            else:
                if report.status == "success":
                    NOTIFY_SENT.labels(channel=canal).inc()
                    self._emit_event(
                        "notify_success",
                        canal,
                        {"meta": meta, "details": report.details},
                    )
                    return True
                if report.status == "skipped":
                    NOTIFY_SKIPPED.labels(channel=canal).inc()
                    self._emit_event(
                        "notify_skipped",
                        canal,
                        {"meta": meta, "details": report.details},
                    )
                    return True
                self._log_safe(
                    "warning",
                    "Resultado desconocido del backend '%s': %s",
                    canal,
                    report.status,
                )
        NOTIFY_FAILED.labels(channel=canal).inc()
        self._emit_event(
            "notify_failed",
            canal,
            {"meta": meta, "reason": "max_retries"},
        )
        return False
    

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
        self._dedup_cache.clear()

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
    
    def _emit_event(self, name: str, channel: str, payload: dict[str, Any] | None = None) -> None:
        if not self._on_event:
            return
        data = {"channel": channel}
        if payload:
            data.update(payload)
        try:
            self._on_event(name, data)
        except Exception:
            pass

    def _build_notification(
        self, mensaje: str, nivel: str, meta: dict[str, Any] | None
    ) -> Notification | None:
        payload = dict(meta or {})
        canales_raw = payload.pop("channels", payload.pop("canales", None))
        if canales_raw is None:
            canales = None
        elif isinstance(canales_raw, str):
            canales = (canales_raw,)
        else:
            canales = tuple(str(canal) for canal in canales_raw)

        dedup_key = payload.pop("dedup_key", None) or payload.pop("idempotency_key", None)
        if dedup_key and not self._register_dedup_key(dedup_key):
            self._log_safe(
                "debug",
                "Notificación duplicada ignorada (key=%s, nivel=%s)",
                dedup_key,
                nivel,
            )
            NOTIFY_SKIPPED.labels(channel="dedup").inc()
            self._emit_event(
                "notify_skipped",
                "dedup",
                {"reason": "duplicate", "dedup_key": dedup_key},
            )
            return None

        return Notification(
            mensaje=mensaje,
            nivel=nivel,
            meta=payload or None,
            canales=canales,
            dedup_key=dedup_key,
        )

    def _register_dedup_key(self, key: str) -> bool:
        ttl = self._dedup_ttl
        if ttl <= 0:
            return True
        now = time.monotonic()
        self._purge_dedup(now)
        expiry = self._dedup_cache.get(key)
        if expiry is not None and expiry > now:
            return False
        self._dedup_cache[key] = now + ttl
        return True

    def _purge_dedup(self, now: float) -> None:
        ttl = self._dedup_ttl
        if ttl <= 0:
            return
        caducados = [clave for clave, exp in self._dedup_cache.items() if exp <= now]
        for clave in caducados:
            self._dedup_cache.pop(clave, None)


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

