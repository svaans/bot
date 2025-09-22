from __future__ import annotations
from core.notificador import Notificador

from typing import Any
import asyncio
import os
from uuid import uuid4
from dotenv import load_dotenv
from core.utils.logger import configurar_logger
from core.event_bus import EventBus
from core.utils.metrics_compat import Counter, Metric

log = configurar_logger('notification_manager', modo_silencioso=True)

NOTIFY_ERRORS_TOTAL = Counter(
    'notify_errors_total',
    'Errores al enviar notificaciones a servicios externos',
)


class _Metrics:
    """Inicializa métricas de forma perezosa para evitar fallos tempranos."""

    _notify_sent: Metric | None = None
    _notify_retry: Metric | None = None
    _notify_failed: Metric | None = None

    @property
    def notify_sent(self) -> Metric:
        if self._notify_sent is None:
            self._notify_sent = Counter(
                'notify_sent_total',
                'Notificaciones enviadas correctamente',
            )
        return self._notify_sent

    @property
    def notify_retry(self) -> Metric:
        if self._notify_retry is None:
            self._notify_retry = Counter(
                'notify_retry_total',
                'Reintentos de envío de notificaciones',
            )
        return self._notify_retry

    @property
    def notify_failed(self) -> Metric:
        if self._notify_failed is None:
            self._notify_failed = Counter(
                'notify_failed_total',
                'Notificaciones que agotaron reintentos',
            )
        return self._notify_failed


METRICS = _Metrics()

class NotificationManager:
    """Encapsula el sistema de notificaciones del bot."""

    def __init__(
        self,
        token: str = '',
        chat_id: str = '',
        modo_test: bool = False,
        bus: EventBus | None = None,
        workers: int = 2,
    ):
        self._notifier = Notificador(token, chat_id, modo_test)
        self._q: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        loop = asyncio.get_event_loop()
        self._workers = [loop.create_task(self._worker()) for _ in range(workers)]
        if bus:
            self.subscribe(bus)

    def subscribe(self, bus: EventBus) -> None:
        bus.subscribe('notify', self._on_notify)
        bus.subscribe('orden_simulada_creada', self._on_orden_creada)
        bus.subscribe('orden_simulada_cerrada', self._on_orden_cerrada)

    async def _on_notify(self, data: Any) -> None:
        """Wrapper a prueba de fallos para las notificaciones."""
        mensaje = data.get('mensaje') or data.get('message')
        tipo = data.get('tipo', 'INFO')
        timeout = data.get('timeout', 5)
        if not mensaje:
            return
        try:
            await asyncio.wait_for(self.enviar_async(mensaje, tipo), timeout)
        except Exception as e:  # pragma: no cover - logueo defensivo
            log.error(f'❌ Error enviando notificación: {e}')
            NOTIFY_ERRORS_TOTAL.inc()

    def enviar(self, mensaje: str, tipo: str = 'INFO') -> None:
        """Envía de forma síncrona sin reintentos."""
        self._notifier.enviar(mensaje, tipo)

    async def enviar_async(self, mensaje: str, tipo: str = 'INFO') -> None:
        """Coloca ``mensaje`` en la cola de envío asíncrono."""
        await self._q.put({"mensaje": mensaje, "tipo": tipo, "attempt": 0, "id": uuid4().hex})

    async def _worker(self) -> None:
        schedule = [1, 2, 5]
        while True:
            item = await self._q.get()
            ok, info = await self._notifier.enviar_async(
                item["mensaje"],
                item.get("tipo", "INFO"),
                request_id=item["id"],
            )
            if ok:
                METRICS.notify_sent.inc()
            else:
                if item["attempt"] < 3:
                    item["attempt"] += 1
                    METRICS.notify_retry.inc()
                    await asyncio.sleep(schedule[item["attempt"] - 1])
                    await self._q.put(item)
                else:
                    METRICS.notify_failed.inc()
                    log.error("Notificación DLQ", extra={"id": item["id"], **info})
            self._q.task_done()

    async def _on_orden_creada(self, data: Any) -> None:
        mensaje = (
            f"🟢 Compra {data['symbol']}\nPrecio: {data['precio']:.2f} Cantidad: {data['cantidad']}"
            f"\nSL: {data['sl']:.2f} TP: {data['tp']:.2f}"
        )
        await self._on_notify({'mensaje': mensaje, 'operation_id': data.get('operation_id')})

    async def _on_orden_cerrada(self, data: Any) -> None:
        mensaje = (
            f"📤 Venta {data['symbol']}\nPrecio: {data['precio_cierre']:.2f}"
            f"\nRetorno: {data['retorno'] * 100:.2f}%\nMotivo: {data['motivo']}"
        )
        await self._on_notify({'mensaje': mensaje, 'operation_id': data.get('operation_id')})

    def iniciar_comando_status(self, alert_manager, intervalo: int = 5) -> None:
        """Inicia escucha del comando /status."""

        asyncio.create_task(self._notifier.escuchar_status(alert_manager, intervalo))

    def iniciar_resumen_periodico(self, alert_manager, intervalo: int = 300) -> None:
        """Envía un resumen agrupado de eventos cada ``intervalo`` segundos."""

        async def _resumir():
            while True:
                await asyncio.sleep(intervalo)
                resumen = alert_manager.format_summary(intervalo)
                if resumen != 'Sin eventos':
                    await self._on_notify({'mensaje': resumen})

        asyncio.create_task(_resumir())

    async def close(self) -> None:
            """Cancela los workers asíncronos y limpia la cola interna."""

            if not self._workers:
                return
            for worker in self._workers:
                worker.cancel()
            await asyncio.gather(
                *self._workers,
                return_exceptions=True,
            )
            self._workers.clear()
            while not self._q.empty():
                try:
                    self._q.get_nowait()
                    self._q.task_done()
                except asyncio.QueueEmpty:  # pragma: no cover - carrera defensiva
                    break


def crear_notification_manager_desde_env() -> "NotificationManager":
    """Crea un ``NotificationManager`` leyendo las variables de entorno.

    Las credenciales se cargan desde ``config/claves.env`` si está presente.
    """

    load_dotenv("config/claves.env")
    token = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    modo_test = os.getenv("MODO_TEST_NOTIFICADOR", "false").lower() == "true"
    return NotificationManager(token=token, chat_id=chat_id, modo_test=modo_test)
