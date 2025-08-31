from __future__ import annotations
from core.notificador import Notificador

from typing import Any
import asyncio
from prometheus_client import Counter
from core.utils.logger import configurar_logger
from core.event_bus import EventBus

log = configurar_logger('notification_manager', modo_silencioso=True)

NOTIFY_ERRORS_TOTAL = Counter(
    'notify_errors_total',
    'Errores al enviar notificaciones a servicios externos',
)

class NotificationManager:
    """Encapsula el sistema de notificaciones del bot."""

    def __init__(self, token: str = '', chat_id: str = '', modo_test: bool = False, bus: EventBus | None = None):
        self._notifier = Notificador(token, chat_id, modo_test)
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

    def enviar(self, mensaje: str, tipo: str='INFO') ->None:
        self._notifier.enviar(mensaje, tipo)

    async def enviar_async(self, mensaje: str, tipo: str='INFO') ->None:
        await self._notifier.enviar_async(mensaje, tipo)

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
