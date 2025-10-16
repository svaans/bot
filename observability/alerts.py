"""Herramientas de observabilidad para consolidar alertas estructuradas.

Este módulo centraliza el enrutamiento de eventos críticos del ``EventBus`` y
los transforma en alertas estructuradas para diferentes canales externos
Discord, Slack y métricas Prometheus. Al normalizar los mensajes se facilita la
auditoría y se evita la proliferación de consumidores huérfanos en otras
partes del códigobase.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, List, MutableMapping, Sequence

import httpx

from core.event_bus import EventBus
from core.utils.logger import configurar_logger
from observability.metrics import NOTIFICATIONS_TOTAL

log = configurar_logger("observability.alerts", modo_silencioso=True)


UTC = timezone.utc


def _now_iso() -> str:
    """Retorna la hora actual en formato ISO 8601 (UTC)."""

    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class StructuredAlert:
    """Representa una notificación normalizada para canales externos."""

    event: str
    severity: str
    title: str
    message: str
    timestamp: str = field(default_factory=_now_iso)
    metadata: MutableMapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convierte la alerta en un ``dict`` listo para serializar."""

        data = {
            "event": self.event,
            "severity": self.severity,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp,
        }
        if self.metadata:
            data["metadata"] = dict(self.metadata)
        return data


@dataclass(slots=True)
class AlertDeliveryResult:
    """Resultado de un envío a un canal de notificación."""

    channel: str
    success: bool
    skipped: bool = False
    error: str | None = None


class AlertChannel:
    """Interfaz mínima para canales de notificaciones externas."""

    name: str = "channel"

    async def send(
        self,
        alert: StructuredAlert,
        *,
        session: httpx.AsyncClient,
    ) -> AlertDeliveryResult:
        raise NotImplementedError


class DiscordWebhookChannel(AlertChannel):
    """Publica alertas como *embeds* en un webhook de Discord."""

    name = "discord"

    def __init__(
        self,
        webhook_url: str | None,
        *,
        username: str | None = None,
        avatar_url: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._url = (webhook_url or "").strip()
        self._username = username
        self._avatar_url = avatar_url
        self._timeout = timeout

    async def send(
        self,
        alert: StructuredAlert,
        *,
        session: httpx.AsyncClient,
    ) -> AlertDeliveryResult:
        if not self._url:
            return AlertDeliveryResult(channel=self.name, success=True, skipped=True)

        color = {
            "CRITICAL": 0xE74C3C,
            "ERROR": 0xE67E22,
            "WARNING": 0xF1C40F,
        }.get(alert.severity.upper(), 0x2ECC71)

        embed: dict[str, Any] = {
            "title": alert.title,
            "description": alert.message,
            "timestamp": alert.timestamp,
            "color": color,
        }
        if alert.metadata:
            fields: List[dict[str, Any]] = []
            for key, value in alert.metadata.items():
                fields.append({"name": str(key), "value": str(value), "inline": False})
            if fields:
                embed["fields"] = fields

        payload: dict[str, Any] = {"embeds": [embed]}
        if self._username:
            payload["username"] = self._username
        if self._avatar_url:
            payload["avatar_url"] = self._avatar_url

        try:
            response = await session.post(
                self._url,
                json=payload,
                timeout=self._timeout,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            return AlertDeliveryResult(
                channel=self.name,
                success=False,
                error=str(exc),
            )

        return AlertDeliveryResult(channel=self.name, success=True)


class SlackWebhookChannel(AlertChannel):
    """Envía alertas a un webhook de Slack usando bloques simples."""

    name = "slack"

    def __init__(self, webhook_url: str | None, *, timeout: float = 10.0) -> None:
        self._url = (webhook_url or "").strip()
        self._timeout = timeout

    async def send(
        self,
        alert: StructuredAlert,
        *,
        session: httpx.AsyncClient,
    ) -> AlertDeliveryResult:
        if not self._url:
            return AlertDeliveryResult(channel=self.name, success=True, skipped=True)

        lines = [f"*{alert.title}*", alert.message]
        if alert.metadata:
            metadata_lines = [f"• *{k}*: {v}" for k, v in alert.metadata.items()]
            lines.extend(metadata_lines)
        lines.append(f"`{alert.severity.upper()} | {alert.event}`")

        payload = {
            "text": alert.title,
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(lines)},
                }
            ],
        }

        try:
            response = await session.post(
                self._url,
                json=payload,
                timeout=self._timeout,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            return AlertDeliveryResult(
                channel=self.name,
                success=False,
                error=str(exc),
            )

        return AlertDeliveryResult(channel=self.name, success=True)


class PrometheusAlertRecorder(AlertChannel):
    """Canal que solo registra métricas en Prometheus."""

    name = "prometheus"

    async def send(
        self,
        alert: StructuredAlert,
        *,
        session: httpx.AsyncClient,
    ) -> AlertDeliveryResult:  # pragma: no cover - no usa HTTP
        # No se requiere sesión HTTP; se mantiene la firma homogénea.
        NOTIFICATIONS_TOTAL.labels(channel=self.name, result="success").inc()
        return AlertDeliveryResult(channel=self.name, success=True)


class AlertDispatcher:
    """Consolidación de eventos de alertas sobre un ``EventBus``."""

    def __init__(
        self,
        *,
        bus: EventBus | None = None,
        channels: Sequence[AlertChannel] | None = None,
        discord_webhook: str | None = None,
        slack_webhook: str | None = None,
        enable_prometheus: bool = True,
        http_timeout: float = 10.0,
    ) -> None:
        self._bus = bus
        self._http_timeout = http_timeout
        channel_list: List[AlertChannel] = list(channels or [])
        if discord_webhook is not None:
            channel_list.append(DiscordWebhookChannel(discord_webhook))
        if slack_webhook is not None:
            channel_list.append(SlackWebhookChannel(slack_webhook))
        if enable_prometheus:
            channel_list.append(PrometheusAlertRecorder())
        self._channels = channel_list
        self._session: httpx.AsyncClient | None = None
        if bus:
            self.subscribe(bus)

    @property
    def bus(self) -> EventBus | None:
        """``EventBus`` asociado al dispatcher."""

        return self._bus

    def subscribe(self, bus: EventBus) -> None:
        """Suscribe el dispatcher a los eventos relevantes del ``EventBus``."""

        self._bus = bus
        bus.subscribe("notify", self._on_notify)
        bus.subscribe("risk.cooldown_activated", self._on_cooldown)

    async def aclose(self) -> None:
        """Cierra recursos asociados (sesión HTTP)."""

        if self._session is not None:
            await self._session.aclose()
            self._session = None

    async def _ensure_session(self) -> httpx.AsyncClient:
        if self._session is None:
            self._session = httpx.AsyncClient()
        return self._session

    async def _on_notify(self, payload: Any) -> None:
        message = ""
        severity = "INFO"
        metadata: MutableMapping[str, Any] | None = None
        if isinstance(payload, dict):
            message = str(payload.get("mensaje", "")).strip()
            severity = str(payload.get("tipo", "INFO")).upper()
            metadata = {
                k: v
                for k, v in payload.items()
                if k not in {"mensaje", "tipo"}
            } or None
        else:
            message = str(payload)

        if not message:
            message = "Notificación recibida sin cuerpo"

        alert = StructuredAlert(
            event="notify",
            severity=severity or "INFO",
            title=f"Notificación {severity.upper()}",
            message=message,
            metadata=metadata,
        )
        await self._dispatch(alert)

    async def _on_cooldown(self, payload: Any) -> None:
        metadata: MutableMapping[str, Any] | None = None
        symbol = "desconocido"
        perdida = payload
        cooldown_fin = None
        if isinstance(payload, dict):
            symbol = str(payload.get("symbol", symbol))
            perdida = payload.get("perdida", perdida)
            cooldown_fin = payload.get("cooldown_fin")
            metadata = {
                "symbol": symbol,
                "perdida": perdida,
            }
            if cooldown_fin:
                metadata["cooldown_fin"] = cooldown_fin

        mensaje = (
            "Cooldown global activado tras exceder las pérdidas permitidas."
        )
        alert = StructuredAlert(
            event="risk.cooldown_activated",
            severity="CRITICAL",
            title="Gestor de riesgo en cooldown",
            message=mensaje,
            metadata=metadata,
        )
        await self._dispatch(alert)

    async def _dispatch(self, alert: StructuredAlert) -> None:
        if not self._channels:
            log.debug("No hay canales configurados para alertas: %s", alert)
            return

        session = await self._ensure_session()
        tasks = [self._send(channel, alert, session=session) for channel in self._channels]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, AlertDeliveryResult):
                self._handle_result(result)
            elif isinstance(result, Exception):
                log.error("❌ Error inesperado enviando alerta", exc_info=result)

    async def _send(
        self,
        channel: AlertChannel,
        alert: StructuredAlert,
        *,
        session: httpx.AsyncClient,
    ) -> AlertDeliveryResult:
        try:
            return await channel.send(alert, session=session)
        except Exception as exc:  # pragma: no cover - salvaguarda
            log.error(
                "❌ Canal '%s' lanzó excepción al enviar alerta",
                channel.name,
                exc_info=exc,
            )
            return AlertDeliveryResult(channel=channel.name, success=False, error=str(exc))

    @staticmethod
    def _handle_result(result: AlertDeliveryResult) -> None:
        channel = result.channel
        if result.skipped:
            NOTIFICATIONS_TOTAL.labels(channel=channel, result="skipped").inc()
            return

        if result.success:
            NOTIFICATIONS_TOTAL.labels(channel=channel, result="success").inc()
        else:
            NOTIFICATIONS_TOTAL.labels(channel=channel, result="error").inc()
            if result.error:
                log.warning(
                    "⚠️ Falló envío en canal '%s': %s",
                    channel,
                    result.error,
                )


__all__ = [
    "AlertChannel",
    "AlertDeliveryResult",
    "AlertDispatcher",
    "DiscordWebhookChannel",
    "PrometheusAlertRecorder",
    "SlackWebhookChannel",
    "StructuredAlert",
]