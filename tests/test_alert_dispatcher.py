"""Tests para el ``AlertDispatcher`` de observabilidad."""

from __future__ import annotations

import asyncio

import pytest

from core.event_bus import EventBus
from observability.alerts import (
    AlertChannel,
    AlertDeliveryResult,
    AlertDispatcher,
)
from observability.metrics import ALERT_NOTIFY_SUPPRESSED_TOTAL, NOTIFICATIONS_TOTAL


class _DummyChannel(AlertChannel):
    name = "dummy"

    def __init__(self) -> None:
        self.alerts = []

    async def send(self, alert, *, session) -> AlertDeliveryResult:
        self.alerts.append(alert)
        return AlertDeliveryResult(channel=self.name, success=True)


class _FailingChannel(AlertChannel):
    name = "failing"

    async def send(self, alert, *, session) -> AlertDeliveryResult:
        return AlertDeliveryResult(channel=self.name, success=False, error="boom")


async def _wait_until(condition, timeout: float = 0.5) -> None:
    start = asyncio.get_event_loop().time()
    while True:
        if condition():
            return
        if asyncio.get_event_loop().time() - start > timeout:
            raise AssertionError("Timeout esperando a que se cumpla la condición")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_alert_dispatcher_ruta_notify() -> None:
    bus = EventBus()
    dummy = _DummyChannel()
    metric = NOTIFICATIONS_TOTAL.labels(channel="dummy", result="success")
    base_value = metric._value
    dispatcher = AlertDispatcher(
        bus=bus,
        channels=[dummy],
        enable_prometheus=False,
    )

    payload = {"mensaje": "🟢 Compra BTC", "tipo": "warning", "operation_id": "op-1"}
    await bus.publish("notify", payload)
    await _wait_until(lambda: len(dummy.alerts) == 1)

    alert = dummy.alerts[0]
    assert alert.event == "notify"
    assert alert.severity == "WARNING"
    assert alert.metadata == {"operation_id": "op-1"}
    assert "Compra BTC" in alert.message
    assert metric._value == base_value + 1

    await dispatcher.aclose()


@pytest.mark.asyncio
async def test_alert_dispatcher_ruta_cooldown() -> None:
    bus = EventBus()
    dummy = _DummyChannel()
    dispatcher = AlertDispatcher(
        bus=bus,
        channels=[dummy],
        enable_prometheus=False,
    )

    payload = {"symbol": "ETHUSDT", "perdida": 0.12, "cooldown_fin": "2099-01-01"}
    bus.emit("risk.cooldown_activated", payload)
    await _wait_until(lambda: len(dummy.alerts) == 1)

    alert = dummy.alerts[0]
    assert alert.event == "risk.cooldown_activated"
    assert alert.severity == "CRITICAL"
    assert alert.metadata == payload

    await dispatcher.aclose()


@pytest.mark.asyncio
async def test_alert_dispatcher_rate_limit_critical_by_operation_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALERT_NOTIFY_RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("ALERT_NOTIFY_MIN_INTERVAL_CRITICAL_SEC", "3600")
    monkeypatch.setenv("ALERT_NOTIFY_MIN_INTERVAL_ERROR_SEC", "0")

    bus = EventBus()
    dummy = _DummyChannel()
    suppressed = ALERT_NOTIFY_SUPPRESSED_TOTAL.labels(
        severity="CRITICAL",
        key_kind="operation_id",
    )
    base_sup = suppressed._value

    dispatcher = AlertDispatcher(
        bus=bus,
        channels=[dummy],
        enable_prometheus=False,
    )

    p1 = {
        "mensaje": "fallo A",
        "tipo": "CRITICAL",
        "operation_id": "op-dedup",
    }
    await bus.publish("notify", p1)
    await bus.publish("notify", dict(p1, mensaje="fallo B"))
    await _wait_until(lambda: len(dummy.alerts) == 1)

    assert dummy.alerts[0].message == "fallo A"
    assert suppressed._value == base_sup + 1

    await dispatcher.aclose()


@pytest.mark.asyncio
async def test_alert_dispatcher_warning_not_rate_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALERT_NOTIFY_MIN_INTERVAL_CRITICAL_SEC", "3600")

    bus = EventBus()
    dummy = _DummyChannel()
    dispatcher = AlertDispatcher(
        bus=bus,
        channels=[dummy],
        enable_prometheus=False,
    )

    p = {"mensaje": "w", "tipo": "WARNING", "operation_id": "same"}
    await bus.publish("notify", p)
    await bus.publish("notify", dict(p, mensaje="w2"))
    await _wait_until(lambda: len(dummy.alerts) == 2)

    await dispatcher.aclose()


@pytest.mark.asyncio
async def test_alert_dispatcher_registra_fallos() -> None:
    bus = EventBus()
    dummy = _DummyChannel()
    failing = _FailingChannel()
    success_metric = NOTIFICATIONS_TOTAL.labels(channel="dummy", result="success")
    error_metric = NOTIFICATIONS_TOTAL.labels(channel="failing", result="error")
    base_success = success_metric._value
    base_error = error_metric._value

    dispatcher = AlertDispatcher(
        bus=bus,
        channels=[dummy, failing],
        enable_prometheus=False,
    )

    await bus.publish("notify", {"mensaje": "hola"})
    await _wait_until(lambda: len(dummy.alerts) == 1)

    assert success_metric._value == base_success + 1
    assert error_metric._value == base_error + 1

    await dispatcher.aclose()


@pytest.mark.asyncio
async def test_alert_dispatcher_notify_usa_nivel_si_no_hay_tipo() -> None:
    """Compatibilidad con enqueue_notification (nivel) frente a OrderManager (tipo)."""

    bus = EventBus()
    dummy = _DummyChannel()
    dispatcher = AlertDispatcher(
        bus=bus,
        channels=[dummy],
        enable_prometheus=False,
    )
    await bus.publish(
        "notify",
        {"mensaje": "solo nivel", "nivel": "WARNING"},
    )
    await _wait_until(lambda: len(dummy.alerts) == 1)
    assert dummy.alerts[0].severity == "WARNING"

    await dispatcher.aclose()