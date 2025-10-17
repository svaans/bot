from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any

import pytest

from core.supervisor import Supervisor


@pytest.mark.asyncio
async def test_supervisor_circuit_breaker_blocks_until_manual_reset(monkeypatch) -> None:
    real_sleep = asyncio.sleep

    async def fast_sleep(delay: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr("core.supervisor.asyncio.sleep", fast_sleep)

    events: list[tuple[str, dict[str, Any]]] = []

    def on_event(name: str, data: dict[str, Any]) -> None:
        events.append((name, data))

    supervisor = Supervisor(
        on_event=on_event,
        circuit_breaker_failures=3,
        circuit_breaker_window=60.0,
    )

    attempts = 0

    async def failing_task() -> None:
        nonlocal attempts
        attempts += 1
        if attempts <= 3:
            raise RuntimeError("boom")
        await real_sleep(0)

    supervisor.supervised_task(failing_task, name="fail", delay=0.01)

    for _ in range(10):
        await real_sleep(0)

    assert attempts == 3
    assert supervisor.is_task_circuit_open("fail")
    event_names = Counter(name for name, _ in events)
    assert event_names["task_circuit_open"] == 1

    attempts_before = attempts
    await supervisor.restart_task("fail")
    for _ in range(3):
        await real_sleep(0)
    assert attempts == attempts_before
    event_names = Counter(name for name, _ in events)
    assert event_names["task_restart_blocked"] >= 1

    closed = supervisor.reset_task_circuit("fail")
    assert closed is True
    await supervisor.restart_task("fail")
    for _ in range(5):
        await real_sleep(0)

    assert attempts == 4
    assert not supervisor.is_task_circuit_open("fail")
    event_names = Counter(name for name, _ in events)
    assert event_names["task_circuit_closed"] >= 1

    await supervisor.shutdown()


@pytest.mark.asyncio
async def test_supervisor_circuit_breaker_respects_reset_check(monkeypatch) -> None:
    real_sleep = asyncio.sleep

    async def fast_sleep(delay: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr("core.supervisor.asyncio.sleep", fast_sleep)

    events: list[tuple[str, dict[str, Any]]] = []

    def on_event(name: str, data: dict[str, Any]) -> None:
        events.append((name, data))

    allow_reset = False

    def reset_check(task_name: str) -> bool:
        return allow_reset

    supervisor = Supervisor(
        on_event=on_event,
        circuit_breaker_failures=2,
        circuit_breaker_window=30.0,
        circuit_breaker_check=reset_check,
    )

    attempts = 0

    async def failing_task() -> None:
        nonlocal attempts
        attempts += 1
        if attempts <= 2:
            raise RuntimeError("boom")
        await real_sleep(0)

    supervisor.supervised_task(failing_task, name="fail-check", delay=0.01)

    for _ in range(10):
        await real_sleep(0)

    assert attempts == 2
    assert supervisor.is_task_circuit_open("fail-check")

    await supervisor.restart_task("fail-check")
    for _ in range(3):
        await real_sleep(0)
    assert attempts == 2

    allow_reset = True
    await supervisor.restart_task("fail-check")
    for _ in range(5):
        await real_sleep(0)

    assert attempts == 3
    assert not supervisor.is_task_circuit_open("fail-check")

    event_names = Counter(name for name, _ in events)
    assert event_names["task_circuit_closed"] >= 1

    await supervisor.shutdown()