from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from core.metrics import TASK_TIMEOUT_SECONDS
from core.supervisor import Supervisor
import core.supervisor as supervisor_module


@pytest.mark.asyncio
async def test_supervisor_beat_updates_expected_interval() -> None:
    supervisor = Supervisor()
    try:
        supervisor.beat("producer", expected_interval=12.5)
        assert supervisor.task_expected_interval["producer"] == pytest.approx(12.5)

        supervisor.beat("producer", expected_interval=0)
        assert "producer" not in supervisor.task_expected_interval
    finally:
        await supervisor.shutdown()


@pytest.mark.asyncio
async def test_watchdog_records_timeout_metric() -> None:
    if hasattr(TASK_TIMEOUT_SECONDS, "clear"):
        TASK_TIMEOUT_SECONDS.clear()
    else:  # pragma: no cover - compatibilidad con clientes antiguos
        metrics = getattr(TASK_TIMEOUT_SECONDS, "_metrics", None)
        if isinstance(metrics, dict):
            metrics.clear()
    supervisor = Supervisor(watchdog_timeout=20, watchdog_check_interval=1)
    try:
        now = supervisor._now()
        supervisor.task_heartbeat["worker"] = now - timedelta(seconds=40)
        supervisor.task_intervals["worker"].extend([9, 10, 10, 11, 12])
        supervisor.set_task_expected_interval("worker", 15)

        await supervisor._watchdog_once(20)

        metric = TASK_TIMEOUT_SECONDS.labels("worker")
        assert metric._value == pytest.approx(90.0)
    finally:
        await supervisor.shutdown()


@pytest.mark.asyncio
async def test_restart_task_scales_cooldown(monkeypatch: pytest.MonkeyPatch) -> None:
    supervisor = Supervisor(cooldown_sec=7, cooldown_max_sec=20)
    try:
        current_time = {"value": datetime(2024, 1, 1, tzinfo=timezone.utc)}

        def fake_now() -> datetime:
            return current_time["value"]

        supervisor._now = fake_now  # type: ignore[assignment]
        supervisor.task_factories["worker"] = lambda: None

        orig_sleep = asyncio.sleep

        async def fake_sleep(delay: float, *args, **kwargs) -> None:
            await orig_sleep(0)

        monkeypatch.setattr(supervisor_module.asyncio, "sleep", fake_sleep)

        def fake_supervised_task(*args, **kwargs):
            return None

        supervisor.supervised_task = fake_supervised_task  # type: ignore[assignment]

        previous_cooldown = supervisor.task_cooldown.get("worker")
        await supervisor.restart_task("worker")
        for _ in range(5):
            await orig_sleep(0)
            current_cooldown = supervisor.task_cooldown.get("worker")
            if current_cooldown is not None and current_cooldown != previous_cooldown:
                break
        assigned_time = current_time["value"]
        cooldown_1 = (
            supervisor.task_cooldown["worker"] - assigned_time
        ).total_seconds()
        assert cooldown_1 == pytest.approx(7)

        current_time["value"] = current_time["value"] + timedelta(seconds=1)
        previous_cooldown = supervisor.task_cooldown.get("worker")
        await supervisor.restart_task("worker")
        for _ in range(5):
            await orig_sleep(0)
            current_cooldown = supervisor.task_cooldown.get("worker")
            if current_cooldown is not None and current_cooldown != previous_cooldown:
                break
        assigned_time = current_time["value"]
        cooldown_2 = (
            supervisor.task_cooldown["worker"] - assigned_time
        ).total_seconds()
        assert cooldown_2 == pytest.approx(14)

        current_time["value"] = current_time["value"] + timedelta(seconds=1)
        previous_cooldown = supervisor.task_cooldown.get("worker")
        await supervisor.restart_task("worker")
        for _ in range(5):
            await orig_sleep(0)
            current_cooldown = supervisor.task_cooldown.get("worker")
            if current_cooldown is not None and current_cooldown != previous_cooldown:
                break
        assigned_time = current_time["value"]
        cooldown_3 = (
            supervisor.task_cooldown["worker"] - assigned_time
        ).total_seconds()
        assert cooldown_3 == pytest.approx(20)

        assert supervisor.task_backoff["worker"] == 3
    finally:
        await supervisor.shutdown()