from __future__ import annotations

from datetime import timedelta

import pytest

from core.metrics import TASK_TIMEOUT_SECONDS
from core.supervisor import Supervisor


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