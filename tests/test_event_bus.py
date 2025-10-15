import asyncio
import threading

import pytest

from core.event_bus import EventBus


def _create_bus_with_pending_event() -> EventBus:
    bus = EventBus()
    bus.emit("boot", 7)
    return bus


@pytest.mark.asyncio
async def test_emit_buffered_until_loop_registered() -> None:
    bus = _create_bus_with_pending_event()
    results: list[int] = []

    async def listener(data: int) -> None:
        results.append(data)

    bus.subscribe("boot", listener)

    for _ in range(10):
        if results:
            break
        await asyncio.sleep(0)

    assert results == [7]
    await bus.close()


@pytest.mark.asyncio
async def test_event_bus_publish_and_subscribe() -> None:
    bus = EventBus()
    results: list[int] = []

    async def listener(data: int) -> None:
        results.append(data)

    bus.subscribe("test", listener)
    await bus.publish("test", 42)

    for _ in range(10):
        if results:
            break
        await asyncio.sleep(0)

    assert results == [42]
    await bus.close()


@pytest.mark.asyncio
async def test_emit_from_thread_without_loop_is_not_lost() -> None:
    bus = EventBus()
    results: list[str] = []

    async def listener(data: str) -> None:
        results.append(data)

    def emit_from_thread() -> None:
        bus.emit("thread", "ok")

    thread = threading.Thread(target=emit_from_thread)
    thread.start()
    thread.join()

    bus.subscribe("thread", listener)

    for _ in range(10):
        if results:
            break
        await asyncio.sleep(0)

    assert results == ["ok"]
    await bus.close()


@pytest.mark.asyncio
async def test_event_bus_wait_resolves_on_emit() -> None:
    bus = EventBus()

    async def trigger() -> None:
        await asyncio.sleep(0)
        bus.emit("ready", {"ok": True})

    asyncio.create_task(trigger())
    data = await asyncio.wait_for(bus.wait("ready"), timeout=1)

    assert data == {"ok": True}
    await bus.close()


@pytest.mark.asyncio
async def test_event_bus_wait_sync_blocks_until_event() -> None:
    bus = EventBus()
    bus.start()

    async def trigger() -> None:
        await asyncio.sleep(0.01)
        await bus.publish("ready", {"ok": True})

    result, _ = await asyncio.gather(
        asyncio.to_thread(bus.wait_sync, "ready", 1.0),
        trigger(),
    )

    assert result == {"ok": True}
    await bus.close()


@pytest.mark.asyncio
async def test_event_bus_close_stops_dispatcher() -> None:
    bus = EventBus()
    closed = False

    async def listener(_: int) -> None:
        nonlocal closed
        closed = True

    bus.subscribe("stop", listener)
    await bus.publish("stop", 1)
    await asyncio.sleep(0)
    await bus.close()

    assert bus._closed is True
    assert closed is True


@pytest.mark.asyncio
async def test_event_bus_inflight_tasks_cleanup() -> None:
    bus = EventBus()
    done = asyncio.Event()

    async def listener(_: object) -> None:
        await asyncio.sleep(0)
        done.set()

    bus.subscribe("tick", listener)
    await bus.publish("tick", None)
    await asyncio.wait_for(done.wait(), timeout=1)
    await asyncio.sleep(0)

    assert not bus._inflight
    await bus.close()


@pytest.mark.asyncio
async def test_event_bus_payload_cache_prunes_old_entries() -> None:
    bus = EventBus(max_cached_payloads=2)

    bus.emit("alpha", 1)
    bus.emit("beta", 2)
    bus.emit("gamma", 3)

    # Permitir que las tareas de publish pendientes se ejecuten antes de esperar.
    await asyncio.sleep(0)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(bus.wait("alpha", timeout=0.01), timeout=0.05)

    assert await bus.wait("gamma") == 3
    await bus.close()