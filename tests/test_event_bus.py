import asyncio

import pytest

from core.event_bus import EventBus


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
