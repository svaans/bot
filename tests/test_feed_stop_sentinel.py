import asyncio, pytest

@pytest.mark.asyncio
async def test_consumer_termina_con_sentinel():
    q = asyncio.Queue()
    SENTINEL = object()
    async def consumer():
        while True:
            it = await q.get()
            if it is SENTINEL:
                break
    task = asyncio.create_task(consumer())
    await asyncio.sleep(0)
    await q.put(SENTINEL)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert task.done()
