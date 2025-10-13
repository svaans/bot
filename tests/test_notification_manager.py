import asyncio
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp import test_utils

from core.notification_manager import (
    NotificationDeliveryError,
    NotificationManager,
    _TelegramBackend,
)


@pytest.mark.asyncio
async def test_telegram_backend_disabled_returns_true() -> None:
    backend = _TelegramBackend(token=None, chat_id=None)
    assert await backend.send("hola mundo") is True


@pytest.mark.asyncio
async def test_telegram_backend_real_http_success() -> None:
    called: dict[str, Any] = {"value": False}

    async def handler(request: web.Request) -> web.Response:
        body = await request.json()
        assert body["chat_id"] == "chat"
        assert body["text"] == "mensaje"
        called["value"] = True
        return web.json_response({"ok": True, "result": {"message_id": 123}})

    app = web.Application()
    app.router.add_post("/botTOKEN/sendMessage", handler)
    server = test_utils.TestServer(app)
    await server.start_server()
    try:
        base_url = str(server.make_url(""))
        async with aiohttp.ClientSession() as session:
            backend = _TelegramBackend(
                token="TOKEN",
                chat_id="chat",
                session=session,
                base_url=base_url,
            )
            assert await backend.send("mensaje") is True
        assert called["value"] is True
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_telegram_backend_http_error_raises() -> None:
    async def handler(_: web.Request) -> web.Response:
        return web.json_response({"ok": False, "description": "forbidden"}, status=403)

    app = web.Application()
    app.router.add_post("/botTOKEN/sendMessage", handler)
    server = test_utils.TestServer(app)
    await server.start_server()
    try:
        base_url = str(server.make_url(""))
        async with aiohttp.ClientSession() as session:
            backend = _TelegramBackend(
                token="TOKEN",
                chat_id="chat",
                session=session,
                base_url=base_url,
            )
            with pytest.raises(NotificationDeliveryError):
                await backend.send("mensaje")
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_notification_manager_retries_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = NotificationManager(token="TOKEN", chat_id="chat", retries=(0.01,))

    class FakeBackend:
        async def send(self, _: str) -> bool:
            raise NotificationDeliveryError("fallo simulado")

    monkeypatch.setattr(manager, "_backend", FakeBackend())

    manager.start()
    await manager.enviar_async("mensaje", nivel="INFO")

    await asyncio.sleep(0.05)
    await manager.close()
