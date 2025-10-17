import asyncio
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp import test_utils

from core.notification_manager import (
    DeliveryReport,
    NotificationDeliveryError,
    NotificationManager,
    NOTIFY_RETRY,
    NOTIFY_SKIPPED,
    _TelegramBackend,
)


@pytest.mark.asyncio
async def test_telegram_backend_disabled_returns_skip() -> None:
    backend = _TelegramBackend(token=None, chat_id=None)
    report = await backend.send("hola mundo")
    assert isinstance(report, DeliveryReport)
    assert report.status == "skipped"
    assert report.details == {"reason": "telegram_disabled"}


@pytest.mark.asyncio
async def test_telegram_backend_real_http_success() -> None:
    called: dict[str, Any] = {"value": False}

    async def handler(request: web.Request) -> web.Response:
        body = await request.json()
        assert body["chat_id"] == "chat"
        assert body["text"] == "mensaje"
        assert body["parse_mode"] == "Markdown"
        assert body["disable_web_page_preview"] is True
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
            report = await backend.send("mensaje")
            assert report.status == "success"
            assert report.details and report.details["response"]["ok"] is True
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
async def test_notification_manager_retries_on_failure() -> None:
    retry_metric = NOTIFY_RETRY.labels(channel="telegram")
    before = retry_metric._value

    class FakeBackend:
        async def send(self, _: str, *, meta: dict[str, Any] | None = None) -> DeliveryReport:
            raise NotificationDeliveryError("fallo simulado")

    manager = NotificationManager(backends={"telegram": FakeBackend()}, retries=(0.01,))

    manager.start()
    await manager.enviar_async("mensaje", nivel="INFO")

    await asyncio.sleep(0.05)
    await manager.close()
    assert retry_metric._value == before + 1


@pytest.mark.asyncio
async def test_notification_manager_dedup_skips_duplicates() -> None:
    backend_calls: list[str] = []

    class CapturingBackend:
        async def send(self, text: str, *, meta: dict[str, Any] | None = None) -> DeliveryReport:
            backend_calls.append(text)
            return DeliveryReport(status="success", details=None)

    manager = NotificationManager(backends={"telegram": CapturingBackend()}, dedup_ttl=60.0, retries=())
    manager.start()
    await manager.enviar_async("mensaje", nivel="INFO", dedup_key="abc")
    await manager.enviar_async("mensaje", nivel="INFO", dedup_key="abc")
    await asyncio.sleep(0.05)
    await manager.close()

    assert backend_calls == ["[INFO] mensaje"]
    skipped_metric = NOTIFY_SKIPPED.labels(channel="dedup")
    assert skipped_metric._value >= 1


@pytest.mark.asyncio
async def test_notification_manager_multiple_channels() -> None:
    calls: dict[str, list[str]] = {"tg": [], "discord": []}

    class TgBackend:
        async def send(self, text: str, *, meta: dict[str, Any] | None = None) -> DeliveryReport:
            calls["tg"].append(text)
            return DeliveryReport(status="success", details=None)

    class DiscordBackend:
        async def send(self, text: str, *, meta: dict[str, Any] | None = None) -> DeliveryReport:
            calls["discord"].append(text)
            return DeliveryReport(status="success", details=None)

    manager = NotificationManager(
        backends={"telegram": TgBackend(), "discord": DiscordBackend()},
        retries=(),
    )
    manager.start()
    await manager.enviar_async("hola", nivel="WARN", channels=("telegram", "discord"))
    await asyncio.sleep(0.05)
    await manager.close()

    assert calls["tg"] == ["[WARN] hola"]
    assert calls["discord"] == ["[WARN] hola"]
