"""Servidor HTTP embebido para el dashboard del bot."""
from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

from aiohttp import web

from .state import get_snapshot, obtener_capital_real, update_from_heartbeat

_log = logging.getLogger("dashboard")

_INDEX_HTML = Path(__file__).parent / "index.html"


class _HeartbeatLogHandler(logging.Handler):
    """Handler que intercepta el logger trader.heartbeat y actualiza el estado."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if hasattr(record, "ws_connected"):
                update_from_heartbeat(record)
        except Exception:
            pass


_handler_installed = False


def install_log_handler() -> None:
    """Registra el handler en el logger del heartbeat (idempotente)."""
    global _handler_installed
    if _handler_installed:
        return
    logger = logging.getLogger("trader.heartbeat")
    logger.addHandler(_HeartbeatLogHandler())
    _handler_installed = True


async def _handle_status(request: web.Request) -> web.Response:
    try:
        snapshot = get_snapshot()
        try:
            snapshot["capital_real"] = await obtener_capital_real()
        except Exception:
            snapshot["capital_real"] = None
        body = json.dumps(snapshot, default=str, ensure_ascii=False)
        return web.Response(
            text=body,
            content_type="application/json",
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as exc:
        return web.Response(status=500, text=str(exc))


async def _handle_index(request: web.Request) -> web.Response:
    try:
        html = _INDEX_HTML.read_text(encoding="utf-8")
        return web.Response(text=html, content_type="text/html")
    except Exception as exc:
        return web.Response(status=500, text=f"Error loading dashboard: {exc}")


def _json(data: dict, status: int = 200) -> web.Response:
    return web.Response(
        text=json.dumps(data, default=str, ensure_ascii=False),
        content_type="application/json",
        status=status,
        headers={"Access-Control-Allow-Origin": "*"},
    )


async def _handle_study_status(request: web.Request) -> web.Response:
    from .study import estado_estudio  # noqa: PLC0415

    return _json(estado_estudio())


async def _dias_de_request(request: web.Request) -> int:
    try:
        if request.body_exists:
            body = await request.json()
            return int(body.get("days", 365))
        if "days" in request.query:
            return int(request.query["days"])
    except Exception:
        pass
    return 365


async def _handle_study_run(request: web.Request) -> web.Response:
    from .study import lanzar_estudio  # noqa: PLC0415

    return _json(lanzar_estudio(days=await _dias_de_request(request)))


async def _handle_sim_status(request: web.Request) -> web.Response:
    from .study import estado_simulacion  # noqa: PLC0415

    return _json(estado_simulacion())


async def _handle_sim_run(request: web.Request) -> web.Response:
    from .study import lanzar_simulacion  # noqa: PLC0415

    days = 365
    aprendizaje = False
    try:
        if request.body_exists:
            body = await request.json()
            days = int(body.get("days", 365))
            aprendizaje = bool(body.get("aprendizaje", False))
        elif "days" in request.query:
            days = int(request.query["days"])
            aprendizaje = request.query.get("aprendizaje") in ("1", "true", "yes")
    except Exception:
        days, aprendizaje = 365, False
    return _json(lanzar_simulacion(days=days, aprendizaje=aprendizaje))


async def run_dashboard(port: int | None = None) -> None:
    """Arranca el servidor y bloquea hasta la cancelación."""
    install_log_handler()
    port = port or int(os.getenv("DASHBOARD_PORT", "8080"))
    # En un VPS conviene no exponer el dashboard a internet (no tiene auth):
    # DASHBOARD_HOST=127.0.0.1 + túnel SSH, o la IP privada de Tailscale.
    host = os.getenv("DASHBOARD_HOST", "0.0.0.0")

    app = web.Application()
    app.router.add_get("/", _handle_index)
    app.router.add_get("/api/status", _handle_status)
    app.router.add_get("/api/study/status", _handle_study_status)
    app.router.add_post("/api/study/run", _handle_study_run)
    app.router.add_get("/api/sim/status", _handle_sim_status)
    app.router.add_post("/api/sim/run", _handle_sim_run)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    _log.info("Dashboard disponible en http://localhost:%s", port)
    print(f"📊 Dashboard: http://localhost:{port}")

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()
