"""Implementación principal del StartupManager modularizado."""

from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from typing import Any, Optional

from core.diag.phase_logger import phase
from core.trader_modular import Trader
from core.utils.utils import configurar_logger

from .bootstrap import BootstrapMixin
from .config import ConfigMixin
from .feed import FeedLifecycleMixin
from .snapshot import SnapshotMixin
from .types import Config
from .ws import WebSocketMonitorMixin


class StartupManager(
    ConfigMixin,
    BootstrapMixin,
    FeedLifecycleMixin,
    WebSocketMonitorMixin,
    SnapshotMixin,
):
    """Orquesta las fases de arranque del bot."""

    def __init__(
        self,
        trader: Optional[Trader] = None,
        *,
        data_feed: Any | None = None,
        feed: Any | None = None,
        config: Optional[Config] = None,
        ws_timeout: float | None = None,
        startup_timeout: float | None = None,
    ) -> None:
        self.trader = trader
        self.data_feed = data_feed if data_feed is not None else feed
        if self.data_feed is None and trader is not None:
            self.data_feed = getattr(trader, "data_feed", None)

        self.config: Optional[Config] = config or getattr(trader, 'config', None)
        if self.trader is not None and self.config is not None:
            with suppress(Exception):
                self.trader.config = self.config

        if self.trader is not None and self.data_feed is not None:
            with suppress(Exception):
                setattr(self.trader, "data_feed", self.data_feed)

        self.task: Optional[asyncio.Task] = None
        self._feed_task: Optional[asyncio.Task] = None
        self._trader_hold: Optional[asyncio.Event] = None
        self._fallback_ws_signal_task: Optional[asyncio.Task] = None
        self._ws_started_logged = False
        self._datafeed_connected_announced = False

        self.ws_timeout = ws_timeout
        if startup_timeout is None:
            env_timeout = os.getenv("STARTUP_TIMEOUT")
            try:
                self.startup_timeout = float(env_timeout) if env_timeout else 90.0
            except (TypeError, ValueError):
                self.startup_timeout = 90.0
        else:
            try:
                self.startup_timeout = float(startup_timeout)
            except (TypeError, ValueError):
                self.startup_timeout = 90.0

        self.event_bus = None
        if self.trader is not None:
            self.event_bus = getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)

        if self.config is not None and ws_timeout is not None:
            with suppress(Exception):
                setattr(self.config, "ws_timeout", ws_timeout)

        self.log = configurar_logger('startup')
        self._restart_alert_seconds = self._resolve_restart_alert_threshold()
        self._previous_snapshot: dict[str, Any] | None = None

    async def run(self) -> tuple[Trader, asyncio.Task, Config]:
        executed = [self._stop_trader]
        try:
            await self._load_config()

            with phase("_bootstrap"):
                await asyncio.wait_for(self._bootstrap(), timeout=15)

            assert self.trader is not None, "Trader no inicializado tras bootstrap"
            feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
            if feed is not None:
                with suppress(Exception):
                    setattr(self.trader, "data_feed", feed)

            feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
            if feed is not None and hasattr(feed, "verificar_continuidad"):
                if not feed.verificar_continuidad():
                    raise RuntimeError("DataFeed sin continuidad al arrancar")
            else:
                self.log.debug(
                    "Omitiendo verificación de continuidad del DataFeed (stub de tests o feed no disponible)."
                )

            await self._validate_feeds()
            await self._open_streams()
            executed.append(self._stop_streams)

            ws_timeout = self._resolve_ws_timeout()
            with phase("_wait_ws", extra={"timeout": ws_timeout}):
                await asyncio.wait_for(
                    self._wait_ws(ws_timeout),
                    timeout=ws_timeout + 5,
                )

            await self._enable_strategies()

            assert self.task is not None, "La tarea principal del Trader no quedó inicializada"
            assert self.config is not None, "Config no inicializada"

            return self.trader, self.task, self.config  # type: ignore[return-value]
        except Exception as e:
            self.log.error(f'Fallo en arranque: {e}')
            for rollback in reversed(executed):
                with suppress(Exception):
                    await rollback()
            raise
