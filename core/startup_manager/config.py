"""Funciones relacionadas con la carga y manipulación de configuración."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import replace, is_dataclass
from typing import Any, Optional

from config.config_manager import ConfigManager
from core.trader_modular import Trader

from .types import Config


class ConfigMixin:
    """Proporciona utilidades de configuración para :class:`StartupManager`."""

    trader: Optional[Trader]
    config: Optional[Config]
    event_bus: Any | None
    log: Any
    _previous_snapshot: dict[str, Any] | None

    async def _load_config(self) -> None:
        """Carga la configuración y construye el trader si es necesario."""
        if self.trader is not None and self.config is not None:
            self._inspect_previous_snapshot()
            self._restore_persistencia_state()
            return

        if self.trader is not None and self.config is None:
            self.config = getattr(self.trader, "config", None)

        if self.config is None:
            self.config = ConfigManager.load_from_env()

        if self.trader is None:
            self.trader = Trader(self.config)  # type: ignore[arg-type]

        if self.trader is not None and getattr(self, "event_bus", None) is None:
            self.event_bus = getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)

        self._inspect_previous_snapshot()
        self._restore_persistencia_state()

    def _get_config_value(self, key: str, default: Any | None = None) -> Any | None:
        cfg = self.config or getattr(self.trader, "config", None)
        if cfg is None:
            return default
        getter = getattr(cfg, "get", None)
        if callable(getter):
            try:
                return getter(key, default)
            except Exception:
                return default
        return getattr(cfg, key, default)

    def _set_config_value(self, key: str, value: Any) -> None:
        cfg = self.config or getattr(self.trader, "config", None)
        if cfg is None:
            return
        if hasattr(cfg, "__setitem__"):
            try:
                cfg[key] = value
                return
            except Exception:
                pass
        with suppress(Exception):
            setattr(cfg, key, value)

    async def _apply_clock_drift_safety(self) -> None:
        """Desactiva el modo real cuando la verificación de reloj falla."""
        if self.config is not None:
            if is_dataclass(self.config):
                self.config = replace(self.config, modo_real=False)
            else:
                with suppress(Exception):
                    setattr(self.config, "modo_real", False)
        if hasattr(self.trader, "config"):
            with suppress(Exception):
                self.trader.config = self.config
        with suppress(Exception):
            setattr(self.trader, "modo_real", False)
        with suppress(Exception):
            setattr(self.trader, "cliente", None)

    async def _stop_trader(self) -> None:
        """Cierra el trader durante los rollbacks."""
        trader = self.trader
        if trader is None:
            return
        cerrar = getattr(trader, "cerrar", None)
        if not callable(cerrar):
            return
        try:
            result = cerrar()
            if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                with suppress(asyncio.CancelledError, Exception):
                    await result  # type: ignore[func-returns-value]
        except Exception:
            self.log.debug("Excepción al cerrar Trader durante rollback", exc_info=True)

    async def _close_trader(self) -> None:
        """Alias retrocompatible de :meth:`_stop_trader`."""
        await self._stop_trader()
