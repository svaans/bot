# tests/test_smoke_startup.py
"""
Integration tests for the bot startup sequence.

These tests exercise the :class:`core.startup_manager.StartupManager` end-to-end,
without spinning up real network connections or a full Binance client. The aim
is to verify that the bot's startup orchestration works correctly when the
``DataFeed`` becomes active in time and to reproduce a timeout when it does not.
"""

from __future__ import annotations

import asyncio
import types
import sys
import time
import pytest


def _install_stubs() -> None:
    """Install minimal stubs needed by StartupManager to run without IO."""
    def stub(name: str) -> types.ModuleType:
        mod = sys.modules.get(name)
        if mod is None:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
        return mod

    # --- Logger stub ---
    logger_mod = stub('core.utils.logger')
    if not hasattr(logger_mod, 'configurar_logger'):
        logger_mod.configurar_logger = lambda *a, **k: types.SimpleNamespace(
            info=lambda *aa, **kk: None,
            warning=lambda *aa, **kk: None,
            error=lambda *aa, **kk: None,
            critical=lambda *aa, **kk: None,
            debug=lambda *aa, **kk: None,
        )

    # --- Warmup stub ---
    warmup_mod = stub('core.warmup_inicial')
    if not hasattr(warmup_mod, 'warmup_inicial'):
        async def warmup_inicial(*a, **k):  # no-op warmup
            await asyncio.sleep(0)
        warmup_mod.warmup_inicial = warmup_inicial

    # --- ConfigManager stub ---
    cfg_manager = stub('config.config_manager')
    if not hasattr(cfg_manager, 'ConfigManager'):
        class Config:
            # minimal fields used by StartupManager / Trader
            modo_real = False
            symbols = ['BTCUSDT']
            max_spread_ratio = 0.0
        class ConfigManager:
            def __init__(self) -> None:
                self.config = Config()
        cfg_manager.ConfigManager = ConfigManager

    # --- DataFeed stub (we will override methods in tests) ---
    data_feed_mod = stub('core.data_feed')
    if not hasattr(data_feed_mod, 'DataFeed'):
        class DataFeed:
            def __init__(self, *a, **k) -> None:
                self._active = False
            async def start(self) -> None:
                await asyncio.sleep(0)
            def is_active(self) -> bool:
                return self._active
            def _set_active(self, value: bool) -> None:
                self._active = value
        data_feed_mod.DataFeed = DataFeed

    # --- Trader stub (run loop will be patched in test) ---
    trader_modular = stub('core.trader_modular')
    if not hasattr(trader_modular, 'Trader'):
        class Trader:
            def __init__(self, *a, **k) -> None:
                self._running = False
                self.config = types.SimpleNamespace(max_spread_ratio=0.0)
            async def run(self) -> None:
                self._running = True
                # Simulate a short-running loop so StartupManager can create task
                while self._running:
                    await asyncio.sleep(0.01)
            def stop(self) -> None:
                self._running = False
        trader_modular.Trader = Trader

    # --- Observability / metrics stub (no-op) ---
    obs = stub('observability.metrics')
    if not hasattr(obs, '_get_metric'):
        obs._get_metric = lambda *a, **k: None

    # --- Misc health/storages checks used by StartupManager ---
    utils = stub('core.utils.healthchecks')
    if not hasattr(utils, 'check_system_clock'):
        utils.check_system_clock = lambda *a, **k: True
    if not hasattr(utils, 'check_storage_writable'):
        utils.check_storage_writable = lambda *a, **k: True

    # aiohttp stub for anything importing it (avoid real IO)
    aiohttp = stub('aiohttp')
    if not hasattr(aiohttp, 'ClientSession'):
        class ClientSession:
            async def __aenter__(self): return self
            async def __aexit__(self, *exc): return False
            async def get(self, *a, **k):
                class _Resp:
                    status = 200
                    async def json(self): return {}
                    async def text(self): return ''
                return _Resp()
        aiohttp.ClientSession = ClientSession


@pytest.mark.asyncio
async def test_startup_succeeds_when_feed_becomes_active(monkeypatch):
    """
    The StartupManager should complete when the DataFeed becomes active
    within the ws_timeout.
    """
    _install_stubs()

    from core.startup_manager import StartupManager  # type: ignore
    from core.data_feed import DataFeed  # type: ignore
    from core.trader_modular import Trader  # type: ignore
    from config.config_manager import ConfigManager  # type: ignore

    # Prepare a DataFeed instance that will flip to active after a short delay
    feed = DataFeed()
    async def start_feed():
        await asyncio.sleep(0.01)
        feed._set_active(True)
    monkeypatch.setattr(feed, 'start', start_feed)

    # Minimal Trader.run that exits quickly (but enough to be scheduled)
    async def fast_run(self):
        # One short iteration to get scheduled
        await asyncio.sleep(0.01)
    monkeypatch.setattr(Trader, 'run', fast_run, raising=True)

    # Build StartupManager with small timeouts
    sm = StartupManager(
        config_manager=ConfigManager(),
        trader=Trader(),
        data_feed=feed,
        ws_timeout=0.5,         # tight timeout to keep test fast
        startup_timeout=1.0,    # overall startup limit
    )

    t0 = time.perf_counter()
    await sm.run()
    elapsed = time.perf_counter() - t0

    # Assertions: feed is active and startup finished promptly
    assert feed.is_active() is True
    assert elapsed < 1.0  # sanity check that it didn't hang
    # Trader task should have been created (if StartupManager stores it)
    if hasattr(sm, '_trader_task'):
        assert sm._trader_task is not None
        assert not sm._trader_task.done()


@pytest.mark.asyncio
async def test_startup_fails_when_feed_never_active(monkeypatch):
    """
    The StartupManager should raise when the DataFeed never becomes active
    within the ws_timeout, surfacing the websocket-not-connected scenario.
    """
    _install_stubs()

    from core.startup_manager import StartupManager  # type: ignore
    from core.data_feed import DataFeed  # type: ignore
    from core.trader_modular import Trader  # type: ignore
    from config.config_manager import ConfigManager  # type: ignore

    feed = DataFeed()
    # Ensure start() does NOT activate the feed
    async def never_activate():
        await asyncio.sleep(0.2)  # pretend to try connecting
        # remains inactive
    monkeypatch.setattr(feed, 'start', never_activate, raising=True)

    sm = StartupManager(
        config_manager=ConfigManager(),
        trader=Trader(),
        data_feed=feed,
        ws_timeout=0.1,       # very short timeout to provoke failure
        startup_timeout=0.3,
    )

    with pytest.raises(RuntimeError):
        await sm.run()
