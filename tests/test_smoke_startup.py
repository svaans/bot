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
import inspect
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

    # --- Trader stub (run loop will be patched in test) ---
    trader_modular = stub('core.trader_modular')
    if not hasattr(trader_modular, 'Trader'):
        class Trader:
            def __init__(self, *a, **k) -> None:
                self._running = False
                self.config = types.SimpleNamespace(max_spread_ratio=0.0)
            async def run(self) -> None:
                self._running = True
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


def _make_startup_manager(StartupManager, Trader, feed, ConfigManager, ws_timeout, startup_timeout):
    """
    Crea una instancia de StartupManager adaptándose a su firma real.
    Intenta mapear config/config_manager/cfg, data_feed/feed, trader/runner, etc.
    """
    params = inspect.signature(StartupManager).parameters
    kwargs = {}

    # Config: puede ser 'config_manager', 'config' o 'cfg' (o no existir)
    if 'config_manager' in params:
        kwargs['config_manager'] = ConfigManager()
    elif 'config' in params:
        kwargs['config'] = getattr(ConfigManager(), 'config', ConfigManager())
    elif 'cfg' in params:
        kwargs['cfg'] = getattr(ConfigManager(), 'config', ConfigManager())

    # DataFeed: puede llamarse 'data_feed' o 'feed'
    if 'data_feed' in params:
        kwargs['data_feed'] = feed
    elif 'feed' in params:
        kwargs['feed'] = feed

    # Trader: puede llamarse 'trader' o 'runner'
    if 'trader' in params:
        kwargs['trader'] = Trader()
    elif 'runner' in params:
        kwargs['runner'] = Trader()

    # Timeouts (si existen en la firma)
    if 'ws_timeout' in params:
        kwargs['ws_timeout'] = ws_timeout
    if 'startup_timeout' in params:
        kwargs['startup_timeout'] = startup_timeout

    return StartupManager(**kwargs)


@pytest.mark.asyncio
async def test_startup_succeeds_when_feed_becomes_active(monkeypatch):
    """
    The StartupManager should complete when the DataFeed becomes active
    within the ws_timeout.
    """
    _install_stubs()

    from core.startup_manager import StartupManager  # type: ignore
    import core.data_feed as df_mod  # type: ignore
    from core.trader_modular import Trader  # type: ignore
    from config.config_manager import ConfigManager  # type: ignore

    # --- Replace real DataFeed by a minimal dummy to avoid signature coupling ---
    class _DummyFeed:
        def __init__(self, *a, **k) -> None:
            self._active = False
        async def start(self) -> None:
            await asyncio.sleep(0)
        def is_active(self) -> bool:
            return self._active
        def _set_active(self, v: bool) -> None:
            self._active = v

    monkeypatch.setattr(df_mod, 'DataFeed', _DummyFeed, raising=True)

    feed = df_mod.DataFeed()  # uses dummy

    # Flip to active shortly after start()
    async def start_feed():
        await asyncio.sleep(0.01)
        feed._set_active(True)
    monkeypatch.setattr(feed, 'start', start_feed)

    # Minimal Trader.run that exits quickly (but enough to be scheduled)
    async def fast_run(self):
        await asyncio.sleep(0.01)
    monkeypatch.setattr(Trader, 'run', fast_run, raising=True)

    sm = _make_startup_manager(
        StartupManager=StartupManager,
        Trader=Trader,
        feed=feed,
        ConfigManager=ConfigManager,
        ws_timeout=0.5,
        startup_timeout=1.0,
    )

    t0 = time.perf_counter()
    await sm.run()
    elapsed = time.perf_counter() - t0

    assert feed.is_active() is True
    assert elapsed < 1.0
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
    import core.data_feed as df_mod  # type: ignore
    from core.trader_modular import Trader  # type: ignore
    from config.config_manager import ConfigManager  # type: ignore

    # --- Replace real DataFeed by a minimal dummy to avoid signature coupling ---
    class _DummyFeed:
        def __init__(self, *a, **k) -> None:
            self._active = False
        async def start(self) -> None:
            await asyncio.sleep(0.2)  # simulate trying to connect
        def is_active(self) -> bool:
            return self._active
        def _set_active(self, v: bool) -> None:
            self._active = v

    monkeypatch.setattr(df_mod, 'DataFeed', _DummyFeed, raising=True)

    feed = df_mod.DataFeed()  # remains inactive

    sm = _make_startup_manager(
        StartupManager=StartupManager,
        Trader=Trader,
        feed=feed,
        ConfigManager=ConfigManager,
        ws_timeout=0.1,
        startup_timeout=0.3,
    )

    with pytest.raises(RuntimeError):
        await sm.run()


