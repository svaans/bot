# tests/test_smoke_startup.py
from __future__ import annotations

import asyncio
import types
import sys
import time
import inspect
import importlib
import pytest


def _install_stubs() -> None:
    def stub(name: str) -> types.ModuleType:
        mod = sys.modules.get(name)
        if mod is None:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
        return mod

    logger_mod = stub('core.utils.logger')
    if not hasattr(logger_mod, 'configurar_logger'):
        logger_mod.configurar_logger = lambda *a, **k: types.SimpleNamespace(
            info=lambda *aa, **kk: None,
            warning=lambda *aa, **kk: None,
            error=lambda *aa, **kk: None,
            critical=lambda *aa, **kk: None,
            debug=lambda *aa, **kk: None,
        )

    obs = stub('observability.metrics')
    if not hasattr(obs, '_get_metric'):
        obs._get_metric = lambda *a, **k: None

    utils = stub('core.utils.healthchecks')
    if not hasattr(utils, 'check_system_clock'):
        utils.check_system_clock = lambda *a, **k: True
    if not hasattr(utils, 'check_storage_writable'):
        utils.check_storage_writable = lambda *a, **k: True

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
    params = inspect.signature(StartupManager).parameters
    kwargs = {}
    if 'config_manager' in params:
        kwargs['config_manager'] = ConfigManager()
    elif 'config' in params:
        kwargs['config'] = getattr(ConfigManager(), 'config', ConfigManager())
    elif 'cfg' in params:
        kwargs['cfg'] = getattr(ConfigManager(), 'config', ConfigManager())
    if 'data_feed' in params:
        kwargs['data_feed'] = feed
    elif 'feed' in params:
        kwargs['feed'] = feed
    if 'trader' in params:
        kwargs['trader'] = Trader()
    elif 'runner' in params:
        kwargs['runner'] = Trader()
    if 'ws_timeout' in params:
        kwargs['ws_timeout'] = ws_timeout
    if 'startup_timeout' in params:
        kwargs['startup_timeout'] = startup_timeout
    return StartupManager(**kwargs)


@pytest.mark.asyncio
async def test_startup_succeeds_when_feed_becomes_active(monkeypatch):
    """
    StartupManager debe completar cuando el DataFeed se activa dentro del ws_timeout.
    Limpia sys.modules['core.startup_manager'] para evitar stubs de otros tests.
    """
    _install_stubs()

    # ← limpiar contaminación de módulos de otros tests
    if 'core.startup_manager' in sys.modules:
        del sys.modules['core.startup_manager']

    from core.startup_manager import StartupManager  # type: ignore
    import core.data_feed as df_mod  # type: ignore
    import core.data.bootstrap as bootstrap  # type: ignore
    from core.trader_modular import Trader  # type: ignore
    from config.config_manager import ConfigManager  # type: ignore

    class _MiniDF:
        def __init__(self, data=None, columns=None):
            self._data = list(data or [])
            self._columns = list(columns or [])
        def __len__(self):
            return len(self._data)
        def to_csv(self, *a, **k):
            return None
    _fake_pd = types.SimpleNamespace(
        DataFrame=_MiniDF,
        read_csv=lambda *a, **k: _MiniDF([], []),
    )
    monkeypatch.setattr(bootstrap, 'pd', _fake_pd, raising=False)

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
    feed = df_mod.DataFeed()

    async def start_feed():
        await asyncio.sleep(0.01)
        feed._set_active(True)
    monkeypatch.setattr(feed, 'start', start_feed)

    async def fake_fetch_ohlcv_async(cliente, symbol, tf, limit=400):
        return [[i, 1.0, 1.0, 1.0, 1.0, 10.0] for i in range(min(10, limit))]
    monkeypatch.setattr(bootstrap, 'fetch_ohlcv_async', fake_fetch_ohlcv_async, raising=True)

    async def fast_run(self):
        await asyncio.sleep(0.01)
    monkeypatch.setattr(Trader, 'run', fast_run, raising=False)

    sm = _make_startup_manager(
        StartupManager=StartupManager,
        Trader=Trader,
        feed=feed,
        ConfigManager=ConfigManager,
        ws_timeout=0.5,
        startup_timeout=1.0,
    )

    if not hasattr(sm, 'config') or not hasattr(sm.config, 'symbols') or not hasattr(sm.config, 'intervalo_velas'):
        sm.config = types.SimpleNamespace(
            symbols=['BTCUSDT'],
            intervalo_velas='1m',
            modo_real=False,
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
    StartupManager debe fallar cuando el DataFeed nunca se activa dentro del ws_timeout.
    También limpia módulos para evitar stubs previos.
    """
    _install_stubs()

    if 'core.startup_manager' in sys.modules:
        del sys.modules['core.startup_manager']

    from core.startup_manager import StartupManager  # type: ignore
    import core.data_feed as df_mod  # type: ignore
    import core.data.bootstrap as bootstrap  # type: ignore
    from core.trader_modular import Trader  # type: ignore
    from config.config_manager import ConfigManager  # type: ignore

    class _MiniDF:
        def __init__(self, data=None, columns=None):
            self._data = list(data or [])
            self._columns = list(columns or [])
        def __len__(self):
            return len(self._data)
        def to_csv(self, *a, **k):
            return None
    _fake_pd = types.SimpleNamespace(
        DataFrame=_MiniDF,
        read_csv=lambda *a, **k: _MiniDF([], []),
    )
    monkeypatch.setattr(bootstrap, 'pd', _fake_pd, raising=False)

    class _DummyFeed:
        def __init__(self, *a, **k) -> None:
            self._active = False
        async def start(self) -> None:
            await asyncio.sleep(0.2)
        def is_active(self) -> bool:
            return self._active

    monkeypatch.setattr(df_mod, 'DataFeed', _DummyFeed, raising=True)
    feed = df_mod.DataFeed()

    async def fake_fetch_ohlcv_async(cliente, symbol, tf, limit=400):
        return [[i, 1.0, 1.0, 1.0, 1.0, 1.0, 10.0] for i in range(min(10, limit))]
    monkeypatch.setattr(bootstrap, 'fetch_ohlcv_async', fake_fetch_ohlcv_async, raising=True)

    sm = _make_startup_manager(
        StartupManager=StartupManager,
        Trader=Trader,
        feed=feed,
        ConfigManager=ConfigManager,
        ws_timeout=0.1,
        startup_timeout=0.3,
    )

    if not hasattr(sm, 'config') or not hasattr(sm.config, 'symbols') or not hasattr(sm.config, 'intervalo_velas'):
        sm.config = types.SimpleNamespace(
            symbols=['BTCUSDT'],
            intervalo_velas='1m',
            modo_real=False,
        )

    with pytest.raises(RuntimeError):
        await sm.run()






