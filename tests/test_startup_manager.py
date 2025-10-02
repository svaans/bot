# tests/test_startup_manager.py
"""Tests for the StartupManager orchestrator.

Este módulo ejerce los métodos internos de ``core.startup_manager.StartupManager``
sin depender de bibliotecas externas.  Se instalan stubs para todos los módulos
que se importan en tiempo de carga, permitiendo así importar ``core.startup_manager``
y llamar a sus funciones.  Cada prueba se centra en un fragmento de lógica.
"""

from __future__ import annotations

import asyncio
from typing import Generator
import importlib
import sys
import types
import time
from typing import Any  # <- añadido

import pytest


def _install_startup_stubs() -> None:
    """Instala stubs ligeros en ``sys.modules`` para paquetes externos."""
    def stub_module(name: str) -> types.ModuleType:
        mod = sys.modules.get(name)
        if mod is None:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
        return mod

    dotenv = stub_module('dotenv')
    if not hasattr(dotenv, 'load_dotenv'):
        dotenv.load_dotenv = lambda *args, **kwargs: None
    colorlog = stub_module('colorlog')
    if not hasattr(colorlog, 'ColoredFormatter'):
        colorlog.ColoredFormatter = lambda *args, **kwargs: None
    stub_module('colorama')

    prometheus_client = stub_module('prometheus_client')
    for name in ['Counter', 'Gauge', 'Summary', 'Histogram']:
        if not hasattr(prometheus_client, name):
            setattr(prometheus_client, name, lambda *args, **kwargs: None)
    if not hasattr(prometheus_client, 'start_wsgi_server'):
        prometheus_client.start_wsgi_server = lambda *args, **kwargs: None

    stub_module('psutil')
    stub_module('aiomonitor')

    numpy = stub_module('numpy')
    if not hasattr(numpy, 'nan'):
        numpy.nan = float('nan')

    pandas = stub_module('pandas')
    if not hasattr(pandas, 'DataFrame'):
        pandas.DataFrame = object
    tseries = stub_module('pandas.tseries')
    stub_module('pandas.tseries.offsets')
    frequencies = stub_module('pandas.tseries.frequencies')
    if not hasattr(frequencies, 'to_offset'):
        frequencies.to_offset = lambda *args, **kwargs: None

    stub_module('scipy')
    matplotlib = stub_module('matplotlib')
    matplotlib.pyplot = stub_module('matplotlib.pyplot')

    ta = stub_module('ta')
    trend = stub_module('ta.trend')
    if not hasattr(trend, 'ADXIndicator'):
        trend.ADXIndicator = lambda *args, **kwargs: None
    momentum = stub_module('ta.momentum')
    if not hasattr(momentum, 'RSIIndicator'):
        momentum.RSIIndicator = lambda *args, **kwargs: None

    ccxt = stub_module('ccxt')
    if not hasattr(ccxt, 'binance'):
        ccxt.binance = lambda *args, **kwargs: None

    filelock = stub_module('filelock')
    if not hasattr(filelock, 'FileLock'):
        filelock.FileLock = lambda *args, **kwargs: None

    sqlalchemy = stub_module('sqlalchemy')
    sqlalchemy.create_engine = lambda *args, **kwargs: None
    sqlalchemy.MetaData = lambda *args, **kwargs: None
    sqlalchemy.Table = lambda *args, **kwargs: None
    sqlalchemy.Column = lambda *args, **kwargs: None
    sqlalchemy.Float = float
    sqlalchemy.Integer = int
    sqlalchemy.String = str
    sqlalchemy.select = lambda *args, **kwargs: None
    sqlalchemy.text = lambda *args, **kwargs: None
    sqlalchemy.exc = types.SimpleNamespace(IntegrityError=Exception)

    binance_api = stub_module('binance_api')
    binance_api.websocket = stub_module('binance_api.websocket')
    binance_api.cliente = stub_module('binance_api.cliente')
    if not hasattr(binance_api.cliente, 'crear_cliente'):
        binance_api.cliente.crear_cliente = lambda *args, **kwargs: None

    cfg = stub_module('config.config')
    if not hasattr(cfg, 'BACKFILL_MAX_CANDLES'):
        cfg.BACKFILL_MAX_CANDLES = 500
    if not hasattr(cfg, 'INTERVALO_VELAS'):
        cfg.INTERVALO_VELAS = '1m'

    trader_mod = stub_module('core.trader_modular')
    class DummyTrader:
        def __init__(self, config: Any) -> None:
            self.config = config
            self.data_feed = types.SimpleNamespace(activos=False, verificar_continuidad=lambda: True)
            self.cliente = None
            self.modo_real = getattr(config, 'modo_real', False)
        async def _precargar_historico(self) -> None: return
        async def ejecutar(self) -> None: return
        async def cerrar(self) -> None: return
        def habilitar_estrategias(self) -> None: return
    trader_mod.Trader = DummyTrader  # type: ignore

    data_bootstrap = stub_module('core.data.bootstrap')
    if not hasattr(data_bootstrap, 'warmup_inicial'):
        async def warmup_inicial(symbols, intervalo_velas, min_bars=400): return
        data_bootstrap.warmup_inicial = warmup_inicial  # type: ignore

    core_utils_logger = stub_module('core.utils.logger')
    if not hasattr(core_utils_logger, 'configurar_logger'):
        core_utils_logger.configurar_logger = lambda *args, **kwargs: types.SimpleNamespace(
            info=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            error=lambda *a, **k: None,
            critical=lambda *a, **k: None
        )

    core_utils_utils = stub_module('core.utils.utils')
    if not hasattr(core_utils_utils, 'configurar_logger'):
        core_utils_utils.configurar_logger = core_utils_logger.configurar_logger

    aiohttp = stub_module('aiohttp')
    class _DummyResponse:
        def __init__(self, payload): self._payload = payload
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False
        async def json(self): return self._payload
    class _DummySession:
        def __init__(self, payload): self._payload = payload
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False
        def get(self, *args, **kwargs): return _DummyResponse(self._payload)
    class _Factory:
        def __init__(self, payload): self.payload = payload
        def __call__(self, *args, **kwargs): return _DummySession(self.payload)
    # Definimos ClientError pero no establecemos ClientSession por defecto
    aiohttp.ClientError = Exception


@pytest.fixture(autouse=True)
def install_startup_stubs(monkeypatch) -> Generator[None, None, None]:
    """Instala stubs antes de cada prueba y vacía el caché de importación."""
    _install_startup_stubs()
    sys.modules.pop('core.startup_manager', None)
    yield


@pytest.mark.asyncio
async def test_validate_feeds_requires_client() -> None:
    """Con ``modo_real=True`` y sin cliente, ``_validate_feeds`` debe fallar."""
    importlib.invalidate_caches()
    sm_mod = importlib.import_module('core.startup_manager')
    dummy_cfg = types.SimpleNamespace(modo_real=True, ws_timeout=0.2)
    dummy_trader = types.SimpleNamespace(config=dummy_cfg, cliente=None)
    sm = sm_mod.StartupManager(trader=dummy_trader)
    with pytest.raises(RuntimeError):
        await sm._validate_feeds()


@pytest.mark.asyncio
async def test_validate_feeds_ok_when_simulado() -> None:
    """En modo simulado no se requiere cliente para validar feeds."""
    importlib.invalidate_caches()
    sm_mod = importlib.import_module('core.startup_manager')
    dummy_cfg = types.SimpleNamespace(modo_real=False, ws_timeout=0.2)
    dummy_trader = types.SimpleNamespace(config=dummy_cfg, cliente=None)
    sm = sm_mod.StartupManager(trader=dummy_trader)
    await sm._validate_feeds()


@pytest.mark.asyncio
async def test_wait_ws_times_out(monkeypatch) -> None:
    """``_wait_ws`` debe lanzar si el feed nunca se activa dentro del timeout."""
    importlib.invalidate_caches()
    sm_mod = importlib.import_module('core.startup_manager')
    dummy_feed = types.SimpleNamespace(activos=False)
    dummy_trader = types.SimpleNamespace(data_feed=dummy_feed)
    sm = sm_mod.StartupManager(trader=dummy_trader)
    with pytest.raises(RuntimeError):
        await sm._wait_ws(0.15)


@pytest.mark.asyncio
async def test_wait_ws_success(monkeypatch) -> None:
    """``_wait_ws`` debe retornar cuando el feed se vuelva activo."""
    importlib.invalidate_caches()
    sm_mod = importlib.import_module('core.startup_manager')
    flag = {'active': False}
    class DummyFeed:
        @property
        def activos(self) -> bool: return flag['active']
    dummy_trader = types.SimpleNamespace(data_feed=DummyFeed())
    sm = sm_mod.StartupManager(trader=dummy_trader)
    async def activate(): await asyncio.sleep(0.05); flag['active'] = True
    asyncio.create_task(activate())
    await sm._wait_ws(0.2)


@pytest.mark.asyncio
async def test_check_clock_drift_ok(monkeypatch) -> None:
    """``_check_clock_drift`` devuelve True si la deriva de reloj es pequeña."""
    importlib.invalidate_caches()
    sm_mod = importlib.import_module('core.startup_manager')
    payload = {'serverTime': int(time.time() * 1000)}
    class _GoodResp:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False
        async def json(self): return payload
    class _GoodSession:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False
        def get(self, *args, **kwargs): return _GoodResp()
    aiohttp = sys.modules['aiohttp']
    aiohttp.ClientSession = lambda *args, **kwargs: _GoodSession()
    sm = sm_mod.StartupManager(trader=types.SimpleNamespace())
    assert await sm._check_clock_drift() is True


@pytest.mark.asyncio
async def test_check_clock_drift_large(monkeypatch) -> None:
    """``_check_clock_drift`` devuelve False cuando la deriva es grande."""
    importlib.invalidate_caches()
    sm_mod = importlib.import_module('core.startup_manager')
    payload = {'serverTime': int((time.time() - 10) * 1000)}
    class _BadResp:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False
        async def json(self): return payload
    class _BadSession:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False
        def get(self, *args, **kwargs): return _BadResp()
    aiohttp = sys.modules['aiohttp']
    aiohttp.ClientSession = lambda *args, **kwargs: _BadSession()
    sm = sm_mod.StartupManager(trader=types.SimpleNamespace())
    assert await sm._check_clock_drift() is False


@pytest.mark.asyncio
async def test_check_storage_ok(monkeypatch, tmp_path) -> None:
    """``_check_storage`` debe devolver True cuando puede escribir en el snapshot."""
    importlib.invalidate_caches()
    sm_mod = importlib.import_module('core.startup_manager')
    snapshot_path = tmp_path / 'snapshot.json'
    monkeypatch.setattr(sm_mod, 'SNAPSHOT_PATH', snapshot_path, raising=True)
    sm = sm_mod.StartupManager(trader=types.SimpleNamespace())
    assert await sm._check_storage() is True


@pytest.mark.asyncio
async def test_check_storage_unwritable(monkeypatch) -> None:
    """``_check_storage`` devuelve False cuando no puede escribir en el directorio."""
    importlib.invalidate_caches()
    sm_mod = importlib.import_module('core.startup_manager')
    snapshot_path = types.SimpleNamespace(
        parent=types.SimpleNamespace(mkdir=lambda parents, exist_ok: None),
        write_text=lambda content: (_ for _ in ()).throw(IOError('no write')),
        unlink=lambda: None
    )
    monkeypatch.setattr(sm_mod, 'SNAPSHOT_PATH', snapshot_path, raising=True)
    sm = sm_mod.StartupManager(trader=types.SimpleNamespace())
    assert await sm._check_storage() is False

