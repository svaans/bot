# tests/test_data_feed.py
"""Tests covering the simplified DataFeed implementation.

Estas pruebas verifican el comportamiento de la clase ``data_feed.lite.DataFeed``
de forma aislada.  Se instalan stubs para librerías externas como ``dotenv``,
``colorama``, ``prometheus_client``, ``numpy``, ``pandas``, ``sqlalchemy``,
``ta`` y ``binance_api``.  La atención se centra en la lógica de propiedades
(como el flag ``activos`` y la verificación de continuidad) y en el método
de backfill ``precargar``.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
import types

import pytest


def _install_data_feed_stubs() -> None:
    """Registra stubs ligeros para los módulos importados por ``data_feed.lite``."""
    def stub_module(name: str) -> types.ModuleType:
        mod = sys.modules.get(name)
        if mod is None:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
        return mod

    stub_module('dotenv').load_dotenv = lambda *args, **kwargs: None
    stub_module('colorlog').ColoredFormatter = lambda *args, **kwargs: None
    stub_module('colorama')
    prometheus_client = stub_module('prometheus_client')
    for name in ['Counter', 'Gauge', 'Summary', 'Histogram']:
        setattr(prometheus_client, name, lambda *args, **kwargs: None)
    prometheus_client.start_wsgi_server = lambda *args, **kwargs: None
    stub_module('psutil')
    stub_module('aiomonitor')
    numpy = stub_module('numpy')
    numpy.nan = float('nan')
    pandas = stub_module('pandas')
    pandas.DataFrame = object
    stub_module('pandas.tseries')
    stub_module('pandas.tseries.offsets')
    stub_module('pandas.tseries.frequencies').to_offset = lambda *args, **kwargs: None
    stub_module('scipy')
    matplotlib = stub_module('matplotlib')
    matplotlib.pyplot = stub_module('matplotlib.pyplot')
    ta = stub_module('ta')
    ta.trend = types.SimpleNamespace(ADXIndicator=lambda *args, **kwargs: None)
    ta.momentum = types.SimpleNamespace(RSIIndicator=lambda *args, **kwargs: None)
    stub_module('ccxt').binance = lambda *args, **kwargs: None
    stub_module('filelock').FileLock = lambda *args, **kwargs: None
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
    async def _dummy_ws(*args, **kwargs): return
    binance_api.websocket.escuchar_velas = _dummy_ws  # type: ignore
    binance_api.websocket.escuchar_velas_combinado = _dummy_ws  # type: ignore
    binance_api.websocket.InactividadTimeoutError = Exception
    binance_api.cliente = stub_module('binance_api.cliente')
    async def _dummy_fetch(*args, **kwargs): return []
    binance_api.cliente.fetch_ohlcv_async = _dummy_fetch  # type: ignore
    binance_api.cliente.crear_cliente = lambda *args, **kwargs: None
    config_mod = stub_module('config.config')
    config_mod.BACKFILL_MAX_CANDLES = 500
    config_mod.INTERVALO_VELAS = '1m'
    utils_logger = stub_module('core.utils.logger')
    utils_logger.configurar_logger = lambda *args, **kwargs: types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None, critical=lambda *a, **k: None)
    utils_mod = stub_module('core.utils')
    utils_mod.intervalo_a_segundos = lambda s: 60 if isinstance(s, str) else s
    utils_mod.validar_integridad_velas = lambda *args, **kwargs: True
    utils_mod.timestamp_alineado = lambda *args, **kwargs: True
    utils_mod.configurar_logger = utils_logger.configurar_logger


@pytest.fixture(autouse=True)
def install_data_feed_stubs(monkeypatch) -> None:
    """Instala stubs y limpia el módulo ``data_feed.lite`` antes de cada prueba."""
    _install_data_feed_stubs()
    sys.modules.pop('data_feed.lite', None)
    yield


def test_data_feed_initialization_sets_interval_and_defaults() -> None:
    """El constructor debe establecer el intervalo y derivar valores por defecto sensatos."""
    import data_feed.lite  # importación retrasada tras los stubs
    DataFeed = data_feed.lite.DataFeed
    df = DataFeed('1m')
    assert df.intervalo == '1m'
    assert df.intervalo_segundos == 60
    df2 = DataFeed('1m', queue_policy='invalid')
    assert df2.queue_policy == 'drop_oldest'


def test_data_feed_activos_flag_logic() -> None:
    """``activos`` solo es True cuando el feed está en ejecución y tiene tareas pendientes."""
    import data_feed.lite
    DataFeed = data_feed.lite.DataFeed
    df = DataFeed('1m')
    assert not df.activos
    df._running = True
    df._tasks = {}
    assert not df.activos
    done_future = asyncio.Future()
    done_future.set_result(None)
    df._tasks['DONE'] = done_future
    assert not df.activos
    pending_future = asyncio.Future()
    df._tasks['PENDING'] = pending_future
    assert df.activos


def test_data_feed_verificar_continuidad_logic() -> None:
    """Verificar la continuidad en distintos escenarios de backfill."""
    import data_feed.lite
    DataFeed = data_feed.lite.DataFeed
    df = DataFeed('1m')
    assert df.verificar_continuidad() is True
    df._symbols = ['BTCUSDT']
    assert df.verificar_continuidad() is False
    df._last_close_ts['BTCUSDT'] = 1000
    df._last_backfill_ts['BTCUSDT'] = 1000
    assert df.verificar_continuidad() is True
    df._last_backfill_ts['BTCUSDT'] = 1000
    df._last_close_ts['BTCUSDT'] = 1000 + df.intervalo_segundos * 2 * 1000
    assert df.verificar_continuidad() is False


@pytest.mark.asyncio
async def test_data_feed_precargar_resets_min_buffer() -> None:
    """``precargar`` debe invocar el backfill y restaurar ``min_buffer_candles``."""
    import data_feed.lite
    DataFeed = data_feed.lite.DataFeed
    df = DataFeed('1m')
    original_min = df.min_buffer_candles
    called: list[str] = []
    async def dummy_backfill(symbol: str) -> None:
        called.append(symbol)
    df._do_backfill = dummy_backfill  # type: ignore
    await df.precargar(['BTCUSDT'], minimo=5)
    assert called == ['BTCUSDT']
    assert df.min_buffer_candles == original_min
