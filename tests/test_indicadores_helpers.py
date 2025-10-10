"""Tests para utilidades de indicadores."""

from __future__ import annotations

import sys
import threading
import time
import types

import pandas as pd

_indicators_pkg = types.ModuleType('indicators')
_indicators_pkg.__path__ = []  # type: ignore[attr-defined]
sys.modules.setdefault('indicators', _indicators_pkg)
sys.modules.setdefault('indicators.ema', types.ModuleType('indicators.ema'))
sys.modules.setdefault('indicators.rsi', types.ModuleType('indicators.rsi'))

from indicadores import helpers
from indicadores.settings import _reset_indicator_settings_cache_for_tests


def test_sanitize_series_respects_global_normalize(monkeypatch):
    monkeypatch.setenv('INDICADORES_NORMALIZE_DEFAULT', 'false')
    _reset_indicator_settings_cache_for_tests()
    serie = pd.Series([1.0, 3.0, 7.0])

    resultado = helpers.sanitize_series(serie, normalize=None)

    try:
        assert list(resultado) == [1.0, 3.0, 7.0]
    finally:
        _reset_indicator_settings_cache_for_tests()


def test_indicator_cache_enforces_lru_limit(monkeypatch):
    monkeypatch.setenv('INDICADORES_CACHE_MAX_ENTRIES', '2')
    _reset_indicator_settings_cache_for_tests()
    df = pd.DataFrame({'close': [1, 2, 3]})
    call_count = {'value': 0}

    def _compute(valor: str):
        def _inner(_df: pd.DataFrame) -> str:
            call_count['value'] += 1
            return valor

        return _inner

    try:
        assert helpers._cached_value(df, ('foo', 1), _compute('a')) == 'a'
        assert helpers._cached_value(df, ('bar', 1), _compute('b')) == 'b'
        assert helpers._cached_value(df, ('foo', 1), _compute('a')) == 'a'

        # Al insertar un tercer elemento se debe expulsar el menos reciente (``bar``)
        assert helpers._cached_value(df, ('baz', 1), _compute('c')) == 'c'
        assert helpers._cached_value(df, ('foo', 1), _compute('a')) == 'a'
        assert helpers._cached_value(df, ('bar', 1), _compute('b')) == 'b'
        assert call_count['value'] == 4
    finally:
        _reset_indicator_settings_cache_for_tests()


def test_indicator_cache_is_thread_safe(monkeypatch):
    monkeypatch.setenv('INDICADORES_CACHE_MAX_ENTRIES', '4')
    _reset_indicator_settings_cache_for_tests()
    df = pd.DataFrame({'close': [1, 2, 3]})
    call_count = {'value': 0}

    def compute(_df: pd.DataFrame) -> float:
        call_count['value'] += 1
        time.sleep(0.05)
        return float(_df['close'].iloc[-1])

    resultados: list[float] = []

    def worker() -> None:
        resultados.append(helpers._cached_value(df, ('thread', 1), compute))

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    try:
        assert resultados == [3.0, 3.0]
        assert call_count['value'] == 1
    finally:
        _reset_indicator_settings_cache_for_tests()
