import numpy as np
import pandas as pd

from indicators.rsi import calcular_rsi, calcular_rsi_fast
import indicators.rsi as rsi_mod
from indicators.atr import calcular_atr, calcular_atr_fast
from indicators.slope import calcular_slope, calcular_slope_fast
import fast_indicators as fi


def generate_df(n=30):
    rng = np.random.default_rng(0)
    data = rng.random(n) * 10 + 100
    highs = data + rng.random(n)
    lows = data - rng.random(n)
    df = pd.DataFrame({"close": data, "high": highs, "low": lows})
    return df


def test_rsi_fast_matches_python():
    df = generate_df()
    fast_series = calcular_rsi_fast(df, periodo=14, serie_completa=True)
    py_series = calcular_rsi(df, periodo=14, serie_completa=True)
    assert np.allclose(fast_series, py_series, equal_nan=True)


def test_atr_fast_matches_python():
    df = generate_df()
    fast_val = calcular_atr_fast(df, periodo=14)
    py_val = calcular_atr(df, periodo=14)
    if np.isnan(fast_val):
        assert np.isnan(py_val)
    else:
        assert fast_val == py_val


def test_slope_fast_matches_python():
    df = generate_df()
    fast_val = calcular_slope_fast(df, periodo=5)
    py_val = calcular_slope(df, periodo=5)
    assert fast_val == py_val


def test_extension_import():
    assert hasattr(fi, "HAS_FAST")
    if fi.HAS_FAST:
        # simple call to ensure the extension works
        arr = fi.rsi(np.array([1.0, 2.0, 3.0]), 2)
        assert arr.size == 3


def test_rsi_fast_rust_path(monkeypatch):
    df = generate_df()
    called = []

    def fake_rust(close: np.ndarray, periodo: int) -> np.ndarray:
        called.append(True)
        return fi.rsi(close, periodo)

    monkeypatch.setattr(rsi_mod, "HAS_RUST", True)
    monkeypatch.setattr(rsi_mod, "_rsi_rust", fake_rust)
    monkeypatch.setattr(rsi_mod, "_rsi_fast", lambda *a, **k: (_ for _ in ()).throw(AssertionError("_rsi_fast used")))

    fast_series = calcular_rsi_fast(df, periodo=14, serie_completa=True)
    py_series = calcular_rsi(df, periodo=14, serie_completa=True)

    assert called and np.allclose(fast_series, py_series, equal_nan=True)