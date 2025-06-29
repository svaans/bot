import numpy as np
import pandas as pd

from indicators.rsi import calcular_rsi, calcular_rsi_fast
import indicators.rsi as rsi_mod
from indicators.atr import calcular_atr, calcular_atr_fast
import indicators.atr as atr_mod
from indicators.slope import calcular_slope, calcular_slope_fast
import indicators.slope as slope_mod


def generate_df(n=30):
    rng = np.random.default_rng(1)
    data = rng.random(n) * 10 + 100
    highs = data + rng.random(n)
    lows = data - rng.random(n)
    return pd.DataFrame({"close": data, "high": highs, "low": lows})


def test_rsi_rust_path(monkeypatch):
    df = generate_df()
    monkeypatch.setattr(rsi_mod, "HAS_RUST", False)
    py_series = calcular_rsi_fast(df, periodo=14, serie_completa=True)
    monkeypatch.setattr(rsi_mod, "HAS_RUST", True)
    orig_fast = rsi_mod._rsi_fast
    monkeypatch.setattr(rsi_mod, "_rsi_rust", lambda close, p: orig_fast(close, p))
    monkeypatch.setattr(rsi_mod, "_rsi_fast", lambda *a, **k: (_ for _ in ()).throw(AssertionError("_rsi_fast used")))
    rust_series = calcular_rsi_fast(df, periodo=14, serie_completa=True)
    assert np.allclose(py_series, rust_series, equal_nan=True)


def test_atr_rust_path(monkeypatch):
    df = generate_df()
    monkeypatch.setattr(atr_mod, "HAS_RUST", False)
    py_val = calcular_atr_fast(df, periodo=14)
    monkeypatch.setattr(atr_mod, "HAS_RUST", True)
    orig_fast = atr_mod._atr_fast
    monkeypatch.setattr(atr_mod, "_atr_rust", lambda h, l, c, p: orig_fast(h, l, c, p))
    monkeypatch.setattr(atr_mod, "_atr_fast", lambda *a, **k: (_ for _ in ()).throw(AssertionError("_atr_fast used")))
    rust_val = calcular_atr_fast(df, periodo=14)
    if np.isnan(py_val):
        assert np.isnan(rust_val)
    else:
        assert rust_val == py_val


def test_slope_rust_path(monkeypatch):
    df = generate_df()
    monkeypatch.setattr(slope_mod, "HAS_RUST", False)
    py_val = calcular_slope_fast(df, periodo=5)
    monkeypatch.setattr(slope_mod, "HAS_RUST", True)
    orig_fast = slope_mod._slope_fast
    monkeypatch.setattr(slope_mod, "_slope_rust", lambda close, p: orig_fast(close, p))
    monkeypatch.setattr(slope_mod, "_slope_fast", lambda *a, **k: (_ for _ in ()).throw(AssertionError("_slope_fast used")))
    rust_val = calcular_slope_fast(df, periodo=5)
    assert rust_val == py_val