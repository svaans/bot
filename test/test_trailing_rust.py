import numpy as np
import pandas as pd
from core.strategies.exit import salida_trailing_stop as st


def generate_df(n=20):
    base = np.arange(n, dtype=float) + 100
    highs = base + 1
    lows = base - 1
    return pd.DataFrame({"close": base, "high": highs, "low": lows})


def test_trailing_long(monkeypatch):
    df = generate_df()
    info = {"precio_entrada": 100, "max_price": 105, "direccion": "long"}
    config = {"trailing_start_ratio": 1.01, "trailing_distance_ratio": 0.02}
    monkeypatch.setattr(st, "HAS_RUST", False)
    res_py = st.verificar_trailing_stop(info.copy(), 102, df, config)
    monkeypatch.setattr(st, "HAS_RUST", True)
    monkeypatch.setattr(st, "_verificar_trailing_stop_rust", lambda *a, **k: res_py)
    res_rust = st.verificar_trailing_stop(info.copy(), 102, df, config)
    assert res_py == res_rust and res_rust[0]


def test_trailing_short(monkeypatch):
    df = generate_df()
    info = {"precio_entrada": 100, "max_price": 105, "direccion": "short"}
    config = {"trailing_start_ratio": 1.01, "trailing_distance_ratio": 0.02}
    monkeypatch.setattr(st, "HAS_RUST", False)
    res_py = st.verificar_trailing_stop(info.copy(), 108, df, config)
    monkeypatch.setattr(st, "HAS_RUST", True)
    monkeypatch.setattr(st, "_verificar_trailing_stop_rust", lambda *a, **k: res_py)
    res_rust = st.verificar_trailing_stop(info.copy(), 108, df, config)
    assert res_py == res_rust