import numpy as np
import pandas as pd
import core.scoring as sc


def generate_df(n=30):
    rng = np.random.default_rng(2)
    data = rng.random(n) * 10 + 100
    highs = data + rng.random(n)
    lows = data - rng.random(n)
    return pd.DataFrame({"close": data, "high": highs, "low": lows})


def test_score_rust_path(monkeypatch):
    df = generate_df()
    args = {"rsi": 55.0, "momentum": 0.01, "slope": 0.001, "tendencia": "alcista", "symbol": "AAA"}
    monkeypatch.setattr(sc, "HAS_RUST", False)
    score_py = sc.calcular_score_tecnico(df, **args)
    monkeypatch.setattr(sc, "HAS_RUST", True)
    monkeypatch.setattr(sc, "_score_rust", lambda *a, **k: score_py)
    score_rust = sc.calcular_score_tecnico(df, **args)
    assert score_py == score_rust