import numpy as np
import pandas as pd
import core.adaptador_dinamico as ad


def generate_df(n=40):
    rng = np.random.default_rng(0)
    data = rng.random(n) * 50 + 100
    highs = data + rng.random(n)
    lows = data - rng.random(n)
    return pd.DataFrame({"close": data, "high": highs, "low": lows})


def test_tp_sl_rust_path(monkeypatch):
    df = generate_df()
    cfg = {"sl_ratio": 1.5, "tp_ratio": 2.5}
    monkeypatch.setattr(ad, "_fast_tp_sl", None)
    res_py = ad.calcular_tp_sl_adaptativos("AAA/USDT", df, cfg)
    monkeypatch.setattr(ad, "_fast_tp_sl", lambda *a, **k: res_py)
    res_rust = ad.calcular_tp_sl_adaptativos("AAA/USDT", df, cfg)
    assert res_py == res_rust