import numpy as np
import pandas as pd
from core.utils import umbral_helper as uh


def generate_df(n=40):
    rng = np.random.default_rng(0)
    data = rng.random(n) * 10 + 100
    highs = data + rng.random(n)
    lows = data - rng.random(n)
    vols = rng.random(n) * 1000 + 100
    return pd.DataFrame({"close": data, "high": highs, "low": lows, "volume": vols})


def test_metricas_rust_path(monkeypatch):
    df = generate_df()
    called = []

    def fake_rust(close, high, low, volume):
        called.append(True)
        return uh._calcular_metricas_py(df)

    monkeypatch.setattr(uh, "HAS_RUST", True)
    monkeypatch.setattr(uh, "_calcular_metricas_rust", fake_rust)

    res = uh._calcular_metricas(df)
    assert called and res == uh._calcular_metricas_py(df)


def test_limites_rust_path(monkeypatch):
    val = 7.3
    called = []

    def fake_rust(x):
        called.append(True)
        return uh._limites_adaptativos_py(x)

    monkeypatch.setattr(uh, "HAS_RUST", True)
    monkeypatch.setattr(uh, "_limites_adaptativos_rust", fake_rust)

    res = uh._limites_adaptativos(val)
    assert called and res == uh._limites_adaptativos_py(val)


def test_calculo_umbral_consistente(monkeypatch):
    df = generate_df()
    estrategias = {"a": True}
    pesos = {"a": 5}

    monkeypatch.setattr(uh, "HAS_RUST", False)
    umbral_py = uh.calcular_umbral_avanzado("T", df, estrategias, pesos)

    monkeypatch.setattr(uh, "HAS_RUST", True)
    monkeypatch.setattr(uh, "_calcular_metricas_rust", lambda *a: uh._calcular_metricas_py(df))
    monkeypatch.setattr(uh, "_limites_adaptativos_rust", lambda x: uh._limites_adaptativos_py(x))
    umbral_rust = uh.calcular_umbral_avanzado("T", df, estrategias, pesos)

    assert umbral_py == umbral_rust