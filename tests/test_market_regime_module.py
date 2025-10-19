import importlib
import sys
import types

import pandas as pd
import pytest

if "indicators" not in sys.modules:
    indicators_pkg = types.ModuleType("indicators")
    sys.modules["indicators"] = indicators_pkg
    for nombre in ("ema", "rsi"):
        modulo = types.ModuleType(f"indicators.{nombre}")
        setattr(indicators_pkg, nombre, modulo)
        sys.modules[f"indicators.{nombre}"] = modulo

market_regime = importlib.import_module("core.market_regime")


def make_df(close_values: list[float], *, symbol: str = "BTCUSDT") -> pd.DataFrame:
    size = len(close_values)
    df = pd.DataFrame(
        {
            "close": close_values,
            "high": [c * 1.01 for c in close_values],
            "low": [c * 0.99 for c in close_values],
            "volume": [1.0] * size,
        }
    )
    df.attrs["symbol"] = symbol
    return df


@pytest.fixture(autouse=True)
def _reset_state() -> None:
    market_regime.reset_regimen_cache()


def test_medir_volatilidad_sin_atr(monkeypatch: pytest.MonkeyPatch) -> None:
    df = make_df([100.0, 101.0])
    monkeypatch.setattr(market_regime, "get_atr", lambda *_, **__: None)
    assert market_regime.medir_volatilidad(df) == 0.0


def test_medir_volatilidad_cierre_cero(monkeypatch: pytest.MonkeyPatch) -> None:
    df = make_df([100.0, 0.0])
    monkeypatch.setattr(market_regime, "get_atr", lambda *_, **__: 5.0)
    assert market_regime.medir_volatilidad(df) == 0.0


def test_medir_volatilidad_devuelve_ratio(monkeypatch: pytest.MonkeyPatch) -> None:
    df = make_df([100.0, 105.0, 110.0])
    monkeypatch.setattr(market_regime, "get_atr", lambda *_, **__: 2.2)
    assert pytest.approx(market_regime.medir_volatilidad(df), rel=1e-6) == 2.2 / 110.0


def test_pendiente_medias_sin_close() -> None:
    df = pd.DataFrame({"open": [1, 2, 3]})
    assert market_regime.pendiente_medias(df) == 0.0


def test_pendiente_medias_datos_insuficientes() -> None:
    df = make_df([1.0, 2.0, 3.0, 4.0])
    assert market_regime.pendiente_medias(df, ventana=5) == 0.0


def test_pendiente_medias_usa_sma(monkeypatch: pytest.MonkeyPatch) -> None:
    df = make_df([float(i) for i in range(1, 15)])
    captured: dict[str, int] = {}

    def fake_slope(valores: pd.Series) -> float:
        captured["len"] = len(valores)
        return 0.123

    monkeypatch.setattr(market_regime, "calcular_slope_pct", fake_slope)
    resultado = market_regime.pendiente_medias(df, ventana=5)
    assert resultado == pytest.approx(0.123)
    assert captured["len"] == len(df["close"].rolling(window=5).mean().dropna())


def test_detectar_regimen_alta_vol(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(market_regime, "medir_volatilidad", lambda *_: 0.05)
    monkeypatch.setattr(market_regime, "pendiente_medias", lambda *_: 0.0005)
    monkeypatch.setattr(market_regime, "_multi_horizon_confirmation", lambda *_: None)
    monkeypatch.setattr(market_regime, "_atr_ratio_actual", lambda *_: 0.01)
    df = make_df([100.0] * 40)
    assert market_regime.detectar_regimen(df) == "alta_volatilidad"


def test_detectar_regimen_tendencial_alcista(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(market_regime, "medir_volatilidad", lambda *_: 0.01)
    monkeypatch.setattr(market_regime, "pendiente_medias", lambda *_: 0.004)
    monkeypatch.setattr(market_regime, "_multi_horizon_confirmation", lambda *_: True)
    monkeypatch.setattr(market_regime, "_atr_ratio_actual", lambda *_: 0.01)
    df = make_df([100.0] * 40)
    assert market_regime.detectar_regimen(df) == "tendencial"


def test_detectar_regimen_tendencial_bajista(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(market_regime, "medir_volatilidad", lambda *_: 0.01)
    monkeypatch.setattr(market_regime, "pendiente_medias", lambda *_: -0.004)
    monkeypatch.setattr(market_regime, "_multi_horizon_confirmation", lambda *_: True)
    monkeypatch.setattr(market_regime, "_atr_ratio_actual", lambda *_: 0.01)
    df = make_df([100.0] * 40)
    assert market_regime.detectar_regimen(df) == "tendencial"


def test_detectar_regimen_lateral(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(market_regime, "medir_volatilidad", lambda *_: 0.01)
    monkeypatch.setattr(market_regime, "pendiente_medias", lambda *_: 0.0005)
    monkeypatch.setattr(market_regime, "_multi_horizon_confirmation", lambda *_: None)
    monkeypatch.setattr(market_regime, "_atr_ratio_actual", lambda *_: 0.004)
    df = make_df([100.0] * 40)
    assert market_regime.detectar_regimen(df) == "lateral"


def test_histeresis_mantiene_tendencia(monkeypatch: pytest.MonkeyPatch) -> None:
    df = make_df([100.0] * 400)
    monkeypatch.setattr(market_regime, "medir_volatilidad", lambda *_: 0.01)
    monkeypatch.setattr(market_regime, "_multi_horizon_confirmation", lambda *_: True)
    monkeypatch.setattr(market_regime, "_atr_ratio_actual", lambda *_: 0.01)

    monkeypatch.setattr(market_regime, "pendiente_medias", lambda *_: 0.004)
    assert market_regime.detectar_regimen(df) == "tendencial"

    monkeypatch.setattr(market_regime, "pendiente_medias", lambda *_: 0.002)
    assert market_regime.detectar_regimen(df) == "tendencial"

    monkeypatch.setattr(market_regime, "pendiente_medias", lambda *_: 0.0005)
    assert market_regime.detectar_regimen(df) == "lateral"


def test_implied_volatility_eleva_a_alta_vol(monkeypatch: pytest.MonkeyPatch) -> None:
    df = make_df([100.0] * 40)
    df.attrs["implied_volatility"] = 0.03
    monkeypatch.setattr(market_regime, "medir_volatilidad", lambda *_: 0.01)
    monkeypatch.setattr(market_regime, "pendiente_medias", lambda *_: 0.0005)
    monkeypatch.setattr(market_regime, "_multi_horizon_confirmation", lambda *_: None)
    monkeypatch.setattr(market_regime, "_atr_ratio_actual", lambda *_: 0.005)
    assert market_regime.detectar_regimen(df) == "alta_volatilidad"


def test_confirmacion_multi_horizonte_bloquea_tendencia(monkeypatch: pytest.MonkeyPatch) -> None:
    df = make_df([100.0] * 200)
    monkeypatch.setattr(market_regime, "medir_volatilidad", lambda *_: 0.01)
    monkeypatch.setattr(market_regime, "pendiente_medias", lambda *_: 0.004)
    monkeypatch.setattr(market_regime, "_multi_horizon_confirmation", lambda *_: False)
    monkeypatch.setattr(market_regime, "_atr_ratio_actual", lambda *_: 0.01)
    assert market_regime.detectar_regimen(df) == "lateral"