"""Pruebas para el módulo de backtesting de régimen de mercado."""

from __future__ import annotations

from typing import Dict, Generator

import pandas as pd
import pytest

import core.market_regime as market_regime
from core.market_regime import RegimeThresholds
from core.market_regime_backtest import calibrar_umbrales_por_activo


@pytest.fixture(autouse=True)
def _reset_regimen_state() -> Generator[None, None, None]:
    market_regime.reset_regimen_cache()
    yield
    market_regime.reset_regimen_cache()


@pytest.fixture(autouse=True)
def _mock_indicadores(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_vol(df: pd.DataFrame, *_args, **_kwargs) -> float:
        if "volatilidad_mock" not in df:
            return 0.0
        return float(df["volatilidad_mock"].iloc[-1])

    def fake_slope(df: pd.DataFrame, *_args, **_kwargs) -> float:
        if "slope_mock" not in df:
            return 0.0
        return float(df["slope_mock"].iloc[-1])

    def fake_confirm(
        df: pd.DataFrame, slope_pct: float, thresholds: RegimeThresholds
    ) -> bool | None:
        if "confirmacion_mock" not in df:
            return None
        valor = df["confirmacion_mock"].iloc[-1]
        if pd.isna(valor):
            return None
        return bool(valor)

    def fake_atr_ratio(df: pd.DataFrame) -> float:
        if "atr_ratio_mock" not in df:
            return 0.0
        return float(df["atr_ratio_mock"].iloc[-1])

    monkeypatch.setattr(market_regime, "medir_volatilidad", fake_vol)
    monkeypatch.setattr(market_regime, "pendiente_medias", fake_slope)
    monkeypatch.setattr(market_regime, "_multi_horizon_confirmation", fake_confirm)
    monkeypatch.setattr(market_regime, "_atr_ratio_actual", fake_atr_ratio)


def _dataset(
    symbol: str,
    *,
    etiqueta: str,
    volatilidad: float,
    slope: float,
    confirmacion: bool | None,
    size: int = 80,
) -> pd.DataFrame:
    registros: list[Dict[str, float | int | str | bool | None]] = []
    base = 100.0
    timestamp = 1_700_000_000
    for i in range(size):
        close = base + 0.05 * i
        open_ = close - 0.02
        high = close + 0.05
        low = close - 0.05
        volume = 100 + i * 0.2
        registros.append(
            {
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "timestamp": timestamp + i * 60,
                "regimen_real": etiqueta,
                "volatilidad_mock": volatilidad,
                "slope_mock": slope,
                "confirmacion_mock": confirmacion,
                "atr_ratio_mock": volatilidad * 0.8,
            }
        )
    df = pd.DataFrame(registros)
    df.attrs["symbol"] = symbol
    return df


def test_calibrar_umbrales_por_activo_identifica_umbral_optimo() -> None:
    btc = _dataset(
        "BTCUSDT",
        etiqueta="tendencial",
        volatilidad=0.01,
        slope=0.0045,
        confirmacion=True,
    )
    eth = _dataset(
        "ETHUSDT",
        etiqueta="alta_volatilidad",
        volatilidad=0.03,
        slope=0.0004,
        confirmacion=None,
    )

    resultados = calibrar_umbrales_por_activo(
        {"BTCUSDT": btc, "ETHUSDT": eth},
        etiqueta_col="regimen_real",
        window=20,
        step=5,
        alta_vol_candidates=[0.015, 0.05],
        slope_upper_candidates=[0.004, 0.006],
        slope_lower_candidates=[0.001, 0.002],
    )

    btc_res = resultados["BTCUSDT"]
    assert btc_res.total > 0
    assert btc_res.accuracy == pytest.approx(1.0)
    assert btc_res.thresholds.slope_upper == pytest.approx(0.004)
    assert btc_res.thresholds.alta_volatilidad == pytest.approx(0.015)
    assert btc_res.confusion_matrix == {"tendencial": {"tendencial": btc_res.total}}

    eth_res = resultados["ETHUSDT"]
    assert eth_res.total > 0
    assert eth_res.accuracy == pytest.approx(1.0)
    assert eth_res.thresholds.alta_volatilidad == pytest.approx(0.015)
    assert eth_res.thresholds.slope_upper == pytest.approx(0.004)
    assert eth_res.confusion_matrix == {
        "alta_volatilidad": {"alta_volatilidad": eth_res.total}
    }