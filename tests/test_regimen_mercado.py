"""Clasificación de régimen por volatilidad (ATR/cierre)."""

from __future__ import annotations

import pandas as pd
import pytest

from core.strategies.regimen_mercado import (
    aplicar_multiplicadores_regimen,
    atr_ratio_actual,
    etiqueta_volatilidad,
)


def _df_plana(n: int = 25) -> pd.DataFrame:
    close = pd.Series([100.0 + i * 0.01 for i in range(n)], dtype=float)
    return pd.DataFrame(
        {
            "timestamp": range(n),
            "open": close,
            "high": close + 0.02,
            "low": close - 0.02,
            "close": close,
            "volume": [10.0] * n,
        }
    )


def _df_volatil(n: int = 25) -> pd.DataFrame:
    close = pd.Series([100.0 + i * 0.01 for i in range(n)], dtype=float)
    return pd.DataFrame(
        {
            "timestamp": range(n),
            "open": close,
            "high": close + 6.0,
            "low": close - 6.0,
            "close": close,
            "volume": [10.0] * n,
        }
    )


def test_atr_ratio_volatil_mayor_que_plano() -> None:
    p = _df_plana()
    v = _df_volatil()
    assert atr_ratio_actual(p) < atr_ratio_actual(v)


def test_etiqueta_volatilidad() -> None:
    assert etiqueta_volatilidad(_df_volatil(), umbral_alto=0.02, umbral_bajo=0.001) == "alta"
    assert etiqueta_volatilidad(_df_plana(), umbral_alto=0.5, umbral_bajo=0.002) == "baja"


def test_aplicar_multiplicadores_desactivado() -> None:
    u, s = aplicar_multiplicadores_regimen(2.0, 1.5, "alta", {"regimen_entrada_enabled": False})
    assert u == 2.0 and s == 1.5


def test_aplicar_multiplicadores_alta() -> None:
    cfg = {
        "regimen_entrada_enabled": True,
        "regimen_mult_umbral_alta": 1.5,
        "regimen_mult_umbral_score_alta": 2.0,
    }
    u, s = aplicar_multiplicadores_regimen(2.0, 1.0, "alta", cfg)
    assert u == pytest.approx(3.0)
    assert s == pytest.approx(2.0)
