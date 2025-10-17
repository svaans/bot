from __future__ import annotations

import math

import pytest

from core.ajustador_riesgo import ajustar_sl_tp_riesgo, es_modo_agresivo


@pytest.mark.parametrize(
    "volatilidad, slope_pct, esperado",
    [
        # Volatilidad elevada recorta riesgo y amplía SL.
        (
            0.03,
            0.002,
            (1.8, 3.3, 0.0480),
        ),
        # Contexto de baja volatilidad con pendiente acusada aumenta riesgo.
        (
            0.008,
            0.0025,
            (1.5, 3.3, 0.0720),
        ),
        # Tendencia bajista moderada ajusta TP a la baja y mantiene SL dentro de límites.
        (
            0.015,
            -0.003,
            (1.65, 2.7, 0.06),
        ),
    ],
)
def test_ajustar_sl_tp_riesgo_responde_a_contexto(volatilidad, slope_pct, esperado):
    sl, tp, riesgo = ajustar_sl_tp_riesgo(volatilidad, slope_pct)
    sl_esperado, tp_esperado, riesgo_esperado = esperado
    assert pytest.approx(sl, rel=1e-3) == sl_esperado
    assert pytest.approx(tp, rel=1e-3) == tp_esperado
    assert pytest.approx(riesgo, rel=1e-3) == riesgo_esperado


@pytest.mark.parametrize(
    "volatilidad, slope_pct, esperado",
    [
        (0.021, 0.0001, True),
        (0.005, 0.003, True),
        (0.005, 0.0005, False),
    ],
)
def test_es_modo_agresivo_umbral(volatilidad, slope_pct, esperado):
    assert es_modo_agresivo(volatilidad, slope_pct) is esperado


def test_es_modo_agresivo_prioriza_config_sobre_entorno(monkeypatch):
    monkeypatch.setenv("MODO_AGRESIVO_VOL_THRESHOLD", "0.05")
    monkeypatch.setenv("MODO_AGRESIVO_SLOPE_THRESHOLD", "0.01")

    assert es_modo_agresivo(
        volatilidad=0.03,
        slope_pct=0.0002,
        vol_threshold=0.015,
        slope_threshold=0.02,
    )


def test_es_modo_agresivo_lee_umbral_por_simbolo(monkeypatch):
    monkeypatch.setenv("MODO_AGRESIVO_VOL_THRESHOLD_BTCUSDT", "0.1")
    monkeypatch.setenv("MODO_AGRESIVO_SLOPE_THRESHOLD_BTCUSDT", "0.02")

    assert not es_modo_agresivo(
        volatilidad=0.05,
        slope_pct=0.01,
        symbol="BTCUSDT",
    )
    # Otro símbolo debe usar los valores globales por defecto.
    assert es_modo_agresivo(
        volatilidad=0.05,
        slope_pct=0.003,
        symbol="ETHUSDT",
    )


@pytest.mark.parametrize(
    "volatilidad, slope_pct, regime, esperado",
    [
        (0.018, 0.0005, "alta_volatilidad", (1.65, 2.85, 0.0420)),
        (0.012, 0.003, "tendencial", (1.42, 3.79, 0.0630)),
        (0.009, 0.0001, "lateral", (1.5, 2.7, 0.0540)),
    ],
)
def test_ajustar_sl_tp_riesgo_considera_regimen(volatilidad, slope_pct, regime, esperado):
    sl, tp, riesgo = ajustar_sl_tp_riesgo(
        volatilidad,
        slope_pct,
        regime=regime,
    )
    sl_esperado, tp_esperado, riesgo_esperado = esperado
    assert pytest.approx(sl, rel=1e-3) == sl_esperado
    assert pytest.approx(tp, rel=1e-3) == tp_esperado
    assert pytest.approx(riesgo, rel=1e-3) == riesgo_esperado


def test_ajustar_sl_tp_riesgo_respeta_limites_extremos():
    sl, tp, riesgo = ajustar_sl_tp_riesgo(
        volatilidad=1.0,
        slope_pct=-0.05,
        base_riesgo=0.5,
        sl_ratio=10.0,
        tp_ratio=0.5,
    )
    # SL y TP se ciñen a los límites documentados.
    assert math.isclose(sl, 5.0, rel_tol=1e-9)
    assert math.isclose(tp, 6.0, rel_tol=1e-9)
    # El riesgo nunca se vuelve negativo y respeta redondeo.
    assert 0.0 <= riesgo <= 0.5
    assert round(riesgo, 4) == pytest.approx(riesgo, rel=0)