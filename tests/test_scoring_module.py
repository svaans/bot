from __future__ import annotations

import json

import pandas as pd

from core.scoring import DecisionReason, DecisionTrace, ScoreBreakdown, calcular_score_tecnico


def test_calcular_score_tecnico_long() -> None:
    df = pd.DataFrame({"close": [1, 2, 3]})
    score, breakdown = calcular_score_tecnico(
        df,
        rsi=55.0,
        momentum=0.01,
        slope=0.5,
        tendencia="alcista",
        direccion="long",
    )
    assert isinstance(breakdown, ScoreBreakdown)
    assert score == breakdown.total
    assert breakdown.rsi > 0
    assert breakdown.momentum > 0
    assert breakdown.slope > 0
    assert breakdown.tendencia > 0


def test_calcular_score_tecnico_short_penaliza() -> None:
    df = pd.DataFrame({"close": [3, 2, 1]})
    score, breakdown = calcular_score_tecnico(
        df,
        rsi=40.0,
        momentum=-0.02,
        slope=-0.5,
        tendencia="bajista",
        direccion="short",
    )
    assert score > 0
    assert breakdown.momentum > 0
    assert breakdown.tendencia > 0


def test_decision_trace_to_json() -> None:
    breakdown = ScoreBreakdown(rsi=0.5, momentum=0.3, slope=0.2, tendencia=1.0)
    trace = DecisionTrace(
        score=2.0,
        threshold=1.0,
        reason=DecisionReason.BELOW_THRESHOLD,
        breakdown=breakdown,
    )
    data = json.loads(trace.to_json())
    assert data["score"] == 2.0
    assert data["threshold"] == 1.0
    assert data["reason"] == DecisionReason.BELOW_THRESHOLD.value
    assert data["breakdown"]["rsi"] == 0.5