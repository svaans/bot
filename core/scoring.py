"""Funciones de scoring técnico."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional
import json
import pandas as pd


PESOS_SCORE_TECNICO = {
    "RSI": 1.0,
    "Momentum": 0.5,
    "Slope": 1.0,
    "Tendencia": 1.0,
}


@dataclass
class ScoreBreakdown:
    """Detalle de cada contribución al puntaje técnico."""

    rsi: float = 0.0
    momentum: float = 0.0
    slope: float = 0.0
    tendencia: float = 0.0

    @property
    def total(self) -> float:  # pragma: no cover - simple suma
        return self.rsi + self.momentum + self.slope + self.tendencia

    def to_dict(self) -> dict:  # pragma: no cover - simple conversión
        return asdict(self)


class DecisionReason(str, Enum):
    """Motivos estandarizados de decisión."""

    BELOW_THRESHOLD = "BELOW_THRESHOLD"
    NO_CAPITAL = "NO_CAPITAL"
    MAX_PORTFOLIO_EXPOSURE = "MAX_PORTFOLIO_EXPOSURE"
    TECH_INVALID = "TECH_INVALID"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class DecisionTrace:
    """Estructura de logging determinista para decisiones."""

    score: float
    threshold: float
    reason: DecisionReason
    breakdown: ScoreBreakdown

    def to_json(self) -> str:
        """Serializa la traza en JSON ordenado para idempotencia."""
        payload = {
            "score": round(self.score, 6),
            "threshold": round(self.threshold, 6),
            "reason": self.reason.value,
            "breakdown": {
                "rsi": round(self.breakdown.rsi, 6),
                "momentum": round(self.breakdown.momentum, 6),
                "slope": round(self.breakdown.slope, 6),
                "tendencia": round(self.breakdown.tendencia, 6),
            },
        }
        return json.dumps(payload, sort_keys=True)


def calcular_score_tecnico(
    df: pd.DataFrame,
    rsi: Optional[float],
    momentum: Optional[float],
    slope: Optional[float],
    tendencia: str,
    direccion: str = "long",
) -> tuple[float, ScoreBreakdown]:
    """Agrega RSI, momentum, slope y tendencia ponderados para obtener un score técnico."""

    bd = ScoreBreakdown()
    peso_rsi = PESOS_SCORE_TECNICO.get("RSI", 1.0)
    if rsi is not None:
        if tendencia == "alcista" and rsi > 50:
            bd.rsi = peso_rsi
        elif tendencia == "bajista" and rsi < 50:
            bd.rsi = peso_rsi
        elif 45 <= rsi <= 55:
            bd.rsi = peso_rsi * 0.5
    umbral_mom = 0.001
    if momentum is not None:
        peso_mom = PESOS_SCORE_TECNICO.get("Momentum", 1.0)
        if direccion == "long" and momentum > umbral_mom:
            bd.momentum = peso_mom
        elif direccion == "short" and momentum < -umbral_mom:
            bd.momentum = peso_mom
    if slope is not None:
        peso_slope = PESOS_SCORE_TECNICO.get("Slope", 1.0)
        if tendencia == "alcista" and slope > 0:
            bd.slope = peso_slope
        elif tendencia == "bajista" and slope < 0:
            bd.slope = peso_slope

    peso_tend = PESOS_SCORE_TECNICO.get("Tendencia", 1.0)
    if direccion == "long" and tendencia == "alcista":
        bd.tendencia = peso_tend
    elif direccion == "short" and tendencia == "bajista":
        bd.tendencia = peso_tend

    return bd.total, bd
    
class TechnicalScorer:
    """Utilidad para calcular el puntaje técnico de forma orientada a objetos."""

    @staticmethod
    def calcular(
        df: pd.DataFrame,
        rsi: Optional[float],
        momentum: Optional[float],
        slope: Optional[float],
        tendencia: str,
        direccion: str = "long",
    ) -> tuple[float, ScoreBreakdown]:
        """Delegado estático al cálculo procedural."""
        return calcular_score_tecnico(df, rsi, momentum, slope, tendencia, direccion)

