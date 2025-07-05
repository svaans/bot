"""Módulo para cálculo y validación de score técnico"""

from __future__ import annotations
import pandas as pd
import os
from datetime import datetime
from core.utils.utils import configurar_logger
from core.scoring import calcular_score_tecnico
from indicators.rsi import calcular_rsi
from indicators.momentum import calcular_momentum
from indicators.slope import calcular_slope
from core.metricas_semanales import metricas_tracker
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.trader.trader import Trader

log = configurar_logger("trader")

# pesos fijos
PESOS_SCORE_TECNICO = {
    "RSI": 1.0,
    "Momentum": 0.5,
    "Slope": 1.0,
    "Tendencia": 1.0
}

def calcular_score_tecnico_trader(trader: Trader, df: pd.DataFrame, rsi: float | None,
                                  momentum: float | None, tendencia: str, direccion: str) -> tuple[float, dict]:
    slope = calcular_slope(df)
    score_indicadores = calcular_score_tecnico(df, rsi, momentum, slope, tendencia)

    resultados = {
        "RSI": False,
        "Momentum": False,
        "Slope": False,
        "Tendencia": False
    }

    if rsi is not None:
        if direccion == "long":
            resultados["RSI"] = rsi > 50 or 45 <= rsi <= 55
        else:
            resultados["RSI"] = rsi < 50 or 45 <= rsi <= 55
    if momentum is not None:
        resultados["Momentum"] = momentum > 0
    if slope is not None:
        if direccion == "long":
            resultados["Slope"] = slope > 0
        else:
            resultados["Slope"] = slope < 0
    if direccion == "long":
        resultados["Tendencia"] = tendencia in {"alcista", "lateral"}
    else:
        resultados["Tendencia"] = tendencia in {"bajista", "lateral"}

    score_total = score_indicadores
    if resultados["Tendencia"]:
        score_total += PESOS_SCORE_TECNICO.get("Tendencia", 1.0)

    log.info(
        f"📊 Score técnico: {score_total:.2f} | "
        f"RSI: {'✅' if resultados['RSI'] else '❌'} ({rsi if rsi is not None else 0.0}), "
        f"Momentum: {'✅' if resultados['Momentum'] else '❌'} ({momentum if momentum is not None else 0.0}), "
        f"Slope: {'✅' if resultados['Slope'] else '❌'} ({slope:.4f if slope else 0.0}), "
        f"Tendencia: {'✅' if resultados['Tendencia'] else '❌'}"
    )

    return float(score_total), resultados


def validar_puntaje(trader: Trader, symbol: str, puntaje: float, umbral: float) -> bool:
    diferencia = umbral - puntaje
    metricas_tracker.registrar_diferencia_umbral(diferencia)
    if puntaje < umbral:
        log.debug(f"🚫 {symbol}: puntaje {puntaje:.2f} < umbral {umbral:.2f}")
        metricas_tracker.registrar_filtro('umbral')
        return False
    return True


def registrar_rechazo_tecnico(trader: Trader, symbol: str, score: float, puntos: dict,
                              tendencia: str, precio: float, motivo: str, estrategias: dict | None = None) -> None:
    if not trader.registro_tecnico_csv:
        return
    fila = {
        "timestamp": datetime.utcnow().isoformat(),
        "symbol": symbol,
        "puntaje_total": score,
        "indicadores_fallidos": ",".join([k for k, v in puntos.items() if not v]),
        "estado_mercado": tendencia,
        "precio": precio,
        "motivo": motivo,
        "estrategias": ",".join(estrategias.keys()) if estrategias else ""
    }
    modo = "a" if os.path.exists(trader.registro_tecnico_csv) else "w"
    df = pd.DataFrame([fila])
    df.to_csv(trader.registro_tecnico_csv, mode=modo, header=not os.path.exists(trader.registro_tecnico_csv), index=False)


def hay_contradicciones(trader: Trader, df: pd.DataFrame, rsi: float | None,
                        momentum: float | None, direccion: str, score: float) -> bool:
    if direccion == "long":
        if rsi is not None and rsi > 70:
            return True
        if df['close'].iloc[-1] >= df['close'].iloc[-10] * 1.05:
            return True
        if momentum is not None and momentum < 0 and score >= trader.umbral_score_tecnico:
            return True
    else:
        if rsi is not None and rsi < 30:
            return True
        if df['close'].iloc[-1] <= df['close'].iloc[-10] * 0.95:
            return True
        if momentum is not None and momentum > 0 and score >= trader.umbral_score_tecnico:
            return True
    return False


def validar_temporalidad(df: pd.DataFrame, direccion: str) -> bool:
    rsi_series = calcular_rsi(df, serie_completa=True)
    if rsi_series is None or len(rsi_series) < 3:
        return True
    r = rsi_series.iloc[-3:]
    if direccion == "long" and not (r.iloc[-1] > r.iloc[-2] > r.iloc[-3]):
        return False
    if direccion == "short" and not (r.iloc[-1] < r.iloc[-2] < r.iloc[-3]):
        return False
    slope3 = calcular_slope(df, periodo=3)
    slope5 = calcular_slope(df, periodo=5)
    if direccion == "long" and not slope3 > slope5:
        return False
    if direccion == "short" and not slope3 < slope5:
        return False
    return True
