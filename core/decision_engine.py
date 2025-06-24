"""Engine para decisiones basadas en pesos y umbrales adaptativos."""

from __future__ import annotations

import pandas as pd

from core.adaptador_umbral import calcular_umbral_adaptativo, calcular_umbral_salida_adaptativo
from core.strategies.pesos import gestor_pesos, obtener_peso_salida
from core.utils import configurar_logger

log = configurar_logger("decision_engine", modo_silencioso=True)


def sumar_pesos(estrategias: dict[str, bool], symbol: str) -> float:
    """Suma los pesos de ``estrategias`` activas para ``symbol``."""
    return sum(
        gestor_pesos.obtener_peso(nombre, symbol)
        for nombre, activo in estrategias.items()
        if activo
    )


def _contexto_entrada(symbol: str, estrategias: dict[str, bool], contexto: dict | None) -> dict:
    ctx = {
        "estrategias_activas": estrategias,
        "pesos_symbol": gestor_pesos.obtener_pesos_symbol(symbol),
    }
    if contexto:
        ctx.update(contexto)
    return ctx


def decidir_entrada(
    symbol: str,
    df: pd.DataFrame,
    estrategias: dict[str, bool],
    config: dict | None = None,
    contexto: dict | None = None,
) -> dict:
    """Evalúa si el puntaje de entrada supera el umbral adaptativo."""
    score = sumar_pesos(estrategias, symbol)
    ctx = _contexto_entrada(symbol, estrategias, contexto)
    umbral = calcular_umbral_adaptativo(symbol, df, ctx)
    factor = (config or {}).get("factor_umbral_entrada", 1.0)
    umbral *= factor
    permitido = score >= umbral
    log.debug(f"[{symbol}] Score entrada {score:.2f} vs Umbral {umbral:.2f}")
    return {"permitido": permitido, "score": round(score, 2), "umbral": round(umbral, 2)}


def sumar_pesos_salidas(razones: list[str], symbol: str) -> float:
    """Suma los pesos de las razones de salida registradas."""
    return sum(obtener_peso_salida(r, symbol) for r in razones)


def _contexto_salida(razones: list[str], contexto: dict | None) -> dict:
    ctx = {"estrategias_activas": {r: True for r in razones}}
    if contexto:
        ctx.update(contexto)
    return ctx


def decidir_salida(
    symbol: str,
    df: pd.DataFrame,
    razones: list[str],
    config: dict | None = None,
    contexto: dict | None = None,
) -> dict:
    """Determina si debe cerrarse la posición en base a un umbral adaptativo."""
    score = sumar_pesos_salidas(razones, symbol)
    ctx = _contexto_salida(razones, contexto)
    umbral = calcular_umbral_salida_adaptativo(symbol, df, config or {}, ctx)
    factor = (config or {}).get("factor_umbral_salida", 1.0)
    umbral *= factor
    cerrar = score >= umbral
    log.debug(f"[{symbol}] Score salida {score:.2f} vs Umbral {umbral:.2f}")
    return {"cerrar": cerrar, "score": round(score, 2), "umbral": round(umbral, 2)}


def superar_umbral(puntaje: float, umbral: float) -> bool:
    """Comparación genérica de puntaje contra umbral."""
    return puntaje >= umbral