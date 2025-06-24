"""Gestor de estrategias de entrada.

Evalúa las estrategias activas y calcula el score técnico total y la diversidad.
"""
from __future__ import annotations

import pandas as pd
from core.strategies.pesos import gestor_pesos
from .loader import cargar_estrategias
from core.validaciones_comunes import (
    validar_correlacion,
    validar_diversidad,
    validar_volumen,
)
from core.estrategias import (
    obtener_estrategias_por_tendencia,
    calcular_sinergia,
)
from core.score_tecnico import calcular_score_tecnico
from core.utils import configurar_logger

log = configurar_logger("entradas")


_FUNCIONES = cargar_estrategias()

def evaluar_estrategias(symbol: str, df: pd.DataFrame, tendencia: str) -> dict:
    """Evalúa las estrategias correspondientes a ``tendencia``
    Retorna un diccionario con puntaje_total, estrategias_activas y diversidad.
    """
    global _FUNCIONES
    if not _FUNCIONES:
        _FUNCIONES = cargar_estrategias()

    nombres = obtener_estrategias_por_tendencia(tendencia)
    activas: dict[str, bool] = {}
    puntaje_total = 0.0

    for nombre in nombres:
        func = _FUNCIONES.get(nombre)
        if not callable(func):
            log.warning(f"Estrategia no encontrada: {nombre}")
            continue

        try:
            resultado = func(df)
            activo = (
                bool(resultado.get("activo")) if isinstance(resultado, dict) else False
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(f"Error ejecutando {nombre}: {exc}")
            activo = False
        activas[nombre] = activo
        if activo:
            puntaje_total += gestor_pesos.obtener_peso(nombre, symbol)

    diversidad = sum(1 for a in activas.values() if a)
    sinergia = calcular_sinergia(activas, tendencia)
    

    return {
        "puntaje_total": round(puntaje_total, 2),
        "estrategias_activas": activas,
        "diversidad": diversidad,
        "sinergia": sinergia,
    }


def _validar_score(symbol: str, potencia: float, umbral: float) -> bool:
    if potencia < umbral:
        log.info(f"🚫 [{symbol}] Rechazo por score {potencia:.2f} < {umbral:.2f}")
        return False
    return True


def entrada_permitida(
    symbol: str,
    potencia: float,
    umbral: float,
    estrategias_activas: dict,
    rsi: float,
    slope: float,
    momentum: float,
    df=None,
    direccion: str = "long",
    cantidad: float = 0.0,
    df_referencia=None,
    umbral_correlacion: float = 0.9,
    tendencia: str | None = None,
    score: float | None = None,
    persistencia: float = 0.0,
    persistencia_minima: float = 0.0,
) -> bool:
    """Versión simplificada usada en las pruebas unitarias."""
    score_tecnico = (
        score
        if score is not None
        else calcular_score_tecnico(
            df if df is not None else pd.DataFrame(),
            rsi,
            momentum,
            slope,
            tendencia or "lateral",
        )
    )
    potencia_ajustada = potencia * (1 + score_tecnico / 3)

    if not validar_correlacion(symbol, df, df_referencia, umbral_correlacion):
        return False
    if not validar_diversidad(symbol, estrategias_activas):
        return False
    if not _validar_score(symbol, potencia_ajustada, umbral):
        return False
    if not validar_volumen(symbol, df, cantidad):
        return False
    return True
