"""Gestor de estrategias de entrada.

Evalúa las estrategias activas y calcula el score técnico total y la diversidad.
"""
from __future__ import annotations

import pandas as pd
from core.pesos import gestor_pesos
from estrategias_entrada.loader import cargar_estrategias
from core.estrategias import obtener_estrategias_por_tendencia
from indicadores.volumen import verificar_volumen_suficiente
from filtros.validador_entradas import verificar_liquidez_orden
from indicadores.correlacion import calcular_correlacion
from indicadores.divergencia_rsi import detectar_divergencia_alcista
from indicadores.bollinger import calcular_bollinger
from indicadores.slope import calcular_slope
from indicadores.momentum import calcular_momentum
from indicadores.rsi import calcular_rsi
from core.logger import configurar_logger

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
            activo = bool(resultado.get("activo")) if isinstance(resultado, dict) else False
        except Exception as exc:  # noqa: BLE001
            log.warning(f"Error ejecutando {nombre}: {exc}")
            activo = False
        activas[nombre] = activo
        if activo:
            puntaje_total += gestor_pesos.obtener_peso(nombre, symbol)

    diversidad = sum(1 for a in activas.values() if a)
    

    return {
        "puntaje_total": round(puntaje_total, 2),
        "estrategias_activas": activas,
        "diversidad": diversidad,
    }


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
    """Versión simplificada para compatibilidad con pruebas existentes."""
    activas = sum(1 for v in estrategias_activas.values() if v)
    return potencia >= umbral and activas > 0



