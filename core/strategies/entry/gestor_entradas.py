"""Gestor de estrategias de entrada.

Evalúa las estrategias activas y calcula el score técnico total y la diversidad.
"""
from __future__ import annotations
import asyncio
import time
import pandas as pd
from core.utils.metrics_compat import Histogram
from core.strategies.pesos import gestor_pesos
from .loader import cargar_estrategias
from indicadores.correlacion import calcular_correlacion
from core.strategies.entry.validador_entradas import verificar_liquidez_orden
from core.estrategias import obtener_estrategias_por_tendencia, calcular_sinergia
from core.scoring import calcular_score_tecnico
from core.utils import configurar_logger
log = configurar_logger('entradas')
_FUNCIONES = cargar_estrategias()

ENTRADA_EVAL_LATENCY_MS = Histogram(
    "entrada_eval_latency_ms",
    "Latencia de evaluación de cada estrategia de entrada",
    ["symbol", "task_id"],
)


async def evaluar_estrategias(symbol: str, df: pd.DataFrame, tendencia: str) -> dict:
    """Evalúa las estrategias correspondientes a ``tendencia``
    Retorna un diccionario con puntaje_total, estrategias_activas y diversidad.
    """
    global _FUNCIONES
    if not _FUNCIONES:
        _FUNCIONES = cargar_estrategias()
    nombres = obtener_estrategias_por_tendencia(tendencia)
    activas: dict[str, bool] = {}
    puntaje_total = 0.0

    async def ejecutar(nombre: str):
        func = _FUNCIONES.get(nombre)
        if not callable(func):
            log.warning(f'Estrategia no encontrada: {nombre}')
            return nombre, False
        inicio = time.perf_counter()
        try:
            resultado = await asyncio.to_thread(func, df)
            if isinstance(resultado, dict):
                valor = resultado.get('activo')
                if isinstance(valor, pd.Series):
                    activo = bool(valor.iloc[-1])
                else:
                    activo = bool(valor)
            else:
                activo = False
        except Exception as exc:
            log.warning(f'Error ejecutando {nombre}: {exc}')
            activo = False
        finally:
            duracion = (time.perf_counter() - inicio) * 1000
            ENTRADA_EVAL_LATENCY_MS.labels(symbol=symbol, task_id=nombre).observe(duracion)
        return nombre, activo

    tareas = [asyncio.wait_for(ejecutar(n), timeout=5) for n in nombres]
    resultados = await asyncio.gather(*tareas, return_exceptions=True)
    for nombre, resultado in zip(nombres, resultados):
        if isinstance(resultado, Exception):
            if isinstance(resultado, asyncio.TimeoutError):
                log.warning(f'Timeout ejecutando {nombre}')
            else:
                log.warning(f'Error ejecutando {nombre}: {resultado}')
            activas[nombre] = False
            continue
        _, activo = resultado
        activas[nombre] = activo
        if activo:
            puntaje_total += gestor_pesos.obtener_peso(nombre, symbol)
    diversidad = sum(1 for a in activas.values() if a)
    sinergia = calcular_sinergia(activas, tendencia)
    return {
        'puntaje_total': round(puntaje_total, 2),
        'estrategias_activas': activas,
        'diversidad': diversidad,
        'sinergia': sinergia,
    }


def _validar_correlacion(symbol: str, df: pd.DataFrame, df_ref: pd.
    DataFrame, umbral: float) ->bool:
    if df is not None and df_ref is not None and umbral < 1.0:
        correlacion = calcular_correlacion(df, df_ref)
        if correlacion is not None and correlacion >= umbral:
            log.info(
                f'🚫 [{symbol}] Rechazo por correlación {correlacion:.2f} >= {umbral}'
                )
            return False
    return True


def _validar_diversidad(symbol: str, estrategias: dict) ->bool:
    activas = sum(1 for v in estrategias.values() if v)
    if activas <= 0:
        log.info(f'🚫 [{symbol}] Rechazo por falta de estrategias activas')
        return False
    return True


def requiere_ajuste_diversificacion(
    symbol: str,
    score: float,
    abiertas_scores: dict[str, float],
    correlaciones: pd.DataFrame,
    umbral_correlacion: float = 0.8,
) -> bool:
    """Determina si ``symbol`` debe elevar su umbral por correlación.

    Se compara ``score`` con los ``abiertas_scores`` de posiciones ya
    abiertas. Si existe algún símbolo altamente correlacionado cuyo score
    sea mayor o igual, se sugiere elevar el umbral para evitar duplicar
    exposición.
    """
    if not abiertas_scores or correlaciones.empty or symbol not in correlaciones.columns:
        return False
    serie = correlaciones.loc[symbol, list(abiertas_scores.keys())].abs()
    for otro, corr in serie.items():
        if corr >= umbral_correlacion and abiertas_scores.get(otro, 0.0) >= score:
            return True
    return False
    
    
def _validar_score(symbol: str, potencia: float, umbral: float) ->bool:
    if potencia < umbral:
        log.info(
            f'🚫 [{symbol}] Rechazo por score {potencia:.2f} < {umbral:.2f}')
        return False
    if potencia == umbral:
        log.info(
            f'🚫 [{symbol}] Empate en score {potencia:.2f} == {umbral:.2f}')
        return False
    return True


def _validar_volumen(symbol: str, df: pd.DataFrame, cantidad: float) ->bool:
    if df is not None and cantidad > 0:
        if not verificar_liquidez_orden(df, cantidad):
            log.info(
                f'🚫 [{symbol}] Rechazo por volumen insuficiente para {cantidad}'
                )
            return False
    return True


def _validar_capital(symbol: str, capital: float) ->bool:
    if capital <= 0:
        log.info(f'🚫 [{symbol}] Rechazo por capital insuficiente')
        return False
    return True


def _validar_sinergia(symbol: str, sinergia: float, umbral: float) ->bool:
    if sinergia < umbral:
        log.info(
            f'🚫 [{symbol}] Rechazo por sinergia {sinergia:.2f} < {umbral:.2f}'
            )
        return False
    return True


def entrada_permitida(symbol: str, potencia: float, umbral: float,
    estrategias_activas: dict, rsi: float, slope: float, momentum: float,
    df=None, direccion: str='long', cantidad: float=0.0, df_referencia=None,
    umbral_correlacion: float=0.9, tendencia: (str | None)=None, score: (
    float | None)=None, persistencia: float=0.0, persistencia_minima: float=0.0,
    capital_disponible: float=0.0, sinergia: float=1.0, umbral_sinergia: float=0.5
    ) ->bool:
    """Versión simplificada usada en las pruebas unitarias."""
    score_tecnico = score if score is not None else calcular_score_tecnico(
        df if df is not None else pd.DataFrame(), rsi, momentum, slope,
        tendencia or 'lateral', direccion
    )[0]
    assert 0.0 <= sinergia <= 1.0, 'sinergia fuera de rango'
    potencia_ajustada = potencia * (1 + score_tecnico / 3)
    if not _validar_correlacion(symbol, df, df_referencia, umbral_correlacion):
        return False
    if not _validar_diversidad(symbol, estrategias_activas):
        return False
    if not _validar_volumen(symbol, df, cantidad):
        return False
    if not _validar_capital(symbol, capital_disponible):
        return False
    if not _validar_score(symbol, potencia_ajustada, umbral):
        return False
    if not _validar_sinergia(symbol, sinergia, umbral_sinergia):
        return False
    return True
