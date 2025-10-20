import os
import time
import pandas as pd
from collections import defaultdict
from typing import Dict, Iterable, Mapping, Tuple
from dotenv import dotenv_values
from core.strategies.pesos import gestor_pesos
from core.utils.utils import configurar_logger
from .historial_operaciones import HistorialOperaciones, cargar_historial_operaciones
from .utils_resultados import distribuir_retorno_por_estrategia, obtener_retorno_total_registro
CONFIG = dotenv_values('config/claves.env')
MODO_REAL = CONFIG.get('MODO_REAL', 'False') == 'True'
RUTA_PESOS = 'config/estrategias_pesos.json'
MIN_OPERACIONES = 5
log = configurar_logger('entrenador_estrategias')


def evaluar_estrategias(ordenes: pd.DataFrame):
    datos = defaultdict(list)
    for _, orden in ordenes.iterrows():
        retorno = obtener_retorno_total_registro(orden)
        contribuciones = distribuir_retorno_por_estrategia(retorno, orden.get(
            'estrategias_activas', {}))
        for estrategia, retorno_parcial in contribuciones.items():
            datos[estrategia].append(retorno_parcial)
    return datos


def normalizar_scores(scores):
    max_score = max(scores.values(), default=1)
    return {k: (v / max_score) for k, v in scores.items()}


def _resumir_metricas(retornos: Iterable[float]) -> Dict[str, float]:
    retornos = list(retornos)
    if not retornos:
        return {'n': 0, 'promedio': 0.0, 'winrate': 0.0}
    n = len(retornos)
    promedio = sum(retornos) / n
    winrate = sum(1 for r in retornos if r > 0) / n
    return {'n': n, 'promedio': promedio, 'winrate': winrate}


def calcular_pesos_suavizados(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    pesos_actuales: Mapping[str, float],
    factor_suavizado: float,
    minimo_operaciones: int = MIN_OPERACIONES,
) -> Tuple[Dict[str, float] | None, Dict[str, Dict[str, float]]]:
    """Calcula pesos suavizados con penalización basada en validación.

    Retorna una tupla ``(pesos_actualizados, metricas_test)`` donde
    ``pesos_actualizados`` puede ser ``None`` si no se producen cambios.
    ``metricas_test`` expone las métricas agregadas por estrategia en el
    conjunto de validación.
    """

    datos_estrategias = evaluar_estrategias(train_df)
    nuevos_scores = {}
    for estrategia, retornos in datos_estrategias.items():
        if len(retornos) < minimo_operaciones:
            continue
        promedio = sum(retornos) / len(retornos)
        winrate = sum(1 for r in retornos if r > 0) / len(retornos)
        score = promedio * winrate * 100
        if score > 0:
            nuevos_scores[estrategia] = score

    metricas_test = {
        estrategia: _resumir_metricas(retornos)
        for estrategia, retornos in evaluar_estrategias(test_df).items()
        if retornos
    }

    if not nuevos_scores and not metricas_test:
        return None, metricas_test

    nuevos_scores_normalizados = normalizar_scores(nuevos_scores) if nuevos_scores else {}
    umbral_validacion = max(1, minimo_operaciones // 2)
    pesos_suavizados: Dict[str, float] = dict(pesos_actuales)
    cambios = False

    for estrategia, score in nuevos_scores_normalizados.items():
        peso_actual = float(pesos_actuales.get(estrategia, 0.5))
        metrica_test = metricas_test.get(estrategia)
        if (
            metrica_test
            and metrica_test['n'] >= umbral_validacion
            and metrica_test['promedio'] <= 0
        ):
            peso_nuevo = peso_actual * (1 - factor_suavizado)
        else:
            peso_nuevo = peso_actual * (1 - factor_suavizado) + score * factor_suavizado
        if abs(peso_nuevo - peso_actual) > 1e-9:
            cambios = True
        pesos_suavizados[estrategia] = peso_nuevo

    # Penaliza estrategias existentes con mal desempeño en validación aunque no hayan generado score nuevo.
    for estrategia, metrica_test in metricas_test.items():
        if estrategia in nuevos_scores_normalizados:
            continue
        if metrica_test['n'] < umbral_validacion or metrica_test['promedio'] > 0:
            continue
        peso_actual = float(pesos_actuales.get(estrategia, 0.5))
        peso_nuevo = peso_actual * (1 - factor_suavizado)
        if abs(peso_nuevo - peso_actual) > 1e-9:
            cambios = True
        pesos_suavizados[estrategia] = peso_nuevo

    return (pesos_suavizados if cambios else None), metricas_test


def dividir_train_test(df: pd.DataFrame, test_ratio: float=0.2):
    """Divide el DataFrame en particiones de entrenamiento y prueba."""
    if df.empty:
        return df, pd.DataFrame()
    n_test = max(1, int(len(df) * test_ratio))
    df_train = df.iloc[:-n_test]
    df_test = df.iloc[-n_test:]
    return df_train, df_test


def _cargar_historial(symbol: str) -> HistorialOperaciones | None:
    """Obtiene el historial consolidado para ``symbol`` respetando reintentos."""

    errores: list[str] = []
    for intento in range(3):
        try:
            return cargar_historial_operaciones(symbol, max_operaciones=500)
        except FileNotFoundError:
            time.sleep(0.3)
        except RuntimeError as exc:
            errores.append(str(exc))
            break
    if errores:
        log.error(
            '❌ Error al cargar historial consolidado',
            extra={'symbol': symbol, 'errores': errores},
        )
    else:
        log.info(
            '⚠️ No se encontró historial reciente', extra={'symbol': symbol}
        )
    return None


def actualizar_pesos_estrategias_symbol(symbol: str):
    FACTOR_SUAVIZADO = 0.02
    historial = _cargar_historial(symbol)
    if historial is None:
        return
    ordenes = historial.data
    if len(ordenes) < MIN_OPERACIONES:
        log.info(f'⚠️ Insuficientes operaciones para {symbol}.')
        return
    train_df, test_df = dividir_train_test(ordenes)
    if 'retorno_total' in train_df.columns:
        vol_ret = train_df['retorno_total'].std() or 0.0
    else:
        vol_ret = 0.0
    FACTOR_SUAVIZADO = max(0.01, min(0.05, 0.02 + vol_ret * 0.1))
    pesos_totales = gestor_pesos.pesos
    pesos_actuales = pesos_totales.get(symbol, {})
    pesos_suavizados, metricas_test = calcular_pesos_suavizados(
        train_df,
        test_df,
        pesos_actuales,
        FACTOR_SUAVIZADO,
        minimo_operaciones=MIN_OPERACIONES,
    )
    if pesos_suavizados is None:
        log.info(f'⚠️ No se generaron ajustes para {symbol}.')
        return
    pesos_totales[symbol] = pesos_suavizados
    gestor_pesos.pesos = pesos_totales
    gestor_pesos.guardar()
    origen = os.path.relpath(str(historial.source), os.getcwd())
    print(
        f"✅ Pesos suavizados para {symbol} en modo {'REAL' if MODO_REAL else 'SIMULADO'} "
        f"(datos: {origen}):"
    )
    for estrategia, peso in pesos_suavizados.items():
        print(f'  - {estrategia}: {peso:.3f}')
    if metricas_test:
        resumen = []
        for estrategia, metricas in metricas_test.items():
            resumen.append(
                f"{estrategia}: retorno={metricas['promedio']:.3f}, winrate={metricas['winrate'] * 100:.1f}% (n={metricas['n']})"
            )
        log.info(f"📊 Validación {symbol}: {'; '.join(resumen)}")
