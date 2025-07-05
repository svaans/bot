"""Módulo para gestionar persistencia de señales y validaciones de reentrada"""

from __future__ import annotations
import numpy as np
import pandas as pd
from core.utils.utils import configurar_logger
from core.strategies.tendencia import detectar_tendencia
from core.data import coincidencia_parcial, calcular_persistencia_minima
from core.metricas_semanales import metricas_tracker
from core.trader.trader import Trader  # Asegúrate de que la ruta sea correcta

log = configurar_logger("trader")

def evaluar_persistencia(trader: Trader, symbol: str, estado, df: pd.DataFrame,
                         pesos_symbol: dict[str, float], tendencia_actual: str,
                         puntaje: float, umbral: float, estrategias: dict) -> tuple[bool, float, float]:
    ventana_close = df['close'].tail(10)
    media_close = np.mean(ventana_close)
    if np.isnan(media_close) or media_close == 0:
        log.debug(f"⚠️ {symbol}: Media de cierre inválida para persistencia")
        return False, 0, 0

    repetidas = coincidencia_parcial(estado.buffer, pesos_symbol, ventanas=5)
    minimo = calcular_persistencia_minima(symbol, df, tendencia_actual, base_minimo=trader.persistencia.minimo)

    log.info(f"Persistencia detectada {repetidas:.2f} | Mínimo requerido {minimo:.2f}")

    if repetidas < minimo:
        trader._rechazo(symbol, f"Persistencia {repetidas:.2f} < {minimo}",
                        puntaje=puntaje, estrategias=list(estrategias.keys()))
        metricas_tracker.registrar_filtro('persistencia')
        return False, repetidas, minimo
    if repetidas < 1 and puntaje < 1.2 * umbral:
        trader._rechazo(symbol,
                        f"{repetidas:.2f} coincidencia y puntaje débil ({puntaje:.2f})",
                        puntaje=puntaje, estrategias=list(estrategias.keys()))
        return False, repetidas, minimo
    elif repetidas < 1:
        log.info(f"⚠️ Entrada débil en {symbol}: Coincidencia {repetidas:.2f} insuficiente pero puntaje alto ({puntaje}) > Umbral {umbral} — Permitida.")
        metricas_tracker.registrar_filtro('persistencia')
    return True, repetidas, minimo


def tendencia_persistente(trader: Trader, symbol: str, df: pd.DataFrame, tendencia: str, velas: int = 3) -> bool:
    if len(df) < 30 + velas:
        return False
    for i in range(velas):
        sub_df = df.iloc[:-(velas - 1 - i)] if velas - 1 - i > 0 else df
        t, _ = detectar_tendencia(symbol, sub_df)
        if t != tendencia:
            return False
    return True


def validar_reentrada_tendencia(trader: Trader, symbol: str, df: pd.DataFrame, cierre: dict, precio: float) -> bool:
    if cierre.get('motivo') != 'cambio de tendencia':
        return True
    tendencia = cierre.get('tendencia')
    if not tendencia:
        return False
    cierre_dt = pd.to_datetime(cierre.get('timestamp'), errors="coerce")
    if pd.isna(cierre_dt):
        log.warning(f"⚠️ {symbol}: Timestamp de cierre inválido")
        return False
    duracion = cierre.get('duracion', 0)
    retorno = abs(cierre.get('retorno_total', 0))
    velas_requeridas = 3 + min(int(duracion // 30), 3)
    if retorno > 0.05:
        velas_requeridas += 1
    df_post = df[pd.to_datetime(df['timestamp']) > cierre_dt]
    if len(df_post) < velas_requeridas:
        log.info(f"⏳ {symbol}: esperando confirmación de tendencia {len(df_post)}/{velas_requeridas}")
        return False
    if not tendencia_persistente(trader, symbol, df_post, tendencia, velas=velas_requeridas):
        log.info(f"⏳ {symbol}: tendencia {tendencia} no persistente tras cierre")
        return False
    precio_salida = cierre.get('precio')
    if precio_salida is not None and abs(precio - precio_salida) <= precio * 0.001:
        log.info(f"🚫 {symbol}: precio de entrada similar al de salida anterior")
        return False
    return True
