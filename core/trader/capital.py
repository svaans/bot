"""Módulo de gestión de capital y redistribución adaptativa"""

from __future__ import annotations
from typing import Dict
import pandas as pd
import os
from datetime import datetime, timedelta
from core.utils.utils import configurar_logger
from core.reporting import reporter_diario
from core.metricas_semanales import metricas_semanales
from core.trader.trader import Trader  # Asegúrate de que la ruta de importación sea correcta

log = configurar_logger("trader")

def ajustar_capital_diario(trader: Trader, factor: float = 0.2, limite: float = 0.3,
                           penalizacion_corr: float = 0.2, umbral_corr: float = 0.8,
                           fecha: datetime.date | None = None) -> None:
    """
    Redistribuye el capital según múltiples métricas adaptativas.
    """
    total = sum(trader.capital_por_simbolo.values())
    metricas_globales = _metricas_recientes(trader)
    semanales = metricas_semanales()
    pesos: dict[str, float] = {}
    senales = {s: trader._contar_senales(s) for s in trader.capital_por_simbolo}
    max_senales = max(senales.values()) if senales else 0
    correlaciones = trader._calcular_correlaciones()
    stats = getattr(reporter_diario, 'estadisticas', pd.DataFrame())

    for symbol in trader.capital_por_simbolo:
        inicio = trader.capital_inicial_diario.get(symbol, trader.capital_por_simbolo[symbol])
        final = trader.capital_por_simbolo[symbol]
        rendimiento = (final - inicio) / inicio if inicio else 0.0
        peso = 1 + factor * rendimiento
        if max_senales > 0:
            peso += 0.2 * senales[symbol] / max_senales
        corr_media = None
        if not correlaciones.empty and symbol in correlaciones.columns:
            corr_series = correlaciones[symbol].drop(labels=[symbol], errors="ignore").abs()
            corr_media = corr_series.mean()
        if corr_media and corr_media >= umbral_corr:
            peso *= 1 - penalizacion_corr * corr_media

        fila = stats[stats['symbol'] == symbol] if isinstance(stats, pd.DataFrame) and 'symbol' in stats.columns else pd.DataFrame()
        drawdown = 0.0
        winrate = 0.0
        ganancia = 0.0
        if not fila.empty:
            drawdown = float(fila['drawdown'].iloc[0])
            operaciones = float(fila['operaciones'].iloc[0])
            wins = float(fila['wins'].iloc[0])
            ganancia = float(fila['retorno_acumulado'].iloc[0])
            winrate = wins / operaciones if operaciones else 0.0
        if not semanales.empty:
            sem = semanales[semanales['symbol'] == symbol]
            if not sem.empty:
                weekly = float(sem['ganancia_promedio'].iloc[0]) * float(sem['operaciones'].iloc[0])
                if weekly < -0.05:
                    peso *= 0.5
        if drawdown < 0:
            peso *= 1 + drawdown
        if winrate > 0.6 and ganancia > 0:
            refuerzo = min((winrate - 0.6) * ganancia, 0.3)
            peso *= 1 + refuerzo
        if metricas_globales:
            ganancia_global = metricas_globales.get('ganancia_semana', 0.0)
            drawdown_global = metricas_globales.get('drawdown', 0.0)
            ajuste_global = 1 + ganancia_global + drawdown_global
            peso *= max(0.5, min(1.5, ajuste_global))
        peso = max(1 - limite, min(1 + limite, peso))
        pesos[symbol] = peso

    suma = sum(pesos.values()) or 1
    for symbol in trader.capital_por_simbolo:
        trader.capital_por_simbolo[symbol] = round(total * pesos[symbol] / suma, 2)

    for symbol in trader.capital_por_simbolo:
        orden = trader.orders.obtener(symbol)
        reserva = 0.0
        if orden and orden.cantidad_abierta > 0 and trader.estado[symbol].buffer:
            precio_actual = float(trader.estado[symbol].buffer[-1].get('close', 0))
            if precio_actual > orden.precio_entrada:
                reserva = trader.capital_por_simbolo[symbol] * trader.reserva_piramide
        trader.capital_por_simbolo[symbol] -= reserva
        trader.reservas_piramide[symbol] = round(reserva, 2)

    trader.capital_inicial_diario = trader.capital_por_simbolo.copy()
    trader.fecha_actual = fecha or datetime.utcnow().date()
    log.info(f'💰 Capital redistribuido: {trader.capital_por_simbolo}')


def _metricas_recientes(trader: Trader, dias: int = 7) -> dict:
    carpeta = reporter_diario.carpeta
    if not os.path.isdir(carpeta):
        return {
            'ganancia_semana': 0.0,
            'drawdown': 0.0,
            'winrate': 0.0,
            'capital_actual': sum(trader.capital_por_simbolo.values()),
            'capital_inicial': sum(trader.capital_inicial_diario.values())
        }
    fecha_limite = datetime.utcnow().date() - timedelta(days=dias)
    retornos: list[float] = []
    archivos = sorted([f for f in os.listdir(carpeta) if f.endswith('.csv')], reverse=True)[:20]
    for archivo in archivos:
        try:
            fecha = datetime.fromisoformat(archivo.replace('.csv', '')).date()
        except ValueError:
            continue
        if fecha < fecha_limite:
            continue
        ruta_archivo = os.path.join(carpeta, archivo)
        df = pd.read_csv(ruta_archivo)
        if df.empty:
            continue
        if 'retorno_total' in df.columns:
            retornos.extend(df['retorno_total'].dropna().tolist())

    if not retornos:
        return {
            'ganancia_semana': 0.0,
            'drawdown': 0.0,
            'winrate': 0.0,
            'capital_actual': sum(trader.capital_por_simbolo.values()),
            'capital_inicial': sum(trader.capital_inicial_diario.values())
        }

    serie = pd.Series(retornos).cumsum()
    drawdown = float((serie - serie.cummax()).min())
    ganancia = float(serie.iloc[-1])
    return {
        'ganancia_semana': ganancia,
        'drawdown': drawdown
    }
