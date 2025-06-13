"""Mapeo de estrategias a categorías técnicas.

Este módulo define una clasificación simple de las estrategias disponibles
en categorías generales de análisis técnico para facilitar el cálculo de la
diversidad relativa.
"""

from typing import Iterable, Set

# Clasificación manual de las estrategias por categoría
CATEGORIAS_ESTRATEGIAS = {
    # Indicadores de momentum
    "estrategia_momentum": "momentum",
    "estrategia_rsi": "momentum",
    "estrategia_rsi_invertida": "momentum",
    "estrategia_divergencia_rsi": "momentum",
    "estrategia_estocastico": "momentum",
    "estrategia_cruce_ema_stochrsi": "momentum",
    # Estrategias de seguimiento de tendencia
    "estrategia_cruce_medias": "tendencia",
    "cruce_medias_bajista": "tendencia",
    "estrategia_macd": "tendencia",
    "estrategia_macd_hist_inversion": "tendencia",
    "estrategia_sma": "tendencia",
    "estrategia_sma_bajista": "tendencia",
    "estrategia_ema": "tendencia",
    "estrategia_adx": "tendencia",
    # Estrategias tipo breakout/volatilidad
    "estrategia_atr_breakout": "breakout",
    "estrategia_bollinger_breakout": "breakout",
    "estrategia_ichimoku_breakout": "breakout",
    "estrategia_vwap_breakout": "breakout",
    "volatility_breakout": "breakout",
    # Otras categorías
    "estrategia_rango": "rango",
    "estrategia_volumen_alto": "volumen",
    # Patrones chartistas
    "ascending_scallop": "patron",
    "ascending_triangle": "patron",
    "cup_with_handle": "patron",
    "descending_scallop": "patron",
    "descending_triangle": "patron",
    "diamond_bottom": "patron",
    "double_bottom": "patron",
    "flag_alcista": "patron",
    "flag_bajista": "patron",
    "head_and_shoulders": "patron",
    "inverted_cup_with_handle": "patron",
    "measured_move_down": "patron",
    "measured_move_up": "patron",
    "pennant": "patron",
    "pennant_bajista": "patron",
    "symmetrical_triangle_down": "patron",
    "symmetrical_triangle_up": "patron",
    "three_descending_peaks": "patron",
    "three_rising_valleys": "patron",
    "tops_rectangle": "patron",
    "triple_bottom": "patron",
    "triple_top": "patron",
    "wedge_breakout": "patron",
}


def obtener_categoria(estrategia: str) -> str:
    """Devuelve la categoría técnica de una estrategia."""
    return CATEGORIAS_ESTRATEGIAS.get(estrategia, "otras")


def categorias_estrategias(estrategias: Iterable[str]) -> Set[str]:
    """Obtiene el conjunto de categorías presentes en ``estrategias``."""
    return {obtener_categoria(e) for e in estrategias}