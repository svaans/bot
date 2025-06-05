"""Listas de estrategias agrupadas por tipo de tendencia."""

ESTRATEGIAS_POR_TENDENCIA = {
    "alcista": [
        'ascending_scallop',
        'ascending_triangle',
        'cup_with_handle',
        'double_bottom',
        'estrategia_adx',
        'estrategia_bollinger_breakout',
        'estrategia_cruce_medias',
        'estrategia_ema',
        'estrategia_macd',
        'flag_alcista',
        'measured_move_up',
        'pennant',
        'symmetrical_triangle_up',
        'three_rising_valleys',
        'wedge_breakout',
        'triple_bottom'
    ],
    "bajista": [
        'cruce_medias_bajista',
        'descending_scallop',
        'descending_triangle',
        'diamond_bottom',
        'estrategia_macd_hist_inversion',
        'estrategia_rsi_invertida',
        'estrategia_sma_bajista',
        'flag_bajista',
        'inverted_cup_with_handle',
        'measured_move_down',
        'pennant_bajista',
        'symmetrical_triangle_down',
        'three_descending_peaks',
        'triple_top'
    ],
    "lateral": [
        'estrategia_atr_breakout',
        'estrategia_cruce_ema_stochrsi',
        'estrategia_divergencia_rsi',
        'estrategia_estocastico',
        'estrategia_ichimoku_breakout',
        'estrategia_momentum',
        'estrategia_rango',
        'estrategia_rsi',
        'estrategia_sma',
        'estrategia_volumen_alto',
        'estrategia_vwap_breakout',
        'head_and_shoulders',
        'tops_rectangle',
        'volatility_breakout'
    ]
}


def obtener_estrategias_por_tendencia(tendencia: str) -> list:
    """Devuelve las estrategias recomendadas para una tendencia dada."""
    return ESTRATEGIAS_POR_TENDENCIA.get(tendencia.lower(), [])