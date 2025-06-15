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
# Clasificación adicional por régimen de volatilidad
ESTRATEGIAS_POR_REGIMEN = {
    "alta_volatilidad": [
        'estrategia_atr_breakout',
        'estrategia_bollinger_breakout',
        'estrategia_ichimoku_breakout',
        'estrategia_vwap_breakout',
        'volatility_breakout',
        'estrategia_rango',
        'estrategia_momentum',
        'flag_alcista',
        'flag_bajista',
        'measured_move_up',
        'measured_move_down',
        'three_rising_valleys',
        'three_descending_peaks',
    ],
    "baja_volatilidad": [
        'pennant',
        'pennant_bajista',
        'flag_alcista',
        'flag_bajista',
        'cup_with_handle',
        'double_bottom',
        'triple_bottom',
        'triple_top',
        'wedge_breakout',
        'ascending_scallop',
        'descending_scallop',
        'diamond_bottom',
        'inverted_cup_with_handle',
        'tops_rectangle'
    ],
    "tendencial": [
        'estrategia_cruce_medias',
        'cruce_medias_bajista',
        'estrategia_macd',
        'estrategia_macd_hist_inversion',
        'estrategia_sma',
        'estrategia_sma_bajista',
        'estrategia_adx',
        'estrategia_ema',
        'ascending_triangle',
        'descending_triangle',
        'symmetrical_triangle_up',
        'symmetrical_triangle_down',
        'measured_move_up',
        'measured_move_down',
        'three_rising_valleys',
        'three_descending_peaks',
    ],
    "lateral": [
        'estrategia_rsi',
        'estrategia_divergencia_rsi',
        'estrategia_estocastico',
        'estrategia_cruce_ema_stochrsi',
        'estrategia_momentum',
        'estrategia_sma',
        'estrategia_volumen_alto',
        'head_and_shoulders',
        'estrategia_rango',
        'estrategia_vwap_breakout',
        'estrategia_ichimoku_breakout',
        'tops_rectangle'
    ]
}


# Mapeo de estrategia → tendencia ideal
TENDENCIA_IDEAL = {
    nombre: tendencia
    for tendencia, lista in ESTRATEGIAS_POR_TENDENCIA.items()
    for nombre in lista
}


def obtener_estrategias_por_tendencia(tendencia: str) -> list:
    """Devuelve las estrategias recomendadas para una tendencia dada."""
    return ESTRATEGIAS_POR_TENDENCIA.get(tendencia.lower(), [])


def filtrar_por_direccion(estrategias: dict, direccion: str) -> tuple[dict, list[str]]:
    """Filtra ``estrategias`` según la coherencia con ``direccion``."""
    direccion = direccion.lower()
    coherentes = {}
    incoherentes: list[str] = []

    for nombre, activo in estrategias.items():
        ideal = TENDENCIA_IDEAL.get(nombre, "lateral")
        if direccion == "long" and ideal == "bajista":
            incoherentes.append(nombre)
            continue
        if direccion == "short" and ideal == "alcista":
            incoherentes.append(nombre)
            continue
        coherentes[nombre] = activo

    return coherentes, incoherentes


def filtrar_por_regimen(estrategias: dict, regimen: str) -> dict:
    """Conserva solo las estrategias compatibles con ``regimen``."""
    permitidas = set(ESTRATEGIAS_POR_REGIMEN.get(regimen, []))
    if not permitidas:
        return estrategias
    return {e: act for e, act in estrategias.items() if e in permitidas}