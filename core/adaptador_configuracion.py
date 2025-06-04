import pandas as pd
from scipy.stats import linregress


def configurar_parametros_dinamicos(symbol: str, df: pd.DataFrame, base_config: dict) -> dict:
    if df is None or len(df) < 10:
        return base_config

    config = base_config.copy()

    # === Análisis de mercado reciente ===
    cambios = df["close"].pct_change().dropna().tail(10)
    volatilidad = cambios.std() if not cambios.empty else 0

    cierre_reciente = df["close"].tail(10)
    try:
        slope = linregress(range(len(cierre_reciente)), cierre_reciente).slope
    except Exception:
        slope = 0

    # === Ajustes por volatilidad ===
    config["ajuste_volatilidad"] = round(1 + volatilidad * 10, 2)
    factor_dinamico = base_config.get("factor_umbral", 1.0) * (1 + volatilidad)
    config["factor_umbral"] = round(min(3.0, max(0.3, factor_dinamico)), 2)

    # === Ajuste SL/TP ===
    sl_ratio = base_config.get("sl_ratio", 2.0)
    tp_ratio = base_config.get("tp_ratio", 4.0)

    if slope < -0.001:
        sl_ratio *= 1.1
        tp_ratio *= 0.9
    elif slope > 0.001:
        sl_ratio *= 0.9
        tp_ratio *= 1.1

    config["sl_ratio"] = round(max(sl_ratio, 0.5), 2)
    config["tp_ratio"] = round(max(tp_ratio, 1.0), 2)

    # === Ajuste de peso mínimo total ===
    # En mercado muy volátil exigimos más peso
    base_peso = base_config.get("peso_minimo_total", 2.0)
    config["peso_minimo_total"] = round(base_peso * (1 + volatilidad * 1.5), 2)

    # === Diversidad mínima adaptativa ===
    if slope < -0.002:  # mercado muy bajista
        config["diversidad_minima"] = 4
    elif slope > 0.002:
        config["diversidad_minima"] = 2
    else:
        config["diversidad_minima"] = 3

    # === Cooldown tras pérdida ===
    # En mercados volátiles, más cooldown para evitar entradas seguidas
    config["cooldown_tras_perdida"] = min(24, max(0, int(volatilidad * 100)))

    # === Activar/desactivar modo agresivo ===
    config["modo_agresivo"] = volatilidad > 0.01 or slope > 0.003

    # === Ponderar por diversidad si hay baja diversidad reciente ===
    config["ponderar_por_diversidad"] = True if config["diversidad_minima"] <= 2 else False

    # === Multiplicador de estrategias recurrentes adaptado ===
    base_mult = base_config.get("multiplicador_estrategias_recurrentes", 1.5)
    config["multiplicador_estrategias_recurrentes"] = round(base_mult * (1 + volatilidad), 2)

    # === Riesgo máximo diario dinámico ===
    base_riesgo = base_config.get("riesgo_maximo_diario", 2.0)
    config["riesgo_maximo_diario"] = round(min(10.0, base_riesgo + volatilidad * 5), 2)

    return config