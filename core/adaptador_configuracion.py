import pandas as pd
from scipy.stats import linregress
from core.logger import configurar_logger

log = configurar_logger("adaptador_configuracion")


def configurar_parametros_dinamicos(symbol: str, df: pd.DataFrame, base_config: dict) -> dict:
    if df is None or len(df) < 10 or "close" not in df.columns:
        log.warning(f"[{symbol}] ❌ Datos insuficientes para adaptar configuración.")
        return base_config

    config = base_config.copy()

    # === Análisis de mercado reciente ===
    cambios = df["close"].pct_change().dropna().tail(10)
    volatilidad = cambios.std() if not cambios.empty else 0

    cierre_reciente = df["close"].tail(10)
    try:
        slope = linregress(range(len(cierre_reciente)), cierre_reciente).slope
    except ValueError:
        slope = 0

    # === Ajustes por volatilidad ===
    config["ajuste_volatilidad"] = round(min(5.0, 1 + volatilidad * 10), 2)
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

    config["sl_ratio"] = round(min(5.0, max(sl_ratio, 0.5)), 2)
    config["tp_ratio"] = round(min(8.0, max(tp_ratio, 1.0)), 2)

    # === Ajuste de peso mínimo total ===
    base_peso = base_config.get("peso_minimo_total", 2.0)
    config["peso_minimo_total"] = round(min(5.0, base_peso * (1 + volatilidad * 1.5)), 2)

    # === Diversidad mínima adaptativa ===
    if slope < -0.002:
        config["diversidad_minima"] = 2
    elif slope > 0.002:
        config["diversidad_minima"] = 1
    else:
        config["diversidad_minima"] = 2

    # === Cooldown tras pérdida ===
    cooldown = min(24, max(0, int(volatilidad * 100)))
    config["cooldown_tras_perdida"] = cooldown

    # === Activar/desactivar modo agresivo ===
    config["modo_agresivo"] = volatilidad > 0.01 or slope > 0.003

    # === Ponderar por diversidad si la diversidad mínima es baja ===
    config["ponderar_por_diversidad"] = config["diversidad_minima"] <= 2

    # === Multiplicador de estrategias recurrentes ===
    base_mult = base_config.get("multiplicador_estrategias_recurrentes", 1.5)
    config["multiplicador_estrategias_recurrentes"] = round(min(3.0, base_mult * (1 + volatilidad)), 2)

    # === Riesgo máximo diario dinámico ===
    base_riesgo = base_config.get("riesgo_maximo_diario", 2.0)
    config["riesgo_maximo_diario"] = round(min(10.0, base_riesgo + volatilidad * 5), 2)

    # === Log de resumen para auditoría ===
    log.info(
        f"[{symbol}] Config adaptada | Volatilidad={volatilidad:.4f} | Slope={slope:.4f} | "
        f"Factor Umbral={config['factor_umbral']} | SL={config['sl_ratio']} | TP={config['tp_ratio']} | "
        f"PesoMin={config['peso_minimo_total']} | Diversidad={config['diversidad_minima']} | "
        f"Cooldown={config['cooldown_tras_perdida']} | Riesgo Diario={config['riesgo_maximo_diario']} | "
        f"Aggresivo={config['modo_agresivo']}")

    return config
