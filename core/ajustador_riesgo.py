"""Utilidades compartidas para ajustar ratios de SL/TP, riesgo y modo agresivo."""

from __future__ import annotations

from core.utils.utils import configurar_logger

# Umbrales y riesgos base unificados
MODO_AGRESIVO_VOL_THRESHOLD = 0.02
MODO_AGRESIVO_SLOPE_THRESHOLD = 0.002
RIESGO_POR_TRADE_BASE = 0.02  # 2% por operación
RIESGO_MAXIMO_DIARIO_BASE = 0.06  # 6% riesgo global diario

log = configurar_logger("ajustador_riesgo")


def es_modo_agresivo(volatilidad: float, slope_pct: float) -> bool:
    """Determina si el bot debe operar en modo agresivo."""
    return (
        volatilidad > MODO_AGRESIVO_VOL_THRESHOLD
        or abs(slope_pct) > MODO_AGRESIVO_SLOPE_THRESHOLD
    )


def ajustar_sl_tp_riesgo(
    volatilidad: float,
    slope_pct: float,
    base_riesgo: float = RIESGO_MAXIMO_DIARIO_BASE,
    sl_ratio: float = 1.5,
    tp_ratio: float = 3.0,
) -> tuple[float, float, float]:
    """Calcula ratios de SL/TP y riesgo adaptativos."""

    log.info("➡️ Entrando en ajustar_sl_tp_riesgo()")
    riesgo_inicial = base_riesgo
    # Ajuste por volatilidad
    if volatilidad > 0.02:
        sl_ratio *= 1.2
        base_riesgo *= 0.8
    elif volatilidad < 0.01 and abs(slope_pct) > 0.001:
        base_riesgo *= 1.2

    # Ajuste por slope
    if slope_pct > 0.001:
        tp_ratio *= 1.1
    elif slope_pct < -0.001:
        tp_ratio *= 0.9
        sl_ratio *= 1.1

    sl_ratio = min(max(sl_ratio, 0.5), 5.0)
    tp_ratio = max(tp_ratio, sl_ratio * 1.2)
    tp_ratio = min(max(tp_ratio, 1.0), 8.0)
    riesgo_maximo_diario = round(base_riesgo, 4)
    if riesgo_maximo_diario != round(riesgo_inicial, 4):
        log.info(
            "Riesgo diario ajustado de %.4f a %.4f",
            round(riesgo_inicial, 4),
            riesgo_maximo_diario,
        )
    return round(sl_ratio, 2), round(tp_ratio, 2), riesgo_maximo_diario