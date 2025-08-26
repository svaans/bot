"""Utilidades compartidas para ajustar ratios de SL/TP y riesgo.

Centraliza la lógica usada por ``adaptador_configuracion_dinamica`` y
``adaptador_dinamico`` para evitar inconsistencias en los parámetros.
"""
from __future__ import annotations
from core.utils.utils import configurar_logger

log = configurar_logger('ajustador_riesgo')


def ajustar_sl_tp_riesgo(
    volatilidad: float,
    slope_pct: float,
    base_riesgo: float = 2.0,
    sl_ratio: float = 1.5,
    tp_ratio: float = 3.0,
) -> tuple[float, float, float]:
    """Calcula ratios de SL/TP y riesgo adaptativos.

    Parameters
    ----------
    volatilidad: float
        Medida relativa de volatilidad (por ejemplo ATR porcentual).
    slope_pct: float
        Pendiente del precio normalizada por el precio actual.
    base_riesgo: float, optional
        Riesgo máximo diario de partida.
    sl_ratio: float, optional
        Ratio base del *stop loss*.
    tp_ratio: float, optional
        Ratio base del *take profit*.

    Returns
    -------
    tuple
        ``(sl_ratio, tp_ratio, riesgo_maximo_diario)`` ajustados.
    """
    log.info('➡️ Entrando en ajustar_sl_tp_riesgo()')
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
    return round(sl_ratio, 2), round(tp_ratio, 2), riesgo_maximo_diario