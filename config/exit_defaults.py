from __future__ import annotations
from typing import Dict, Any

from .configuracion import cargar_configuracion_simbolo

# Valores predeterminados para todas las estrategias de salida
DEFAULT_EXIT_CFG: Dict[str, Any] = {
    'factor_umbral_sl': 0.7,
    'min_estrategias_relevantes_sl': 3,
    'sl_ratio': 1.5,
    'max_velas_sin_tp': 10,
    'max_evitar_sl': 2,
    'atr_multiplicador': 1.0,
    'trailing_pct': 0.015,
    'trailing_buffer': 0.0,
    'trailing_start_ratio': 1.015,
    'trailing_distance_ratio': 0.02,
    'trailing_por_atr': False,
    'uso_trailing_technico': False,
    'tp_ratio': 2.5,
    'volumen_minimo_salida': 0.0,
    'max_spread_ratio': 0.003,
    'periodo_atr': 14,
    'break_even_atr_mult': 1.5,
    'umbral_rsi_salida': 55,
    'factor_umbral_validacion_salida': 0.8,
    'umbral_puntaje_macro_cierre': 6,
    'max_intentos_cierre': 3,
    'delay_reintento_cierre': 1,
    'sl_emergency_pct': 0.02,
}


def validate_exit_config(cfg: Dict[str, Any]) -> None:
    """Verifica tipos y rangos básicos de la configuración."""
    for clave, valor in cfg.items():
        if clave in {'trailing_por_atr', 'uso_trailing_technico'}:
            if not isinstance(valor, bool):
                raise ValueError(f"Valor inválido para {clave}: {valor}")
        else:
            if not isinstance(valor, (int, float)) or valor < 0:
                raise ValueError(f"Valor inválido para {clave}: {valor}")


def load_exit_config(symbol: str) -> Dict[str, Any]:
    """Carga la configuración de salidas para ``symbol`` aplicando valores por defecto."""
    try:
        almacenada = cargar_configuracion_simbolo(symbol) or {}
    except Exception:
        almacenada = {}
    cfg = DEFAULT_EXIT_CFG.copy()
    for k in DEFAULT_EXIT_CFG:
        if k in almacenada:
            cfg[k] = almacenada[k]
    validate_exit_config(cfg)
    return cfg
