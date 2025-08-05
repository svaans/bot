"""Gestor central de configuraciones dinámicas del bot."""
from __future__ import annotations
import pandas as pd

from core.adaptador_configuracion_dinamica import _adaptar_configuracion_indicadores as _adaptar_indicadores
from core.adaptador_dinamico import _adaptar_configuracion_base as _adaptar_base


def adaptar_configuracion(symbol: str, df: pd.DataFrame, base_config: dict | None=None) -> dict:
    """Devuelve una configuración combinando ajustes dinámicos por indicadores
    y las reglas base del adaptador dinámico.

    Parameters
    ----------
    symbol : str
        Símbolo a evaluar.
    df : pd.DataFrame
        Serie temporal del mercado.
    base_config : dict | None
        Configuración existente que será actualizada.

    Returns
    -------
    dict
        Configuración adaptada.
    """
    if base_config is None:
        base_config = {}
    config = base_config.copy()
    # Ajustes según indicadores (ATR, RSI, pendiente, volumen...)
    config.update(_adaptar_indicadores(symbol, df, config) or {})
    # Ajustes finales del adaptador base
    config = _adaptar_base(symbol, df, config)
    return config
