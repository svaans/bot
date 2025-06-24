from __future__ import annotations

import pandas as pd

from indicators.correlacion import calcular_correlacion
from core.strategies.entry.validador_entradas import verificar_liquidez_orden
from core.utils import configurar_logger

log = configurar_logger("validaciones_comunes", modo_silencioso=True)


def validar_correlacion(
    symbol: str,
    df: pd.DataFrame | None,
    df_ref: pd.DataFrame | None,
    umbral: float,
) -> bool:
    """Rechaza si la correlación entre ``df`` y ``df_ref`` supera ``umbral``."""
    if df is not None and df_ref is not None and umbral < 1.0:
        correlacion = calcular_correlacion(df, df_ref)
        if correlacion is not None and correlacion >= umbral:
            log.info(
                f"🚫 [{symbol}] Rechazo por correlación {correlacion:.2f} >= {umbral}"
            )
            return False
    return True


def validar_diversidad(
    symbol: str,
    estrategias: dict,
    minimo: int = 1,
) -> bool:
    """Verifica que existan al menos ``minimo`` estrategias activas."""
    activas = sum(1 for v in estrategias.values() if v)
    if activas < minimo:
        log.info(f"🚫 [{symbol}] Rechazo por falta de estrategias activas")
        return False
    return True


def validar_volumen(
    symbol: str,
    df: pd.DataFrame | None,
    cantidad: float,
    ventana: int = 20,
    factor: float = 0.2,
) -> bool:
    """Comprueba la liquidez disponible en ``df`` para una orden dada."""
    if df is not None and cantidad > 0:
        if not verificar_liquidez_orden(df, cantidad, ventana=ventana, factor=factor):
            log.info(
                f"🚫 [{symbol}] Rechazo por volumen insuficiente para {cantidad}"
            )
            return False
    return True