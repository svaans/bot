from __future__ import annotations
from core.utils.utils import configurar_logger
from core.strategies.exit.gestor_salidas import (
    PRIORIDADES as PRIORIDAD_EVENTOS,
)
log = configurar_logger('salida_utils')


def resultado_salida(evento: str, cerrar: bool, razon: str, logger=None, **
    extras) ->dict:
    """Genera un diccionario estándar para resultados de salida."""
    if logger and cerrar:
        logger.info(f'{evento} → {razon}')
    data = {'cerrar': cerrar, 'evento': evento, 'razon': razon}
    data.update(extras)
    return data
