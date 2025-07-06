from __future__ import annotations
from core.utils.utils import configurar_logger
log = configurar_logger('salida_utils')
PRIORIDAD_EVENTOS = {'Stop Loss': 3, 'Take Profit': 2, 'Trailing Stop': 1,
    'Tecnico': 0}


def resultado_salida(evento: str, cerrar: bool, razon: str, logger=None, **
    extras) ->dict:
    log.info('➡️ Entrando en resultado_salida()')
    """Genera un diccionario estándar para resultados de salida."""
    if logger and cerrar:
        logger.info(f'{evento} → {razon}')
    data = {'cerrar': cerrar, 'evento': evento, 'razon': razon}
    data.update(extras)
    return data
