import pandas as pd
from core.utils.utils import validar_dataframe
from core.strategies.tendencia import detectar_tendencia
from core.utils import configurar_logger
from core.strategies.exit.salida_utils import resultado_salida
log = configurar_logger('salida_por_tendencia')


def salida_por_tendencia(orden, df):
    """
    Cierra si la tendencia cambia respecto a la entrada.
    """
    tendencia_entrada = orden.get('tendencia')
    if not tendencia_entrada:
        return resultado_salida('Tecnico', False,
            'Sin tendencia previa registrada')
    try:
        tendencia_actual, _ = detectar_tendencia(orden['symbol'], df)
        if tendencia_actual != tendencia_entrada:
            return resultado_salida(
                'Tecnico',
                True,
                f'Cambio de tendencia: {tendencia_entrada} → {tendencia_actual}',
                logger=log,
            )
        else:
            return resultado_salida('Tecnico', False, 'Tendencia estable')
    except (KeyError, ValueError, TypeError) as e:
        log.error(
            f"Error evaluando tendencia para {orden.get('symbol', 'SYM')}: {e}"
        )
        return resultado_salida('Tecnico', False,
            f'Error evaluando tendencia: {e}', logger=log)


def verificar_reversion_tendencia(symbol, df, tendencia_anterior):
    if not validar_dataframe(df, ['high', 'low', 'close']):
        return False
    nueva_tendencia, _ = detectar_tendencia(symbol, df)
    return nueva_tendencia != tendencia_anterior
