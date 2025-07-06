import os
import re
from datetime import datetime, timedelta
import pandas as pd
from core.utils.utils import configurar_logger
from core.utils.utils import leer_csv_seguro
log = configurar_logger('kelly')


def calcular_fraccion_kelly(dias_historia: int=30, fallback: float=0.2
    ) ->float:
    log.info('➡️ Entrando en calcular_fraccion_kelly()')
    """Calcula la fracción de capital a arriesgar usando el Criterio de Kelly.

    Se basa en los reportes diarios generados por ``ReporterDiario``. Si no
    existen suficientes registros, devuelve ``fallback``.
    """
    carpeta = 'reportes_diarios'
    if not os.path.isdir(carpeta):
        return fallback
    fecha_limite = datetime.utcnow().date() - timedelta(days=dias_historia)
    retornos: list[float] = []
    patron = re.compile('\\d{4}-\\d{2}-\\d{2}\\.csv$')
    for archivo in os.listdir(carpeta):
        if not patron.match(archivo):
            continue
        try:
            fecha = datetime.fromisoformat(archivo.replace('.csv', '')).date()
        except ValueError as e:
            log.debug(f'Archivo de reporte ignorado {archivo}: {e}')
            continue
        if fecha < fecha_limite:
            continue
        try:
            df = leer_csv_seguro(os.path.join(carpeta, archivo),
                expected_cols=20)
        except (pd.errors.EmptyDataError, pd.errors.ParserError, OSError) as e:
            log.warning(f'⚠️ No se pudo leer reporte {archivo}: {e}')
            continue
        if 'retorno_total' in df.columns:
            retornos.extend(df['retorno_total'].dropna().tolist())
    if len(retornos) < 10:
        return fallback
    ganadoras = [r for r in retornos if r > 0]
    perdedoras = [r for r in retornos if r < 0]
    if not ganadoras or not perdedoras:
        return fallback
    winrate = len(ganadoras) / len(retornos)
    avg_profit = sum(ganadoras) / len(ganadoras)
    avg_loss = -sum(perdedoras) / len(perdedoras)
    if avg_loss == 0:
        return fallback
    payoff = avg_profit / avg_loss
    f = winrate - (1 - winrate) / payoff
    if f <= 0:
        return fallback
    return min(f, 0.6)
