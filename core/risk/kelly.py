import os
import re
from datetime import datetime, timedelta, timezone

# Directorio de reportes diarios. Se puede sobreescribir con la variable de
# entorno REPORTES_DIARIOS_PATH para que Kelly funcione desde cualquier CWD
# (Docker, CI, scripts de análisis fuera de la raíz del proyecto).
_REPORTES_DIR: str = os.getenv("REPORTES_DIARIOS_PATH", "reportes_diarios")

UTC = timezone.utc
from math import isclose
from core.utils.log_utils import format_exception_for_log
from core.utils.utils import configurar_logger
from core.utils.utils import leer_csv_seguro
log = configurar_logger('kelly')


def calcular_fraccion_kelly(dias_historia: int=30, fallback: float=0.2
    ) ->float:
    """Calcula la fracción de capital a arriesgar usando el Criterio de Kelly.

    Se basa en los reportes diarios generados por ``ReporterDiario``. Si no
    existen suficientes registros, devuelve ``fallback``.
    """
    carpeta = _REPORTES_DIR
    if not os.path.isdir(carpeta):
        return fallback
    fecha_limite = datetime.now(UTC).date() - timedelta(days=dias_historia)
    retornos: list[float] = []
    patron = re.compile('\\d{4}-\\d{2}-\\d{2}\\.csv$')
    for archivo in os.listdir(carpeta):
        if not patron.match(archivo):
            continue
        try:
            fecha = datetime.fromisoformat(archivo.replace('.csv', '')).date()
        except ValueError as e:
            log.debug(
                'Archivo de reporte ignorado %s: %s',
                archivo,
                format_exception_for_log(e, 256),
            )
            continue
        if fecha < fecha_limite:
            continue
        # Sin expected_cols: leer_csv_seguro ya atrapa errores de lectura y
        # devuelve DataFrame vacío. La validación semántica es la presencia de
        # 'retorno_total'; un check de columnas exactas (== 20) silenciaba CSVs
        # con una columna extra cuando se añadía un nuevo campo al reporte.
        df = leer_csv_seguro(os.path.join(carpeta, archivo))
        if df.empty:
            log.debug('Reporte vacío o ilegible: %s', archivo)
            continue
        if 'retorno_total' in df.columns:
            retornos.extend(df['retorno_total'].dropna().tolist())
        else:
            log.debug(
                "Reporte %s sin columna 'retorno_total' (%d cols); ignorado",
                archivo, df.shape[1],
            )
    if len(retornos) < 10:
        return fallback
    ganadoras = [r for r in retornos if r > 0]
    perdedoras = [r for r in retornos if r < 0]
    if not ganadoras or not perdedoras:
        return fallback
    winrate = len(ganadoras) / len(retornos)
    avg_profit = sum(ganadoras) / len(ganadoras)
    avg_loss = -sum(perdedoras) / len(perdedoras)
    if isclose(avg_loss, 0.0, rel_tol=1e-12, abs_tol=1e-12):
        return fallback
    payoff = avg_profit / avg_loss
    f = winrate - (1 - winrate) / payoff
    if f <= 0:
        return fallback
    return min(f, 0.6)
