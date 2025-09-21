from __future__ import annotations
import os
import time
import shutil
import threading
import re
import json
import logging
from contextlib import suppress
from importlib import import_module
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable, Callable

import pandas as pd
import psutil

from .logger import configurar_logger
from core.modo import MODO_REAL
from core.registro_metrico import registro_metrico

_frequencies_module: Any | None = None
with suppress(Exception):
    _frequencies_module = import_module('pandas.tseries.frequencies')

if _frequencies_module and hasattr(_frequencies_module, 'to_offset'):
    to_offset = getattr(_frequencies_module, 'to_offset')
else:
    def to_offset(value: str) -> str:
        return value
if TYPE_CHECKING:
    from core.order_model import Order
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN

ROUNDING_POLICY = ROUND_HALF_EVEN

def round_decimal(value: float | Decimal, places: int = 8) -> Decimal:
    """Redondea ``value`` usando la política global de redondeo."""
    cuantizador = Decimal('1').scaleb(-places)
    return Decimal(str(value)).quantize(cuantizador, rounding=ROUNDING_POLICY)

DATOS_DIR = os.getenv('DATOS_DIR', 'datos')
ESTADO_DIR = os.getenv('ESTADO_DIR', 'estado')
ORDENES_DIR = os.getenv('ORDENES_DIR', 'ordenes_reales')
# Mapeo de intervalos de velas a segundos
_MAPA_SEGUNDOS_INTERVALO = {
    '1m': 60,
    '3m': 180,
    '5m': 300,
    '15m': 900,
    '30m': 1800,
    '1h': 3600,
    '4h': 14400,
    '1d': 86400,
}


def intervalo_a_segundos(intervalo: str) -> int:
    """Devuelve el equivalente en segundos de ``intervalo``.

    Si el intervalo no está mapeado se devuelve ``60`` por defecto.
    """
    return _MAPA_SEGUNDOS_INTERVALO.get(intervalo, 60)


def timestamp_alineado(ts_ms: int, intervalo: str) -> bool:
    """Verifica que ``ts_ms`` esté alineado al inicio del intervalo.

    El timestamp debe venir en milisegundos y corresponder exactamente al
    comienzo del bloque temporal definido por ``intervalo``.
    """
    periodo = intervalo_a_segundos(intervalo) * 1000
    return ts_ms % periodo == 0

def validar_integridad_velas(
    symbol: str,
    tf: str,
    candles: Iterable[dict],
    log: logging.Logger,
    registrar_candles_duplicadas: Callable[[str, int], None] | None = None,
) -> bool:
    """Verifica duplicados, huecos y desalineaciones en ``candles``.

    Registra métricas y opcionalmente contabiliza duplicados mediante
    ``registrar_candles_duplicadas``.
    """
    timestamps = sorted(int(float(c["timestamp"])) for c in candles if "timestamp" in c)
    if len(timestamps) < 2:
        return True
    intervalo_ms = intervalo_a_segundos(tf) * 1000
    dupes = gaps = desalineados = 0
    prev = timestamps[0]
    for curr in timestamps[1:]:
        diff = curr - prev
        if diff == 0:
            dupes += 1
        elif diff > intervalo_ms:
            if diff % intervalo_ms == 0:
                gaps += diff // intervalo_ms - 1
            else:
                desalineados += 1
        elif diff % intervalo_ms != 0:
            desalineados += 1
        prev = curr
    if dupes:
        registro_metrico.registrar(
            "velas_duplicadas", {"symbol": symbol, "tf": tf, "count": dupes}
        )
        if registrar_candles_duplicadas:
            registrar_candles_duplicadas(symbol, dupes)
        log.warning(f"[{symbol}] {dupes} velas duplicadas detectadas en {tf}")
    if gaps:
        registro_metrico.registrar(
            "velas_gap", {"symbol": symbol, "tf": tf, "count": gaps}
        )
        log.warning(f"[{symbol}] Gap de {gaps} velas en {tf}")
    if desalineados:
        log.error(f"[{symbol}] Timestamps desalineados en {tf}: {desalineados}")
    return dupes == 0 and gaps == 0 and desalineados == 0


def safe_resample(df: pd.DataFrame, freq: str):
    """Devuelve un resampler validado para ``df``.

    Normaliza ``freq`` a minúsculas, reemplaza el alias ``T`` por ``min``
    y valida la frecuencia con ``to_offset``. Lanza ``ValueError`` si la
    frecuencia no es válida.
    """
    norm = freq.lower().strip()
    if norm.endswith("t"):
        norm = norm[:-1] + "min"
    try:
        to_offset(norm)
    except ValueError as err:
        raise ValueError(f"Frecuencia inválida: {freq}") from err
    return df.resample(norm)


log = configurar_logger('trader' if MODO_REAL else 'trader_simulado')


def obtener_uso_recursos() -> tuple[float, float]:
    """Devuelve el uso actual de CPU y memoria en porcentaje."""
    cpu = psutil.cpu_percent()
    memoria = psutil.virtual_memory().percent
    return cpu, memoria


def respaldar_archivo(ruta):
    if os.path.exists(ruta):
        backup = ruta.replace('.json', '_backup.json')
        shutil.copy(ruta, backup)


def is_valid_number(value) ->bool:
    """Verifica si un valor es un número finito y válido."""
    try:
        num = float(value)
        return not (pd.isna(num) or pd.isnull(num) or num != num or num in
            [float('inf'), float('-inf')])
    except (ValueError, TypeError):
        return False


def guardar_operacion_en_csv(symbol, info, ruta=ORDENES_DIR, intentos: int = 3):
    os.makedirs(ruta, exist_ok=True)
    archivo = os.path.join(ruta,
        f"ordenes_{symbol.replace('/', '_')}_resultado.csv")
    columnas_orden = ['symbol', 'precio_entrada', 'precio_cierre',
        'retorno_total', 'stop_loss', 'take_profit', 'cantidad',
        'motivo_cierre', 'fecha_cierre', 'estrategias_activas']
    if isinstance(info.get('estrategias_activas'), dict):
        info['estrategias_activas'] = json.dumps(info['estrategias_activas'])
    if 'retorno_total' not in info or not isinstance(info['retorno_total'],
        (float, int)):
        try:
            entrada = float(info.get('precio_entrada', 0))
            cierre = float(info.get('precio_cierre', 0))
            if entrada > 0:
                info['retorno_total'] = round((cierre - entrada) / entrada, 6)
            else:
                info['retorno_total'] = 0.0
        except ValueError as e:
            log.warning(f'⚠️ Error calculando retorno_total para {symbol}: {e}'
                )
            info['retorno_total'] = 0.0
    fila = {col: info.get(col, '') for col in columnas_orden}
    df = pd.DataFrame([fila])
    for intento in range(1, intentos + 1):
        try:
            if os.path.exists(archivo):
                df.to_csv(archivo, mode='a', header=False, index=False)
            else:
                df.to_csv(archivo, mode='w', header=True, index=False)
            break
        except Exception as e:
            if intento == intentos:
                log.warning(f'⚠️ No se pudo guardar operación tras {intentos} intentos: {e}')
            else:
                time.sleep(1)


def validar_dataframe(df, columnas):
    return df is not None and not df.empty and all(col in df.columns for
        col in columnas)


def verificar_integridad_datos(
    df: pd.DataFrame, max_gap_pct: float = 0.5
) -> tuple[bool, pd.DataFrame]:
    """Comprueba valores nulos y gaps en ``df``.

    Devuelve una tupla ``(ok, df_limpio)`` sin modificar el DataFrame original.

    Si se detectan NaNs, gaps temporales amplios o variaciones extremas,
    ``ok`` será ``False``.
    """
    if df is None or df.empty:
        return False, df

    df_limpio = df.dropna().copy()
    if len(df_limpio) != len(df):
        log.warning('⚠️ DataFrame con NaNs detectado — limpiando')
    if df_limpio.empty:
        log.warning('⚠️ DataFrame vacío o con NaNs tras limpieza')
        return False, df_limpio

    if 'timestamp' in df_limpio.columns:
        # ``timestamp`` llega en milisegundos desde epoch; sin especificar la
        # unidad Pandas asume nanosegundos, lo que puede generar fechas de 1970
        # o valores NaT y producir falsos positivos al buscar gaps.
        ts = pd.to_datetime(df_limpio['timestamp'], unit='ms', errors='coerce')
        if ts.isna().any():
            return False, df_limpio
        ts = ts.sort_values()
        diffs = ts.diff().dt.total_seconds().dropna()
        if not diffs.empty and diffs.max() > diffs.median() * 2:
            log.warning('⚠️ Gaps temporales excesivos detectados')
            return False, df_limpio

    if 'close' in df_limpio.columns:
        cambios = df_limpio['close'].pct_change().dropna()
        if not cambios.empty and cambios.abs().max() > max_gap_pct:
            log.warning('⚠️ Variaciones atípicas detectadas')
            return False, df_limpio

    return True, df_limpio


def validar_tp(tp: float, precio: float, max_relativo: float=1.05,
    max_absoluto: float=0.03) ->float:
    """Limita el Take Profit a valores razonables."""
    limite_rel = precio * max_relativo
    limite_abs = precio * (1 + max_absoluto)
    max_tp = min(limite_rel, limite_abs)
    if tp > max_tp:
        log.warning(f'🎯 TP ajustado de {tp} a {max_tp}')
        return max_tp
    return tp


def distancia_minima_valida(precio: float, sl: float, tp: float, min_pct:
    float=0.0005) ->bool:
    """Comprueba que SL y TP estén a una distancia mínima de ``precio``.

    La distancia se evalúa en términos porcentuales de ``precio``. Si
    cualquiera de los niveles está demasiado cerca se considera inválido.
    """
    return abs(precio - sl) >= precio * min_pct and abs(tp - precio
        ) >= precio * min_pct


def margen_tp_sl_valido(tp: float, sl: float, precio_actual: float, min_pct:
    float=0.0005) ->bool:
    """Valida que la distancia entre TP y SL sea suficiente."""
    return abs(tp - sl) >= precio_actual * min_pct


def validar_ratio_beneficio(entrada: float, sl: float, tp: float,
    ratio_minimo: float) ->bool:
    """Comprueba que el ratio beneficio/riesgo cumpla el mínimo requerido."""
    riesgo = entrada - sl
    beneficio = tp - entrada
    if riesgo <= 0:
        return False
    return beneficio / riesgo >= ratio_minimo


def segundos_transcurridos(timestamp_iso):
    """Devuelve los segundos transcurridos desde un timestamp ISO o epoch."""
    try:
        if isinstance(timestamp_iso, (int, float)):
            ts = (timestamp_iso / 1000 if timestamp_iso > 1000000000000.0 else timestamp_iso)
            inicio = datetime.fromtimestamp(ts, tz=timezone.utc)
        elif isinstance(timestamp_iso, str) and timestamp_iso.isdigit():
            ts = int(timestamp_iso)
            ts = ts / 1000 if ts > 1000000000000.0 else ts
            inicio = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            inicio = datetime.fromisoformat(str(timestamp_iso))
            if inicio.tzinfo is None:
                inicio = inicio.replace(tzinfo=timezone.utc)
        ahora = datetime.now(timezone.utc)
        return (ahora - inicio).total_seconds()
    except (ValueError, TypeError) as e:
        log.warning(f'⚠️ Error calculando segundos desde {timestamp_iso}: {e}')
        return 0


def leer_csv_seguro(ruta: str, log_lineas=None, expected_cols: (int | None)
    =None) ->pd.DataFrame:
    """Carga ``ruta`` ignorando líneas corruptas y evitando interrupciones."""
    logger = configurar_logger('csv_reader', modo_silencioso=True)
    line_logger = None
    if log_lineas:
        line_logger = log_lineas if hasattr(log_lineas, 'warning'
            ) else configurar_logger('csv_corrupto', modo_silencioso=True)
    try:
        df = pd.read_csv(ruta)
        if expected_cols and df.shape[1] < expected_cols:
            logger.warning(
                f'⚠️ {ruta} tiene {df.shape[1]} columnas (<{expected_cols}). Reintentando con líneas omitidas'
                )
            df = pd.read_csv(ruta, on_bad_lines='skip')
        return df
    except pd.errors.ParserError as e:
        logger.warning(
            f'⚠️ Error leyendo archivo {ruta}: {e}. Reintentando con líneas omitidas'
            )
        if line_logger:

            def _bad_line(line: list[str]) ->None:
                line_logger.warning(f'{ruta} => {line}')
                return None
            try:
                return pd.read_csv(ruta, on_bad_lines=_bad_line, engine=
                    'python')
            except Exception as err:
                logger.error(f'❌ No se pudo recuperar {ruta}: {err}')
                return pd.DataFrame()
        try:
            return pd.read_csv(ruta, on_bad_lines='skip')
        except Exception as err:
            logger.error(f'❌ No se pudo recuperar {ruta}: {err}')
            return pd.DataFrame()
    except (pd.errors.EmptyDataError, OSError) as e:
        logger.warning(f'⚠️ Archivo vacío o no accesible {ruta}: {e}')
        return pd.DataFrame()
    except Exception as e:
        logger.error(f'❌ Error inesperado leyendo {ruta}: {e}')
        return pd.DataFrame()


def leer_reporte_seguro(path: str, columnas_esperadas: int=24) ->pd.DataFrame:
    """Lee un reporte diario garantizando la estructura correcta."""
    try:
        df = pd.read_csv(path)
        if df.shape[1] < columnas_esperadas:
            raise ValueError(
                f'📛 Columnas esperadas: {columnas_esperadas}, encontradas: {df.shape[1]}'
                )
        if df.shape[1] > columnas_esperadas:
            log.warning(
                f'⚠️ Columnas extras detectadas en {path}: {df.shape[1]}. '
                f'Se recortarán a {columnas_esperadas}.'
            )
            df = df.iloc[:, :columnas_esperadas]
        return df
    except Exception as e:
        log.warning(f'⚠️ Error leyendo archivo {path}: {e}')
        return pd.DataFrame()


def dividir_dataframe_en_bloques(df, n_bloques=10):
    """Divide ``df`` en ``n_bloques`` del mismo tamaño."""
    bloques = []
    total = len(df)
    tam_bloque = total // n_bloques
    for i in range(n_bloques):
        inicio = i * tam_bloque
        fin = (i + 1) * tam_bloque if i < n_bloques - 1 else total
        bloques.append(df.iloc[inicio:fin].copy())
    return bloques


lock_archivo = threading.Lock()


def guardar_orden_simulada(symbol: str, nueva_orden: dict):
    archivo = f"ordenes_simuladas/{symbol.replace('/', '_').lower()}.parquet"
    for intento in range(3):
        try:
            with lock_archivo:
                if os.path.exists(archivo):
                    try:
                        df = pd.read_parquet(archivo)
                        ordenes = df.to_dict('records')
                    except (OSError, ValueError) as e:
                        log.error(f'❌ Error leyendo {archivo}: {e}')
                        raise ValueError(f'Archivo dañado: {e}')
                else:
                    ordenes = []
                ordenes.append(nueva_orden)
                pd.DataFrame(ordenes).to_parquet(archivo, index=False)
                log.info(f'💾 Orden simulada registrada en {archivo}')
                return
        except ValueError as e:
            try:
                timestamp = int(time.time())
                corrupto = archivo.replace('.parquet',
                    f'_corrupto_{timestamp}.parquet')
                os.rename(archivo, corrupto)
                log.warning(
                    f'⚠️ Archivo corrupto renombrado: {archivo} → {corrupto} — Error: {e}'
                    )
            except OSError as err:
                log.error(f'❌ Error al renombrar archivo corrupto: {err}')
                raise
            ordenes = [nueva_orden]
        except PermissionError:
            log.error(f'⏳ Archivo en uso ({archivo}) — intento {intento + 1}/3'
                )
            time.sleep(0.5)
        except Exception as e:
            log.error(f'❌ Error inesperado guardando orden simulada: {e}')
            raise
    log.error(
        f'❌ No se pudo guardar la orden simulada para {symbol} tras 3 intentos.'
        )


def guardar_orden_real(symbol: str, orden: (dict | Order)):
    """
    Guarda una orden real en un archivo Parquet por símbolo de forma segura, eficiente y precisa.
    """
    try:
        data = orden.to_parquet_record() if hasattr(orden, 'to_parquet_record'
            ) else orden.copy()
    except Exception as e:
        log.error(f'❌ No se pudo convertir la orden a formato Parquet: {e}')
        return
    archivo = f"ordenes_reales/{symbol.replace('/', '_').lower()}.parquet"
    os.makedirs(os.path.dirname(archivo), exist_ok=True)
    campos_flotantes = ['precio_entrada', 'precio_cierre', 'cantidad',
        'stop_loss', 'take_profit', 'retorno_total']
    for campo in campos_flotantes:
        try:
            valor = data.get(campo)
            if valor is not None:
                data[campo] = float(round_decimal(valor, 8))
        except (InvalidOperation, TypeError):
            log.warning(
                f'⚠️ Valor inválido en campo {campo}: {valor}. Se asignará 0.0'
                )
            data[campo] = 0.0
    if isinstance(data.get('timestamp'), (pd.Timestamp, datetime)):
        data['timestamp'] = data['timestamp'].timestamp()
    if isinstance(data.get('estrategias_activas'), dict):
        data['estrategias_activas'] = json.dumps(data['estrategias_activas'])
    try:
        with lock_archivo:
            ordenes = []
            if os.path.exists(archivo):
                try:
                    df = pd.read_parquet(archivo)
                    data_set = {json.dumps(data, sort_keys=True)}
                    ordenes = [o for o in df.to_dict('records') if json.
                        dumps(o, sort_keys=True) not in data_set]
                except (OSError, ValueError) as e:
                    backup = archivo.replace('.parquet',
                        f'_corrupto_{int(datetime.now().timestamp())}.parquet')
                    os.rename(archivo, backup)
                    log.warning(f'⚠️ Archivo corrupto renombrado: {backup}')
                    ordenes = []
            ordenes.append(data)
            pd.DataFrame(ordenes).to_parquet(archivo, index=False)
            log.info(f'💾 Orden REAL registrada en {archivo}')
    except Exception as e:
        log.error(f'❌ Error guardando orden real en {archivo}: {e}')
        raise
