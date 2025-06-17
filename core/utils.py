# core/utils.py
import os
import time
import shutil
import threading
import json
import pandas as pd
from datetime import datetime
from core.ordenes_model import Orden
from core.logger import configurar_logger
from decimal import Decimal, InvalidOperation

log = configurar_logger("trader_simulado", modo_silencioso=True)


def respaldar_archivo(ruta):
    if os.path.exists(ruta):
        backup = ruta.replace(".json", "_backup.json")
        shutil.copy(ruta, backup)

def is_valid_number(value) -> bool:
    """Verifica si un valor es un número finito y válido."""
    try:
        num = float(value)
        return not (pd.isna(num) or pd.isnull(num) or num != num or num in [float("inf"), float("-inf")])
    except (ValueError, TypeError):
        return False


def guardar_operacion_en_csv(symbol, info, ruta="ordenes_reales"):
    os.makedirs(ruta, exist_ok=True)
    archivo = os.path.join(ruta, f"ordenes_{symbol.replace('/', '_')}_resultado.csv")

    columnas_orden = [
        "symbol",
        "precio_entrada",
        "precio_cierre",
        "retorno_total",
        "stop_loss",
        "take_profit",
        "cantidad",
        "motivo_cierre",
        "fecha_cierre",
        "estrategias_activas",
    ]

    
    if isinstance(info.get("estrategias_activas"), dict):
        info["estrategias_activas"] = json.dumps(info["estrategias_activas"])

    
    if "retorno_total" not in info or not isinstance(info["retorno_total"], (float, int)):
        try:
            entrada = float(info.get("precio_entrada", 0))
            cierre = float(info.get("precio_cierre", 0))
            if entrada > 0:
                info["retorno_total"] = round((cierre - entrada) / entrada, 6)
            else:
                info["retorno_total"] = 0.0
        except ValueError as e:
            log.warning(f"⚠️ Error calculando retorno_total para {symbol}: {e}")
            info["retorno_total"] = 0.0

    fila = {col: info.get(col, "") for col in columnas_orden}
    df = pd.DataFrame([fila])

    
    if os.path.exists(archivo):
        df.to_csv(archivo, mode="a", header=False, index=False)
    else:
        df.to_csv(archivo, mode="w", header=True, index=False)


def validar_dataframe(df, columnas):
    return df is not None and not df.empty and all(col in df.columns for col in columnas)


def validar_tp(tp: float, precio: float, max_relativo: float = 1.05, max_absoluto: float = 0.03) -> float:
    """Limita el Take Profit a valores razonables."""
    limite_rel = precio * max_relativo
    limite_abs = precio * (1 + max_absoluto)
    max_tp = min(limite_rel, limite_abs)
    if tp > max_tp:
        log.warning(f"🎯 TP ajustado de {tp} a {max_tp}")
        return max_tp
    return tp

def distancia_minima_valida(precio: float, sl: float, tp: float, min_pct: float = 0.0005) -> bool:
    """Comprueba que SL y TP estén a una distancia mínima de ``precio``.

    La distancia se evalúa en términos porcentuales de ``precio``. Si
    cualquiera de los niveles está demasiado cerca se considera inválido.
    """
    return (
        abs(precio - sl) >= precio * min_pct
        and abs(tp - precio) >= precio * min_pct
    )


def margen_tp_sl_valido(tp: float, sl: float, precio_actual: float, min_pct: float = 0.0005) -> bool:
    """Valida que la distancia entre TP y SL sea suficiente."""
    return abs(tp - sl) >= precio_actual * min_pct


def validar_ratio_beneficio(entrada: float, sl: float, tp: float, ratio_minimo: float) -> bool:
    """Comprueba que el ratio beneficio/riesgo cumpla el mínimo requerido."""
    riesgo = entrada - sl
    beneficio = tp - entrada
    if riesgo <= 0:
        return False
    return (beneficio / riesgo) >= ratio_minimo


def segundos_transcurridos(timestamp_iso):
    """Devuelve los segundos transcurridos desde un timestamp ISO o epoch."""
    try:
        if isinstance(timestamp_iso, (int, float)):
            ts = timestamp_iso / 1000 if timestamp_iso > 1e12 else timestamp_iso
            inicio = datetime.utcfromtimestamp(ts)
        elif isinstance(timestamp_iso, str) and timestamp_iso.isdigit():
            ts = int(timestamp_iso)
            ts = ts / 1000 if ts > 1e12 else ts
            inicio = datetime.utcfromtimestamp(ts)
        else:
            inicio = datetime.fromisoformat(str(timestamp_iso))
        ahora = datetime.utcnow()
        return (ahora - inicio).total_seconds()
    except (ValueError, TypeError) as e:
        log.warning(f"⚠️ Error calculando segundos desde {timestamp_iso}: {e}")
        return 0
    

def leer_csv_seguro(ruta: str, log_lineas=None, expected_cols: int | None = None) -> pd.DataFrame:
    """Carga ``ruta`` ignorando líneas corruptas y evitando interrupciones."""
    logger = configurar_logger("csv_reader", modo_silencioso=True)
    line_logger = None
    if log_lineas:
        line_logger = (
            log_lineas
            if hasattr(log_lineas, "warning")
            else configurar_logger("csv_corrupto", modo_silencioso=True)
        )
    try:
        df = pd.read_csv(ruta)
        if expected_cols and df.shape[1] < expected_cols:
            logger.warning(
                f"⚠️ {ruta} tiene {df.shape[1]} columnas (<{expected_cols}). Reintentando con líneas omitidas"
            )
            df = pd.read_csv(ruta, on_bad_lines="skip")
        return df
    except pd.errors.ParserError as e:
        logger.warning(
            f"⚠️ Error leyendo archivo {ruta}: {e}. Reintentando con líneas omitidas"
        )
        if line_logger:
            def _bad_line(line: list[str]) -> None:
                line_logger.warning(f"{ruta} => {line}")
                return None

            try:
                return pd.read_csv(ruta, on_bad_lines=_bad_line, engine="python")
            except Exception as err:
                logger.error(f"❌ No se pudo recuperar {ruta}: {err}")
                return pd.DataFrame()
        try:
            return pd.read_csv(ruta, on_bad_lines="skip")
        except Exception as err:
            logger.error(f"❌ No se pudo recuperar {ruta}: {err}")
            return pd.DataFrame()
    except (pd.errors.EmptyDataError, OSError) as e:
        logger.warning(f"⚠️ Archivo vacío o no accesible {ruta}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"❌ Error inesperado leyendo {ruta}: {e}")
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
                        ordenes = df.to_dict("records")
                    except (OSError, ValueError) as e:
                        log.error(f"❌ Error leyendo {archivo}: {e}")
                        raise ValueError(f"Archivo dañado: {e}")
                else:
                    ordenes = []
                ordenes.append(nueva_orden)
                pd.DataFrame(ordenes).to_parquet(archivo, index=False)
                log.info(f"💾 Orden simulada registrada en {archivo}")
                return
        except ValueError as e:
            try:
                timestamp = int(time.time())
                corrupto = archivo.replace(".parquet", f"_corrupto_{timestamp}.parquet")
                os.rename(archivo, corrupto)
                log.warning(
                    f"⚠️ Archivo corrupto renombrado: {archivo} → {corrupto} — Error: {e}"
                )
            except OSError as err:
                log.error(f"❌ Error al renombrar archivo corrupto: {err}")
                raise
            ordenes = [nueva_orden]
        except PermissionError:
            log.error(f"⏳ Archivo en uso ({archivo}) — intento {intento + 1}/3")
            time.sleep(0.5)
        except Exception as e:
            log.error(f"❌ Error inesperado guardando orden simulada: {e}")
            raise
    log.error(f"❌ No se pudo guardar la orden simulada para {symbol} tras 3 intentos.")

def guardar_orden_real(symbol: str, orden: dict | Orden):
    """
    Guarda una orden real en un archivo Parquet por símbolo de forma segura, eficiente y precisa.
    """
    try:
        data = orden.to_parquet_record() if hasattr(orden, "to_parquet_record") else orden.copy()
    except Exception as e:
        log.error(f"❌ No se pudo convertir la orden a formato Parquet: {e}")
        return

    archivo = f"ordenes_reales/{symbol.replace('/', '_').lower()}.parquet"
    os.makedirs(os.path.dirname(archivo), exist_ok=True)

    # Validación y normalización de campos numéricos
    campos_flotantes = ["precio_entrada", "precio_cierre", "cantidad", "stop_loss", "take_profit", "retorno_total"]
    for campo in campos_flotantes:
        try:
            valor = data.get(campo)
            if valor is not None:
                data[campo] = float(round(Decimal(str(valor)), 8))
        except (InvalidOperation, TypeError):
            log.warning(f"⚠️ Valor inválido en campo {campo}: {valor}. Se asignará 0.0")
            data[campo] = 0.0

    if isinstance(data.get("timestamp"), (pd.Timestamp, datetime)):
        data["timestamp"] = data["timestamp"].timestamp()

    if isinstance(data.get("estrategias_activas"), dict):
        data["estrategias_activas"] = json.dumps(data["estrategias_activas"])

    try:
        with lock_archivo:
            ordenes = []
            if os.path.exists(archivo):
                try:
                    df = pd.read_parquet(archivo)

                    # ✅ Protección contra duplicados exactos
                    data_set = {json.dumps(data, sort_keys=True)}
                    ordenes = [
                        o for o in df.to_dict("records")
                        if json.dumps(o, sort_keys=True) not in data_set
                    ]
                except (OSError, ValueError) as e:
                    backup = archivo.replace(
                        ".parquet", f"_corrupto_{int(datetime.now().timestamp())}.parquet"
                    )
                    os.rename(archivo, backup)
                    log.warning(f"⚠️ Archivo corrupto renombrado: {backup}")
                    ordenes = []

            ordenes.append(data)
            pd.DataFrame(ordenes).to_parquet(archivo, index=False)
            log.info(f"💾 Orden REAL registrada en {archivo}")

    except Exception as e:
        log.error(f"❌ Error guardando orden real en {archivo}: {e}")
        raise
        
