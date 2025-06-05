# core/utils.py
import os
import time
import shutil
import threading
import json
import pandas as pd
from datetime import datetime


from core.logger import configurar_logger


log = configurar_logger("trader_simulado", modo_silencioso=True)

def respaldar_archivo(ruta):
    if os.path.exists(ruta):
        backup = ruta.replace(".json", "_backup.json")
        shutil.copy(ruta, backup)

def guardar_operacion_en_csv(symbol, info, ruta="ordenes_reales"):
    os.makedirs(ruta, exist_ok=True)
    archivo = os.path.join(ruta, f"ordenes_{symbol.replace('/', '_')}_resultado.csv")

    columnas_orden = [
        "symbol", "precio_entrada", "precio_cierre", "retorno_total",
        "stop_loss", "take_profit", "cantidad", "motivo_cierre",
        "fecha_cierre", "estrategias_activas"
    ]

    # Convertir estrategias_activas a JSON string si es dict
    if isinstance(info.get("estrategias_activas"), dict):
        info["estrategias_activas"] = json.dumps(info["estrategias_activas"])

    # Calcular retorno_total si falta
    if "retorno_total" not in info or not isinstance(info["retorno_total"], (float, int)):
        try:
            entrada = float(info.get("precio_entrada", 0))
            cierre = float(info.get("precio_cierre", 0))
            if entrada > 0:
                info["retorno_total"] = round((cierre - entrada) / entrada, 6)
            else:
                info["retorno_total"] = 0.0
        except Exception as e:
            info["retorno_total"] = 0.0

    fila = {col: info.get(col, "") for col in columnas_orden}
    df = pd.DataFrame([fila])

    # Si el archivo existe, añadir sin sobrescribir
    if os.path.exists(archivo):
        df.to_csv(archivo, mode="a", header=False, index=False)
    else:
        df.to_csv(archivo, mode="w", header=True, index=False)


def validar_dataframe(df, columnas):
    return df is not None and not df.empty and all(col in df.columns for col in columnas)

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
    except Exception:
        return 0
    

def dividir_dataframe_en_bloques(df, n_bloques=10):
    """
    Divide el DataFrame en bloques del mismo tamaño.
    Si el número de filas no es divisible, los últimos bloques pueden ser ligeramente más pequeños.

    Args:
        df (pd.DataFrame): El DataFrame completo con históricos.
        n_bloques (int): Número de bloques en los que se quiere dividir.

    Returns:
        List[pd.DataFrame]: Lista de bloques del DataFrame.
    """
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

        for intento in range(3):  # 3 intentos para evitar errores de acceso concurrente
            try:
                with lock_archivo:
                    if os.path.exists(archivo):
                        try:
                            df = pd.read_parquet(archivo)
                            ordenes = df.to_dict("records")
                        except Exception as e:
                            raise ValueError(f"Archivo dañado: {e}")
                    else:
                        ordenes = []

                    ordenes.append(nueva_orden)
                    pd.DataFrame(ordenes).to_parquet(archivo, index=False)
                    log.info(f"💾 Orden simulada registrada en {archivo}")
                    return  # Éxito → salir

            except ValueError as e:
                # Archivo dañado, lo renombramos para evitar pérdida de datos
                try:
                    timestamp = int(time.time())
                    corrupto = archivo.replace(".parquet", f"_corrupto_{timestamp}.parquet")
                    os.rename(archivo, corrupto)
                    log.warning(f"⚠️ Archivo corrupto renombrado: {archivo} → {corrupto} — Error: {e}")
                except Exception as err:
                    log.error(f"❌ Error al renombrar archivo corrupto: {err}")
                ordenes = [nueva_orden]  # Reemplazamos con solo la nueva

            except PermissionError:
                log.error(f"⏳ Archivo en uso ({archivo}) — intento {intento + 1}/3")
                time.sleep(0.5)

            except Exception as e:
                log.error(f"❌ Error inesperado guardando orden simulada: {e}")
                return