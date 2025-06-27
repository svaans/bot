# estrategias_salida/reajuste_tp_sl.py

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

RUTA_RESULTADOS = "backtesting"


def obtener_archivo(symbol):
    return os.path.join(RUTA_RESULTADOS, f"ordenes_{symbol.replace('/', '_')}_resultado.csv")


def calcular_promedios_sl_tp(symbol, dias=1):
    archivo = obtener_archivo(symbol)
    if not os.path.exists(archivo):
        print(f"❌ No hay archivo de resultados para {symbol}")
        return None

    df = pd.read_csv(archivo)
    if df.empty or "precio_entrada" not in df.columns or "precio_cierre" not in df.columns:
        print(f"⚠️ Datos insuficientes para {symbol}")
        return None

    # Convertir fechas y filtrar por rango
    df["fecha_cierre"] = pd.to_datetime(df["fecha_cierre"], errors='coerce')
    hace_dias = datetime.now(timezone.utc) - timedelta(days=dias)
    df = df[df["fecha_cierre"] >= hace_dias]

    if df.empty:
        print(f"⚠️ No hay operaciones recientes para {symbol}")
        return None

    # Cálculo de medias adaptativas
    df["delta"] = (df["precio_cierre"] - df["precio_entrada"]).abs()
    media_delta = df["delta"].mean()

    sl_promedio = round(media_delta * 0.6, 6)
    tp_promedio = round(media_delta * 1.2, 6)

    print(f"📈 {symbol} → SL medio: {sl_promedio} | TP medio: {tp_promedio}")
    return sl_promedio, tp_promedio








