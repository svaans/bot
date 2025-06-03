import json
import os
import pandas as pd
from collections import defaultdict
from core.logger import configurar_logger

log = configurar_logger("pesos")

RUTA_PESOS = "config/estrategias_pesos.json"
PESOS = None  # Solo si prefieres lazy loading

def cargar_pesos_estrategias():
    global PESOS
    if PESOS is not None:
        return PESOS

    if os.path.exists(RUTA_PESOS):
        with open(RUTA_PESOS, "r") as f:
            try:
                PESOS = json.load(f)
            except Exception as e:
                log.error(f"❌ Error cargando pesos desde JSON: {e}")
                PESOS = {}
    else:
        PESOS = {}
        print(f"❌ No se encontró archivo de pesos: {RUTA_PESOS}")
    return PESOS

def guardar_pesos_estrategias(pesos):
    with open(RUTA_PESOS, "w") as f:
        json.dump(pesos, f, indent=4)
    log.info("✅ Pesos guardados.")

def obtener_peso(nombre_estrategia, symbol):
    global PESOS
    if PESOS is None:
        PESOS = cargar_pesos_estrategias()
    if not isinstance(PESOS, dict):
        log.warning(f"⚠️ PESOS no está cargado correctamente. Valor: {PESOS}")
        return 0.0
    return PESOS.get(symbol, {}).get(nombre_estrategia, 0.0)

def calcular_pesos_desde_backtest(simbolos, carpeta="backtesting", escala=20):
    pesos_por_symbol = {}
    for symbol in simbolos:
        ruta = f"{carpeta}/ordenes_{symbol.replace('/', '_')}_resultado.csv"
        if not os.path.exists(ruta):
            log.warning(f"❌ Archivo no encontrado: {ruta}")
            continue
        try:
            df = pd.read_csv(ruta)
        except pd.errors.EmptyDataError:
            log.warning(f"⚠️ Archivo vacío: {ruta}")
            continue

        conteo = defaultdict(int)
        for _, fila in df.iterrows():
            if fila.get("resultado") != "ganancia":
                continue
            try:
                estrategias = json.loads(fila["estrategias_activas"].replace("'", "\""))
            except Exception as e:
                log.warning(f"❌ JSON inválido en fila: {fila['estrategias_activas']}")
                continue

            for estrategia, activa in estrategias.items():
                if activa:
                    conteo[estrategia] += 1

        total = sum(conteo.values())
        if total == 0:
            continue

        normalizados = {k: round(v / total * 10, 2) for k, v in conteo.items()}
        suma_actual = sum(normalizados.values())
        factor = escala / suma_actual if suma_actual > 0 else 1
        reescalados = {k: round(v * factor, 2) for k, v in normalizados.items()}

        pesos_por_symbol[symbol] = reescalados
        log.info(f"📊 {symbol}: {reescalados}")

    guardar_pesos_estrategias(pesos_por_symbol)

