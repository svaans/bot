import os
import json
import pandas as pd
from collections import defaultdict
from core.pesos import gestor_pesos
from core.adaptador_umbral import calcular_umbral_adaptativo
from core.logger import configurar_logger

CARPETA_OPERACIONES = "ultimas_operaciones"
RUTA_PESOS = "config/estrategias_pesos.json"
MAX_OPERACIONES = 30
MIN_OPERACIONES = 5
# Nuevo tamaño de ventana para actualizar pesos
VENTANA_ACTUALIZACION = 10

log = configurar_logger("trader_simulado", modo_silencioso=True)
os.makedirs(CARPETA_OPERACIONES, exist_ok=True)


def registrar_resultado_trade(symbol: str, orden: dict, ganancia: float):
    archivo = os.path.join(CARPETA_OPERACIONES, symbol.replace("/", "_").upper() + ".parquet")

    # ---------- Leer historial anterior de operaciones ----------
    historial = []
    if os.path.exists(archivo):
        try:
            df_prev = pd.read_parquet(archivo)
            historial = df_prev.to_dict("records")
        except Exception as e:
            print(f"⚠️ Archivo dañado: {archivo} — se sobrescribirá. Error: {e}")
            historial = []

    # ---------- Validación segura del campo 'estrategias_activas' ----------
    estrategias_activas = orden.get("estrategias_activas", {})
    if isinstance(estrategias_activas, str):
        try:
            estrategias_activas = json.loads(estrategias_activas.replace("'", "\""))
        except Exception as e:
            print(f"❌ Error al parsear estrategias activas de {symbol}: {e}")
            estrategias_activas = {}

    # ---------- Agregar la nueva operación ----------
    nueva_operacion = {
        "retorno_total": ganancia,
        "estrategias_activas": estrategias_activas
    }

    historial.append(nueva_operacion)
    historial = historial[-MAX_OPERACIONES:]

    # ---------- Guardar el historial actualizado ----------
    try:
        df_guardar = pd.DataFrame(historial)
        df_guardar.to_parquet(archivo, index=False)
    except Exception as e:
        print(f"❌ Error al guardar historial para {symbol}: {e}")
        return

    if len(historial) >= VENTANA_ACTUALIZACION and len(historial) % VENTANA_ACTUALIZACION == 0:
        ventana = historial[-VENTANA_ACTUALIZACION:]
        actualizar_pesos_dinamicos(symbol, ventana)


def actualizar_pesos_dinamicos(symbol: str, historial: list, factor_ajuste=0.05):
    datos = defaultdict(list)

    # Cargar los pesos actuales
    pesos_actuales = gestor_pesos.obtener_pesos_symbol(symbol)

    # Agrupar retornos por estrategia activa
    for orden in historial:
        estrategias = orden.get("estrategias_activas", {})
        if isinstance(estrategias, str):
            try:
                estrategias = json.loads(estrategias.replace("'", "\""))
            except:
                continue
        retorno = orden.get("retorno_total", 0.0)
        for estrategia, activa in estrategias.items():
            if activa:
                datos[estrategia].append(retorno)

    # Aplicar ajustes solo a estrategias activas
    nuevos_pesos = pesos_actuales.copy()
    for estrategia, retornos in datos.items():
        if len(retornos) < MIN_OPERACIONES:
            continue

        promedio = sum(retornos) / len(retornos)
        winrate = sum(1 for r in retornos if r > 0) / len(retornos)

        peso_anterior = nuevos_pesos.get(estrategia, 0.5)
        peso_objetivo = min(1.0, max(0.0, promedio * winrate))  # entre 0 y 1

        # ✅ Mezcla suave: 98% peso actual, 2% nuevo
        nuevos_pesos[estrategia] = peso_anterior * 0.98 + peso_objetivo * 0.02


    # Guardar
    pesos_totales = gestor_pesos.pesos
    pesos_totales[symbol] = nuevos_pesos
    gestor_pesos.guardar(pesos_totales)

    print(f"\n🧠 Pesos ajustados dinámicamente para {symbol}:")
    for estrategia, peso in nuevos_pesos.items():
        print(f"  - {estrategia}: {peso:.3f}")

    # Calcular y guardar nuevo umbral (si hay al menos una estrategia activa en la última operación)
    try:
        df_fake = pd.DataFrame(historial)
        estrategias = df_fake.iloc[-1].get("estrategias_activas", {})
        if isinstance(estrategias, str):
            estrategias = json.loads(estrategias.replace("'", "\""))
        if estrategias:
            umbral = calcular_umbral_adaptativo(symbol, df_fake, estrategias, nuevos_pesos)
            print(f"📈 Umbral estimado para {symbol}: {umbral:.2f}")
    except Exception as e:
        print(f"❌ Error al recalcular/guardar umbral para {symbol}: {e}")

