import os
import json
import pandas as pd
from collections import defaultdict
from core.pesos import guardar_pesos_estrategias, cargar_pesos_estrategias
from core.adaptador_umbral import calcular_umbral_adaptativo, guardar_umbral_optimo
from core.logger import configurar_logger

CARPETA_OPERACIONES = "ultimas_operaciones"
RUTA_PESOS = "config/estrategias_pesos.json"
MAX_OPERACIONES = 30
MIN_OPERACIONES = 5

log = configurar_logger("trader_simulado", modo_silencioso=True)
os.makedirs(CARPETA_OPERACIONES, exist_ok=True)


def registrar_resultado_trade(symbol: str, orden: dict, ganancia: float):
    archivo = os.path.join(CARPETA_OPERACIONES, symbol.replace("/", "_").upper() + ".json")

    # ---------- Leer historial anterior de operaciones ----------
    historial = []
    if os.path.exists(archivo):
        try:
            with open(archivo, "r", encoding="utf-8") as f:
                historial = json.load(f)
                if not isinstance(historial, list):
                    raise ValueError("El historial debe ser una lista de operaciones.")
        except (json.JSONDecodeError, ValueError) as e:
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
        with open(archivo, "w", encoding="utf-8") as f:
            json.dump(historial, f, indent=2)
    except Exception as e:
        print(f"❌ Error al guardar historial para {symbol}: {e}")
        return

    # ---------- Entrenamiento: actualizar pesos si hay suficientes operaciones ----------
    if len(historial) >= MIN_OPERACIONES:
        actualizar_pesos_dinamicos(symbol, historial)


def actualizar_pesos_dinamicos(symbol: str, historial: list, factor_ajuste=0.05):
    datos = defaultdict(list)

    # Cargar los pesos actuales
    pesos_actuales = cargar_pesos_estrategias().get(symbol, {})

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
    pesos_totales = cargar_pesos_estrategias()
    pesos_totales[symbol] = nuevos_pesos
    guardar_pesos_estrategias(pesos_totales)

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
            guardar_umbral_optimo(symbol, umbral)
            print(f"📥 Umbral óptimo guardado para {symbol}: {umbral:.2f}")
    except Exception as e:
        print(f"❌ Error al recalcular/guardar umbral para {symbol}: {e}")

