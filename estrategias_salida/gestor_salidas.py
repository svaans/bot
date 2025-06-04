# estrategias_salida/gestor_salidas.py
import pandas as pd
from core.utils import validar_dataframe
from core.tendencia import detectar_tendencia
from estrategias_entrada.gestor_entradas import evaluar_estrategias
from core.adaptador_umbral import calcular_umbral_adaptativo

from estrategias_salida.loader_salidas import cargar_estrategias_salida

# Ponderación de riesgo por estrategia de salida
PESOS_SALIDAS = {
    "Stop Loss": 10,
    "Trailing Stop": 8,
    "Estrategia: Cambio de tendencia": 6,
    "Estrategia: Cruce bajista de MACD": 5,
    "Estrategia: RSI en sobrecompra o sobreventa": 4,
    "Estrategia: Tiempo máximo alcanzado": 3,
    "Estrategia: Take Profit por ATR": 2,
    "Estrategia: Stop Loss por ATR": 6,
}

UMBRAL_CIERRE = 8  # Puntos acumulados para cerrar

def evaluar_salidas(orden: dict, df, config=None):
    funciones = cargar_estrategias_salida()
    riesgo_total = 0
    motivos = []

    for f in funciones:
        if not callable(f):
            continue  # Evita errores por strings u objetos incorrectos

        try:
            # Detectar si la función acepta 'config' como parámetro
            params = f.__code__.co_varnames
            if "orden" in params and "config" in params:
                resultado = f(orden, df, config=config)
            elif "orden" in params:
                resultado = f(orden, df)
            elif "config" in params:
                resultado = f(df, config=config)
            else:
                resultado = f(df)
        except Exception as e:
            print(f"❌ Error ejecutando estrategia de salida: {f} → {e}")
            continue

        if resultado.get("cerrar", False):
            razon = resultado.get("razon", "Sin motivo")
            motivos.append(razon)
            riesgo = PESOS_SALIDAS.get(razon, 1)
            riesgo_total += riesgo

    if riesgo_total >= UMBRAL_CIERRE:
        return {
            "cerrar": True,
            "razon": f"{', '.join(motivos)} (Riesgo total: {riesgo_total})"
        }

    return {
        "cerrar": False,
        "razon": f"Riesgo insuficiente ({riesgo_total})"
    }

def verificar_filtro_tecnico(symbol, df, estrategias_activas, pesos_symbol):
    if not validar_dataframe(df, ["high", "low", "close"]):
        return False

    tendencia, _ = detectar_tendencia(symbol, df)
    evaluacion = evaluar_estrategias(symbol, df, tendencia)
    if not evaluacion:
        return False

    activas = [k for k, v in evaluacion["estrategias_activas"].items() if v]
    puntaje = evaluacion["puntaje_total"]
    umbral = calcular_umbral_adaptativo(symbol, df, evaluacion["estrategias_activas"], pesos_symbol)

    return len(activas) >= 2 and puntaje >= 0.5 * umbral


