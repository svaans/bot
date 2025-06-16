# estrategias_salida/gestor_salidas.py
import pandas as pd
from core.utils import validar_dataframe
from core.tendencia import detectar_tendencia
from estrategias_entrada.gestor_entradas import evaluar_estrategias
from core.adaptador_umbral import calcular_umbral_adaptativo
from core.logger import configurar_logger

log = configurar_logger("gestor_salidas")

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
    symbol = orden.get("symbol", "SYM")
    if not validar_dataframe(df, ["close", "high", "low", "volume"]):
        log.warning(f"[{symbol}] DataFrame inválido para gestor de salidas")
        return {"cerrar": False, "razon": "Datos insuficientes"}
        
    funciones = cargar_estrategias_salida()
    resultados = []
    PRIORIDAD_ABSOLUTA = {"Stop Loss", "Estrategia: Cambio de tendencia"}

    for f in funciones:
        if not callable(f):
            continue  # Evita errores por strings u objetos incorrectos

        try:
           
            params = f.__code__.co_varnames
            if "symbol" in params and "orden" in params and "config" in params:
                resultado = f(symbol, orden, df, config=config)
            elif "symbol" in params and "orden" in params:
                resultado = f(symbol, orden, df)
            elif "symbol" in params and "config" in params:
                resultado = f(symbol, df, config=config)
            elif "symbol" in params:
                resultado = f(symbol, df)
            elif "orden" in params and "config" in params:
                resultado = f(orden, df, config=config)
            elif "orden" in params:
                resultado = f(orden, df)
            elif "config" in params:
                resultado = f(df, config=config)
            else:
                resultado = f(df)
        except Exception as e:
                        log.warning(f"❌ Error ejecutando estrategia de salida: {f} → {e}")
                        continue

        if resultado.get("cerrar", False):
            razon = resultado.get("razon", "Sin motivo")
            if razon in PRIORIDAD_ABSOLUTA:
                log.info(f"[{symbol}] Cierre prioritario por {razon}")
                return {"cerrar": True, "razon": razon}
            resultados.append(razon)

    tendencia, _ = detectar_tendencia(symbol, df)
    volatilidad = df["close"].pct_change().tail(10).std() if "close" in df else 0.0
    factor_vol = 1 + min(volatilidad * 50, 0.5)
    factor_tend = 1.2 if tendencia in {"alcista", "bajista"} else 1.0
    factor_sig = 1 + len(resultados) / 10
    pesos_dinamicos = {k: v * factor_vol * factor_tend * factor_sig for k, v in PESOS_SALIDAS.items()}

    riesgo_total = sum(pesos_dinamicos.get(r, 1) for r in resultados)
    if riesgo_total >= UMBRAL_CIERRE:
        return {
            "cerrar": True,
            "razon": f"{', '.join(resultados)} (Riesgo total: {riesgo_total:.2f})"
        }

    return {
        "cerrar": False,
        "razon": f"Riesgo insuficiente ({riesgo_total:.2f})"
    }

def verificar_filtro_tecnico(symbol, df, estrategias_activas, pesos_symbol, config=None):
    if not validar_dataframe(df, ["high", "low", "close"]):
        return False

    tendencia, _ = detectar_tendencia(symbol, df)
    evaluacion = evaluar_estrategias(symbol, df, tendencia)
    if not evaluacion:
        return False

    activas = [k for k, v in evaluacion["estrategias_activas"].items() if v]
    puntaje = evaluacion["puntaje_total"]
    umbral = calcular_umbral_adaptativo(
        symbol,
        df,
        evaluacion["estrategias_activas"],
        pesos_symbol,
        persistencia=0.0,
        config=config,
    )

    return len(activas) >= 1 and puntaje >= 0.4 * umbral


