import pandas as pd
from estrategias_entrada.gestor_entradas import evaluar_estrategias
from core.estrategias import obtener_estrategias_por_tendencia

# ------------------ DETECCIÓN DE TENDENCIA ------------------

def detectar_tendencia(symbol, df: pd.DataFrame):
    if len(df) < 50:
        tendencia = "lateral"
    else:
        df = df.copy()
        df["sma_fast"] = df["close"].rolling(window=10).mean()
        df["sma_slow"] = df["close"].rolling(window=30).mean()

        sma_fast = df["sma_fast"].iloc[-1]
        sma_slow = df["sma_slow"].iloc[-1]

        delta = sma_fast - sma_slow
        umbral = df["close"].std() * 0.05

        if abs(delta) < umbral:
            tendencia = "lateral"
        elif delta > 0:
            tendencia = "alcista"
        else:
            tendencia = "bajista"

    estrategias = obtener_estrategias_por_tendencia(tendencia)

    # 🔧 Asegurar que siempre sea dict[str, bool]
    if isinstance(estrategias, list):
        estrategias_activas = {nombre: True for nombre in estrategias}
    elif isinstance(estrategias, dict):
        estrategias_activas = estrategias
    else:
        estrategias_activas = {}

    return tendencia, estrategias_activas


def obtener_parametros_persistencia(tendencia: str, volatilidad: float):
    """
    Ajusta los requisitos de persistencia según la tendencia y volatilidad actual.
    """
    if tendencia == "lateral":
        return 0.6, 3
    elif volatilidad > 0.02:
        return 0.4, 1
    elif tendencia in ["alcista", "bajista"] and volatilidad > 0.01:
        return 0.45, 2
    else:
        return 0.5, 2

def señales_repetidas(buffer, estrategias_func, tendencia_actual, volatilidad_actual, ventanas=3):
    """
    Evalúa cuántas de las últimas `ventanas` velas tienen estrategias activas con buen peso,
    ajustando los requisitos según la tendencia y la volatilidad.
    """
    if len(buffer) < ventanas + 30:
        return 0

    peso_minimo, min_estrategias = obtener_parametros_persistencia(tendencia_actual, volatilidad_actual)

    contador = 0
    df = pd.DataFrame(buffer[-(ventanas + 30):])
    peso_max = sum(estrategias_func.values()) or 1

    for i in range(-ventanas, 0):
        try:
            ventana = df.iloc[i - 30:i]
            if ventana.empty or len(ventana) < 10:
                continue

            symbol = df.iloc[i]["symbol"]
            tendencia, _ = detectar_tendencia(symbol, ventana)
            evaluacion = evaluar_estrategias(symbol, ventana, tendencia)
            if not evaluacion:
                continue

            estrategias_activas = evaluacion["estrategias_activas"]
            estrategias_validas = [
                k for k, v in estrategias_activas.items()
                if v and estrategias_func.get(k, 0) >= peso_minimo * peso_max
            ]

            if len(estrategias_validas) >= min_estrategias:
                contador += 1

        except Exception:
            continue

    return contador

