import importlib
from core.pesos import obtener_peso
from estrategias_entrada.loader import cargar_estrategias
import traceback

# Categorías de estrategias por tipo de tendencia
estrategias_alcistas = [
    "estrategia_rsi", "estrategia_macd", "estrategia_cruce_medias", "estrategia_ema",
    "estrategia_momentum", "flag_alcista", "pennant", "cup_with_handle",
    "ascending_triangle", "symmetrical_triangle_up", "measured_move_up",
    "ascending_scallop", "three_rising_valleys", "double_bottom", "diamond_bottom",
    "triple_bottom", "wedge_breakout", "measured_move_up"
]

estrategias_bajistas = [
    "estrategia_rsi_invertida", "cruce_medias_bajista", "estrategia_sma_bajista",
    "flag_bajista", "pennant_bajista", "inverted_cup_with_handle",
    "descending_triangle", "symmetrical_triangle_down", "measured_move_down",
    "descending_scallop", "three_descending_peaks", "head_and_shoulders", "triple_top"
]

estrategias_laterales = [
    "estrategia_rango", "estrategia_sma", "estrategia_adx", "estrategia_bollinger_breakout",
    "estrategia_estocastico", "estrategia_volumen_alto", "estrategia_cruce_ema_stochrsi",
    "estrategia_vwap_breakout", "estrategia_atr_breakout", "estrategia_ichimoku_breakout",
    "estrategia_divergencia_rsi", "estrategia_macd_hist_inversion", "tops_rectangle",
    "volatility_breakout"
]

def evaluar_estrategias(symbol, df, tendencia):
    # Evalúa las estrategias relevantes según la tendencia detectada.
    # Retorna el puntaje total y las estrategias activadas.

    estrategias_disponibles = cargar_estrategias()

    if tendencia == "alcista":
        activas = estrategias_alcistas
    elif tendencia == "bajista":
        activas = estrategias_bajistas
    else:
        activas = estrategias_laterales

    puntaje_total = 0
    estrategias_activadas = {}

    for nombre in activas:
        funcion = estrategias_disponibles.get(nombre)
        if funcion is None:
            print(f"⚠️ Estrategia no encontrada: {nombre}")
            continue
        try:
            resultado = funcion(df)

            if resultado is None:
                print(f"🛑 {nombre} devolvió None.")
                print(f"   Última fila del df:\n{df.tail(1)}")
                continue
            if not isinstance(resultado, dict):
                print(f"🟡 {nombre} devolvió un tipo inválido: {type(resultado)} – Valor: {resultado}")
                continue
            if "activo" not in resultado:
                print(f"🔴 {nombre} no contiene clave 'activo'. Resultado: {resultado}")
                continue

            activo = resultado.get("activo", False)
            estrategias_activadas[nombre] = activo
            if activo:
                peso = obtener_peso(nombre, symbol)
                puntaje_total += peso

        except Exception as e:
            print(f"❌ Excepción en {nombre}: {e}")
            print(traceback.format_exc())
            continue

    return {
        "puntaje_total": round(puntaje_total, 2),
        "estrategias_activas": estrategias_activadas
    }
import logging

log = logging.getLogger("entradas")

def entrada_permitida(symbol, potencia, umbral, estrategias_activas, rsi, slope, momentum):
    """
    Evalúa si una entrada debe permitirse según potencia, umbral y criterios técnicos.
    """
    estrategias_activas_count = sum(1 for v in estrategias_activas.values() if v)

    if potencia >= umbral:
        log.info(f"🟢 [{symbol}] Entrada directa permitida: {potencia:.2f} >= {umbral:.2f}")
        return True

    if (
        potencia >= umbral * 0.85 and
        estrategias_activas_count >= 4 and
        rsi > 55 and
        slope > 0 and
        momentum > 0.0004
    ):
        log.info(f"🟡 [{symbol}] Entrada validada por criterios técnicos. Potencia: {potencia:.2f} < Umbral: {umbral:.2f}")
        return True

    log.info(f"🔴 [{symbol}] Entrada rechazada. Potencia: {potencia:.2f} < Umbral: {umbral:.2f}")
    return False



