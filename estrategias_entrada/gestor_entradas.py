from core.pesos import gestor_pesos
from estrategias_entrada.loader import cargar_estrategias
from core.estrategias import obtener_estrategias_por_tendencia
from indicadores.volumen import verificar_volumen_suficiente
from indicadores.divergencia_rsi import detectar_divergencia_alcista
from core.logger import configurar_logger
import traceback

log = configurar_logger("entradas")

# Cache de funciones cargadas
ESTRATEGIAS_DISPONIBLES = cargar_estrategias()


def validar_volumen(df, direccion: str) -> bool:
    """Valida el volumen antes de permitir una entrada."""
    if df is None:
        return True
    if not verificar_volumen_suficiente(df):
        return False
    if direccion == "short" and detectar_divergencia_alcista(df):
        return False
    return True
    
def evaluar_estrategias(symbol, df, tendencia):
    """
    Evalúa todas las estrategias activas según la tendencia y calcula:
    - Puntaje total (suma de pesos de estrategias activadas)
    - Diversidad (cantidad de estrategias activadas con peso > 0)
    - Diccionario de estrategias activas

    Retorna un dict con: puntaje_total, estrategias_activas, diversidad
    """

    global ESTRATEGIAS_DISPONIBLES
    if not ESTRATEGIAS_DISPONIBLES:
        ESTRATEGIAS_DISPONIBLES = cargar_estrategias()

    estrategias_candidatas = obtener_estrategias_por_tendencia(tendencia)
    estrategias_activadas = {}
    puntaje_total = 0.0

    for nombre in estrategias_candidatas:
        funcion = ESTRATEGIAS_DISPONIBLES.get(nombre)

        if not funcion:
            log.warning(f"⚠️ Estrategia no encontrada: {nombre}")
            continue

        try:
            resultado = funcion(df)

            if resultado is None:
                log.warning(f"🛑 {nombre} devolvió None.")
                continue
            if not isinstance(resultado, dict):
                log.warning(f"🟡 {nombre} devolvió tipo inválido: {type(resultado)} — Valor: {resultado}")
                continue
            if "activo" not in resultado:
                log.warning(f"🔴 {nombre} no contiene clave 'activo'. Resultado: {resultado}")
                continue

            activo = resultado.get("activo", False)
            estrategias_activadas[nombre] = activo

            if activo:
                peso = gestor_pesos.obtener_peso(nombre, symbol)
                puntaje_total += peso

        except Exception as e:
            log.error(f"❌ Excepción al evaluar {nombre}: {e}")
            log.debug(traceback.format_exc())
            continue

    # Calcular diversidad real ignorando los pesos para evitar que la falta de
    # datos en ``estrategias_pesos.json`` reduzca la cuenta a 1
    diversidad = sum(1 for activo in estrategias_activadas.values() if activo)
    

    return {
        "puntaje_total": round(puntaje_total, 2),
        "estrategias_activas": estrategias_activadas,
        "diversidad": diversidad
    }


def entrada_permitida(
    symbol,
    potencia,
    umbral,
    estrategias_activas,
    rsi,
    slope,
    momentum,
    df=None,
    direccion="long",
):
    """
    Evalúa si se permite una entrada con base en:
    - Potencia vs Umbral
    - Diversidad de estrategias activas
    - Condiciones técnicas adicionales
    """
    estrategias_activas_count = sum(1 for v in estrategias_activas.values() if v)

    if df is not None and not validar_volumen(df, direccion):
        log.info(f"📉 Entrada evitada por volumen insuficiente o divergencia en {symbol}")
        return False

    if potencia >= umbral:
        log.info(f"🟢 [{symbol}] Entrada directa permitida: {potencia:.2f} >= {umbral:.2f}")
        return True

    if (
        potencia >= umbral * 0.95 and
        estrategias_activas_count >= 5 and
        rsi > 55 and
        slope > 0 and
        momentum > 0.0004
    ):
        log.info(f"🟡 [{symbol}] Entrada validada por criterios técnicos. Potencia: {potencia:.2f} < Umbral: {umbral:.2f}")
        return True

    log.info(f"🔴 [{symbol}] Entrada descartada. Potencia: {potencia:.2f} < Umbral: {umbral:.2f}")
    return False




