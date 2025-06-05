
from core.pesos import gestor_pesos
from estrategias_entrada.loader import cargar_estrategias
from core.estrategias import obtener_estrategias_por_tendencia
import traceback

ESTRATEGIAS_DISPONIBLES = cargar_estrategias()
]

def evaluar_estrategias(symbol, df, tendencia):
    # Evalúa las estrategias relevantes según la tendencia detectada.
    # Retorna el puntaje total y las estrategias activadas.

    global ESTRATEGIAS_DISPONIBLES
    if not ESTRATEGIAS_DISPONIBLES:
        ESTRATEGIAS_DISPONIBLES = cargar_estrategias()

    activas = obtener_estrategias_por_tendencia(tendencia)

    puntaje_total = 0
    estrategias_activadas = {}

    for nombre in activas:
        funcion = ESTRATEGIAS_DISPONIBLES.get(nombre)
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
                peso = gestor_pesos.obtener_peso(nombre, symbol)
                puntaje_total += peso

        except Exception as e:
            print(f"❌ Excepción en {nombre}: {e}")
            print(traceback.format_exc())
            continue

    return {
        "puntaje_total": round(puntaje_total, 2),
        "estrategias_activas": estrategias_activadas
    }

from core.logger import configurar_logger

log = configurar_logger("entradas")

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

    log.debug(f"🔴 [{symbol}] Entrada rechazada. Potencia: {potencia:.2f} < Umbral: {umbral:.2f}")
    return False



