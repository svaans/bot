from core.pesos import gestor_pesos
from estrategias_entrada.loader import cargar_estrategias
from core.estrategias import obtener_estrategias_por_tendencia
from indicadores.volumen import verificar_volumen_suficiente
from filtros.validador_entradas import verificar_liquidez_orden
from indicadores.correlacion import calcular_correlacion
from indicadores.divergencia_rsi import detectar_divergencia_alcista
from indicadores.bollinger import calcular_bollinger
from indicadores.slope import calcular_slope
from indicadores.momentum import calcular_momentum
from indicadores.rsi import calcular_rsi
from core.logger import configurar_logger
from collections import Counter
from pathlib import Path
import pandas as pd
import traceback
import json

log = configurar_logger("entradas")

# Cache de funciones cargadas
ESTRATEGIAS_DISPONIBLES = cargar_estrategias()

# Cache de rendimientos históricos por símbolo y estrategia
_RENDIMIENTOS_CACHE: dict | None = None


def _cargar_rendimientos() -> dict:
    """Carga el archivo de rendimiento histórico de estrategias."""
    global _RENDIMIENTOS_CACHE
    if _RENDIMIENTOS_CACHE is not None:
        return _RENDIMIENTOS_CACHE

    ruta = Path("datos/estrategias_por_rendimiento.json")
    if ruta.exists():
        with open(ruta, "r", encoding="utf-8") as fh:
            _RENDIMIENTOS_CACHE = json.load(fh)
    else:
        _RENDIMIENTOS_CACHE = {}
    return _RENDIMIENTOS_CACHE


def _categoria(nombre: str) -> str:
    n = nombre.lower()
    if "volumen" in n:
        return "volumen"
    if any(k in n for k in ["rsi", "macd", "momentum", "adx", "stoch", "ichimoku", "atr"]):
        return "momentum"
    if any(k in n for k in ["ema", "sma", "vwap", "media"]):
        return "medias"
    if any(k in n for k in [
        "triangle",
        "wedge",
        "flag",
        "rectangle",
        "scallop",
        "cup",
        "bottom",
        "top",
        "pennant",
        "diamond",
    ]):
        return "chartismo"
    return "otro"


def _tiene_reversion(nombre: str) -> bool:
    n = nombre.lower()
    return any(k in n for k in ["bottom", "top", "inverted", "scallop", "head_and_shoulders"])


def validar_tecnica_entrada(
    df: pd.DataFrame,
    estrategias_activas: dict,
    symbol: str,
    direccion: str = "long",
    tendencia: str | None = None,
) -> bool:
    """Valida múltiples criterios técnicos antes de ejecutar la entrada."""
    if df is None or len(df) < 30:
        return True

    close_actual = float(df["close"].iloc[-1])
    volumen_actual = float(df["volume"].iloc[-1])
    volumen_prom_30 = float(df["volume"].rolling(30).mean().iloc[-1])

    if volumen_prom_30 > 0 and (volumen_actual / volumen_prom_30) < 1.05:
        log.info(f"🚫 {symbol} volumen relativo bajo")
        return False

    slope = calcular_slope(df, 10)
    momentum = calcular_momentum(df, 10)
    if tendencia == "bajista" and slope > 0:
        log.info(f"🚫 {symbol} slope incompatible con tendencia bajista {slope:.4f}")
        return False
    if tendencia == "alcista" and slope < 0:
        log.info(f"🚫 {symbol} slope incompatible con tendencia alcista {slope:.4f}")
        return False
    if abs(slope) < 0.03:
        log.info(f"🚫 {symbol} slope débil {slope:.4f}")
        return False
    if momentum is not None:
        if tendencia == "bajista" and momentum > 0:
            log.info(f"🚫 {symbol} momentum alcista en tendencia bajista {momentum:.4f}")
            return False
        if tendencia == "alcista" and momentum < 0:
            log.info(f"🚫 {symbol} momentum bajista en tendencia alcista {momentum:.4f}")
            return False

    rsi = calcular_rsi(df, 14)
    if rsi is not None:
        if rsi > 70:
            log.info(f"🚫 {symbol} RSI alto {rsi:.2f}")
            return False
        if direccion == "short" and rsi < 30:
            log.info(f"🚫 {symbol} RSI bajo {rsi:.2f} para short")
            return False

    _, banda_sup, _ = calcular_bollinger(df)
    if banda_sup is not None and abs(banda_sup - close_actual) / close_actual < 0.015:
        log.info(f"🚫 {symbol} muy cerca de resistencia")
        return False

    categorias = [_categoria(n) for n, a in estrategias_activas.items() if a]
    if categorias:
        conteo = Counter(categorias)
        if max(conteo.values()) / len(categorias) > 0.6:
            log.info(f"🚫 {symbol} poca diversidad real de estrategias")
            return False

    if any(_tiene_reversion(n) for n, a in estrategias_activas.items() if a):
        if len(df) >= 5:
            close_5 = float(df["close"].iloc[-5])
            if close_5 != 0 and (close_5 - close_actual) / close_5 < 0.015:
                log.info(f"🚫 {symbol} sin caída previa suficiente para reversión")
                return False

    rendimientos = _cargar_rendimientos().get(symbol, {})
    activas = [n for n, a in estrategias_activas.items() if a]
    if activas and rendimientos:
        total = sum(rendimientos.get(n, 1.0) for n in activas)
        if total / len(activas) < 0.8:
            log.info(f"🚫 {symbol} rendimiento histórico bajo")
            return False

    return True


def validar_volumen(df, direccion: str, cantidad: float = 0.0) -> bool:
    """Valida el volumen y la liquidez antes de permitir una entrada."""
    if df is None:
        return True
    if not verificar_volumen_suficiente(df):
        return False
    if cantidad > 0 and not verificar_liquidez_orden(df, cantidad, factor=0.3):
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
    cantidad=0.0,
    df_referencia=None,
    umbral_correlacion: float = 0.9,
    tendencia: str | None = None,
    score: float | None = None,
    persistencia: float = 0.0,
    persistencia_minima: float = 0.0,
):
    """
    Evalúa si se permite una entrada con base en:
    - Potencia vs Umbral
    - Diversidad de estrategias activas
    - Condiciones técnicas adicionales
    """
    estrategias_activas_count = sum(1 for v in estrategias_activas.values() if v)

    if df is not None and not validar_volumen(df, direccion, cantidad):
        log.info(f"📉 Entrada evitada por volumen insuficiente o divergencia en {symbol}")
        return False
    
    if df is not None and df_referencia is not None:
        corr = calcular_correlacion(df, df_referencia)
        if corr is not None and abs(corr) >= umbral_correlacion:
            log.info(f"🚫 {symbol} correlación alta {corr:.2f}")
            return False
    
    if df is not None and not validar_tecnica_entrada(
        df, estrategias_activas, symbol, direccion, tendencia
    ):
        log.info(f"🔴 [{symbol}] Entrada rechazada por validación técnica previa")
        return False

    if potencia >= umbral:
        log.info(f"🟢 [{symbol}] Entrada directa permitida: {potencia:.2f} >= {umbral:.2f}")
        return True

    if (
        potencia >= umbral * 0.9
        and estrategias_activas_count >= 4
        and rsi > 55
        and (
            (tendencia == "bajista" and slope < 0)
            or (tendencia == "alcista" and slope > 0)
            or tendencia is None
        )
        and momentum > 0.0004
    ):
        log.info(
            f"🟡 [{symbol}] Entrada validada por criterios técnicos. Potencia: {potencia:.2f} < Umbral: {umbral:.2f}"
        )
        return True

    if score is not None and score >= 3.5 and persistencia >= persistencia_minima:
        log.info(
            f"🟢 [{symbol}] Entrada permitida por score {score:.2f} y persistencia {persistencia:.2f}"
        )
        return True

    log.info(f"🔴 [{symbol}] Entrada descartada. Potencia: {potencia:.2f} < Umbral: {umbral:.2f}")
    return False




