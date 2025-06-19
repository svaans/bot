import json
import os
import pandas as pd
from indicadores.rsi import calcular_rsi
from core.logger import configurar_logger

log = configurar_logger("eval_tecnico")

RUTA_PESOS = "config/pesos_tecnicos.json"
PESOS_DEFECTO = {
    "rsi": 1.0,
    "volumen": 1.0,
    "tp_sl": 0.7,
    "no_doji": 0.5,
    "no_sobrecompra": 1.0,
    "cuerpo_sano": 1.0,
    "rsi_creciente": 0.6,
    "volumen_creciente": 0.5,
    "sin_mecha_sup_larga": 0.4,
    "distancia_extremos": 0.6,
}

_pesos_cache: dict | None = None


def _cargar_pesos(symbol: str) -> dict:
    """Devuelve los pesos técnicos para ``symbol``.

    Si ``symbol`` no está presente en el archivo JSON se devuelven los valores
    por defecto. Además, se rellenan claves faltantes con ``PESOS_DEFECTO``.
    """

    global _pesos_cache
    if _pesos_cache is None:
        if os.path.exists(RUTA_PESOS):
            try:
                with open(RUTA_PESOS, "r", encoding="utf-8") as fh:
                    _pesos_cache = json.load(fh)
            except Exception as e:
                log.warning(
                    f"Error leyendo {RUTA_PESOS}: {e}. Usando pesos por defecto"
                )
                _pesos_cache = {}
        else:
            _pesos_cache = {}

    datos_simbolo = _pesos_cache.get(symbol) or _pesos_cache.get("default")
    if not isinstance(datos_simbolo, dict):
        datos_simbolo = {}

    pesos = PESOS_DEFECTO.copy()
    pesos.update(datos_simbolo)
    return pesos


def cargar_pesos_tecnicos(symbol: str) -> dict:
    """Interfaz pública para obtener los pesos de un símbolo."""
    return _cargar_pesos(symbol)


def evaluar_puntaje_tecnico(symbol: str, df: pd.DataFrame, precio: float, sl: float, tp: float) -> dict:
    """Evalúa condiciones técnicas y retorna un puntaje acumulado."""
    pesos = _cargar_pesos(symbol)
    if df is None or len(df) < 30:
        log.warning(f"[{symbol}] datos insuficientes para score tecnico")
        return {"score_total": 0.0, "detalles": {}}

    df = df.tail(60).copy()
    vela = df.iloc[-1]
    cierre = float(vela["close"])
    apertura = float(vela["open"])
    alto = float(vela["high"])
    bajo = float(vela["low"])
    cuerpo = abs(cierre - apertura)
    rango_total = alto - bajo

    rsi = calcular_rsi(df)
    rsi_ant = calcular_rsi(df.iloc[:-1]) if len(df) > 15 else rsi
    volumen_actual = float(vela["volume"])
    volumen_prev = float(df.iloc[-2]["volume"])
    media_vol = df["volume"].rolling(20).mean().iloc[-1]

    detalles: dict[str, float] = {}
    total = 0.0

    def _add(clave: str, condicion: bool):
        nonlocal total
        puntos = pesos.get(clave, 0.0) if condicion else 0.0
        detalles[clave] = float(puntos)
        total += puntos

    _add("rsi", rsi is not None and 40 <= rsi <= 70)
    _add("volumen", media_vol > 0 and volumen_actual > media_vol)
    ratio = (tp - precio) / (precio - sl) if precio != sl else 0
    _add("tp_sl", ratio >= 1.2)
    _add("no_doji", rango_total > 0 and cuerpo / rango_total >= 0.3)
    _add("no_sobrecompra", rsi is None or rsi < 75)
    _add("cuerpo_sano", cierre > apertura and cuerpo >= 0.6 * rango_total)
    _add("rsi_creciente", rsi is not None and rsi_ant is not None and rsi > rsi_ant)
    _add("volumen_creciente", volumen_actual > volumen_prev)
    mecha_sup = alto - max(cierre, apertura)
    _add("sin_mecha_sup_larga", mecha_sup <= 2 * cuerpo)
    max_dia = df["high"].max()
    min_dia = df["low"].min()
    distancia_max = (max_dia - precio) / max_dia if max_dia else 1
    distancia_min = (precio - min_dia) / min_dia if min_dia else 1
    _add("distancia_extremos", distancia_max > 0.002 and distancia_min > 0.002)

    log.info(f"[ENTRY ANALYSIS] {symbol}")
    for k, v in detalles.items():
        log.info(f"- {k}: {'✅' if v else '❌'} (+{v})")
    log.info(f"- Total score: {total:.2f} / {sum(pesos.values()):.2f}")

    return {"score_total": round(total, 2), "detalles": detalles}


def calcular_umbral_adaptativo(score_maximo_esperado: float, tendencia: str, volatilidad: float, volumen: float, estrategias_activas: dict) -> float:
    """Calcula un umbral técnico dinámico simple."""
    base = score_maximo_esperado * 0.5
    if tendencia in {"alcista", "bajista"} and volumen > 1:
        base *= 0.9
    if volatilidad > 0.02:
        base *= 1.2
    if estrategias_activas and len([v for v in estrategias_activas.values() if v]) < 3:
        base *= 1.1
    return round(base, 2)