import pandas as pd
from core.logger import configurar_logger
from indicadores.rsi import calcular_rsi

# Umbrales configurables
UMBRAL_PERDIDA_DIA = -0.02  # -2%
UMBRAL_CERCANIA_EXTREMO = 0.003  # 0.3%
UMBRAL_CUERPO_DOJI = 0.3  # cuerpo < 30% del rango total
UMBRAL_RSI_BAJO = 35
UMBRAL_CUERPO_ALCISTA = 0.6

log = configurar_logger("analisis_previo")

def validar_condiciones_tecnicas_extra(symbol: str, df: pd.DataFrame, precio: float, sl: float, tp: float) -> bool:
    """Aplica filtros técnicos adicionales antes de abrir una operación."""
    columnas = {"open", "high", "low", "close", "volume"}
    if df is None or df.empty or not columnas.issubset(df.columns):
        log.info(f"❌ {symbol}: Datos insuficientes para análisis previo")
        return False

    datos = df.tail(60).copy()
    if datos.empty:
        log.info(f"❌ {symbol}: Sin datos recientes para análisis previo")
        return False

    apertura = datos["open"].iloc[0]
    if apertura > 0:
        variacion = (precio - apertura) / apertura
        if variacion <= UMBRAL_PERDIDA_DIA:
            log.info(f"❌ {symbol} pierde más de 2% en el día. Precio: {precio}")
            return False

    maximo = datos["high"].max()
    minimo = datos["low"].min()
    if (maximo - precio) / precio <= UMBRAL_CERCANIA_EXTREMO:
        log.info(f"❌ {symbol} demasiado cerca del máximo. Precio: {precio}")
        return False

    cerca_minimo = (precio - minimo) / precio <= UMBRAL_CERCANIA_EXTREMO
    if cerca_minimo:
        rsi = calcular_rsi(datos)
        vela = datos.iloc[-1]
        rango = vela["high"] - vela["low"]
        cuerpo = vela["close"] - vela["open"]
        vela_fuerte = cuerpo > 0 and rango > 0 and cuerpo / rango >= UMBRAL_CUERPO_ALCISTA
        rebote = (rsi is not None and rsi < UMBRAL_RSI_BAJO) or vela_fuerte
        if not rebote:
            log.info(f"❌ {symbol} cerca del mínimo sin rebote. Precio: {precio}")
            return False

    vela = datos.iloc[-1]
    rango = vela["high"] - vela["low"]
    cuerpo = abs(vela["close"] - vela["open"])
    if rango > 0 and cuerpo / rango <= UMBRAL_CUERPO_DOJI:
        log.info(f"❌ {symbol} vela de indecisión. Precio: {precio}")
        return False

    if len(datos) >= 3:
        ultimos = datos.tail(3)
        closes = ultimos["close"]
        vols = ultimos["volume"]
        if closes.iloc[0] < closes.iloc[1] < closes.iloc[2] and vols.iloc[0] > vols.iloc[1] > vols.iloc[2]:
            log.info(f"❌ {symbol} volumen decreciente con precio al alza. Precio: {precio}")
            return False

    if abs(tp - precio) < abs(precio - sl):
        log.info(f"❌ {symbol} relación TP/SL desfavorable. Precio: {precio}")
        return False

    return True