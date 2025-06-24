import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator
from core.utils.utils import configurar_logger
from indicators.slope import calcular_slope

log = configurar_logger("persistencia")


def calcular_persistencia_minima(symbol: str, df: pd.DataFrame, tendencia: str, base_minimo: float = 1.0) -> float:
    """Calcula un umbral de persistencia adaptativo según volatilidad, pendiente y RSI."""
    if df is None or len(df) < 10 or "close" not in df.columns:
        log.debug(f"⚠️ [{symbol}] Datos insuficientes o inválidos para persistencia adaptativa")
        return base_minimo

    ventana = df["close"].tail(10)
    media = np.mean(ventana)
    if np.isnan(media) or media == 0:
        log.debug(f"⚠️ [{symbol}] Media de cierre inválida. Usando mínimo base {base_minimo}")
        return base_minimo

    volatilidad = np.std(ventana) / media
    slope = calcular_slope(df, periodo=10)

    try:
        rsi = RSIIndicator(close=df["close"], window=14).rsi().iloc[-1]
    except Exception as e:  # noqa: BLE001
        log.debug(f"⚠️ [{symbol}] Error calculando RSI: {e}")
        rsi = 50

    # Base inicial con ajuste por volatilidad
    minimo = base_minimo * (1 + volatilidad * 0.5)

    # Ajuste por tipo de tendencia
    if tendencia == "lateral":
        minimo += 0.5
    if (tendencia == "alcista" and slope > 0.002) or (
        tendencia == "bajista" and slope < -0.002
    ):
        minimo = max(minimo - 0.2, 0.5)

    # Ajuste por sobrecompra o sobreventa
    if rsi > 70 or rsi < 30:
        minimo += 0.2

    # Límite de persistencia final
    minimo = round(max(0.5, min(minimo, 5.0)), 2)

    log.debug(
        f"[{symbol}] Persistencia adaptada: Base {base_minimo:.2f} -> {minimo:.2f} | "
        f"Vol {volatilidad:.4f} | Slope {slope:.4f} | RSI {rsi:.2f}"
    )
    return minimo

