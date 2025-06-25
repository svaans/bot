from indicators.rsi import calcular_rsi
from indicators.macd import calcular_macd

def validar_necesidad_de_salida(
    df, orden, estrategias_activas: dict, puntaje=None, umbral=None, config: dict | None = None
) -> float:
    """Evalúa si una posición debe mantenerse abierta.

    Aplica filtros basados en RSI y MACD para evitar cierres prematuros.
    """
    try:
        rsi = calcular_rsi(df)
        umbral_rsi = config.get("umbral_rsi_salida", 55) if config else 55
        factor_umbral_puntaje = (
            config.get("factor_umbral_validacion_salida", 0.8) if config else 0.8
        )
        if orden.get("direccion") == "long" and rsi is not None and rsi > umbral_rsi:
            if puntaje and umbral and puntaje > factor_umbral_puntaje * umbral:
                return 0.0  # ✅ RSI sugiere seguir en el trade

        # Validación MACD: cruce alcista reciente
        macd_prev, signal_prev, _ = calcular_macd(df.iloc[:-1])
        macd_actual, signal_actual, _ = calcular_macd(df)
        if None not in (macd_prev, signal_prev, macd_actual, signal_actual):
            if macd_prev < signal_prev and macd_actual > signal_actual:
                return 0.0  # ✅ Confirmación alcista

    except Exception as e:  # pragma: no cover - logueo simple
        print(f"⚠️ Error evaluando filtros de salida: {e}")

    return 1.0

