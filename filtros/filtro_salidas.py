from indicadores.rsi import calcular_rsi
from indicadores.macd import calcular_macd

def validar_necesidad_de_salida(df, orden, estrategias_activas: dict, puntaje=None, umbral=None, config: dict = None) -> bool:
    try:
        rsi = calcular_rsi(df)
        umbral_rsi = config.get("umbral_rsi_salida", 55) if config else 55
        factor_umbral_puntaje = config.get("factor_umbral_validacion_salida", 0.8) if config else 0.8

        if orden.get("direccion") == "long" and rsi is not None and rsi[-1] > umbral_rsi:
            if puntaje and umbral and puntaje > factor_umbral_puntaje * umbral:
                return False  # ✅ RSI sugiere seguir en el trade

        # Validación MACD: cruce alcista reciente
        macd_line, signal = calcular_macd(df)
        if macd_line is not None and signal is not None:
            if macd_line[-2] < signal[-2] and macd_line[-1] > signal[-1]:
                return False  # ✅ Confirmación alcista

    except Exception as e:
        print(f"⚠️ Error evaluando filtros de salida: {e}")

    return True 

