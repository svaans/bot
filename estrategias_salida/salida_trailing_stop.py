import pandas as pd

def salida_trailing_stop(orden: dict, df: pd.DataFrame, config: dict = None) -> dict:
    """
    Evalúa si debe cerrarse una orden utilizando lógica de trailing stop.

    Parámetros:
        - orden: dict con detalles de la orden
        - df: DataFrame con datos recientes del mercado
        - config: dict opcional con parámetros personalizados:
            * trailing_pct (ej. 0.015 para 1.5%)
            * modo (para variantes futuras del algoritmo)

    Devuelve:
        - dict con claves:
            * cerrar (bool): True si debe cerrarse
            * razon (str): explicación
    """
    try:
        if df is None or len(df) < 3 or not {"close"}.issubset(df.columns):
            return {"cerrar": False, "razon": "Datos insuficientes o mal formateados"}

        precio_actual = df["close"].iloc[-1]
        direccion = orden.get("direccion", "long")

        # --- Trailing personalizado o por defecto ---
        trailing_pct = config.get("trailing_pct", 0.015) if config else 0.015

        # --- Inicializa max_precio si no existe ---
        if "max_precio" not in orden:
            orden["max_precio"] = orden["precio_entrada"]

        # --- Lógica de trailing por dirección ---
        if direccion in ["compra", "long"]:
            if precio_actual > orden["max_precio"]:
                orden["max_precio"] = precio_actual
            elif precio_actual < orden["max_precio"] * (1 - trailing_pct):
                return {
                    "cerrar": True,
                    "razon": f"Trailing Stop activado (long) → Max: {orden['max_precio']:.2f}, Precio actual: {precio_actual:.2f}"
                }

        elif direccion in ["venta", "short"]:
            if precio_actual < orden["max_precio"]:
                orden["max_precio"] = precio_actual
            elif precio_actual > orden["max_precio"] * (1 + trailing_pct):
                return {
                    "cerrar": True,
                    "razon": f"Trailing Stop activado (short) → Min: {orden['max_precio']:.2f}, Precio actual: {precio_actual:.2f}"
                }

        return {"cerrar": False, "razon": "Trailing no activado"}

    except Exception as e:
        return {"cerrar": False, "razon": f"Error en trailing stop: {e}"}



def verificar_trailing_stop(info: dict, precio_actual: float, config: dict = None) -> tuple[bool, str]:
    """
    Evalúa si debe cerrarse la orden usando lógica de trailing stop.

    Parámetros:
        - info: dict con información de la orden (precio_entrada, max_price, etc.)
        - precio_actual: último precio de mercado
        - config: configuración personalizada con claves:
            * trailing_start_ratio (ej: 1.015 para +1.5%)
            * trailing_distance_ratio (ej: 0.02 para -2%)

    Devuelve:
        - (True, "razón") si debe cerrarse
        - (False, "") si no
    """
    entrada = info["precio_entrada"]
    max_price = info.get("max_price", entrada)
    if precio_actual > max_price:
        info["max_price"] = precio_actual  # Actualiza el máximo

    # ---- Configuración personalizada o por defecto ----
    trailing_start_ratio = config.get("trailing_start_ratio", 1.015) if config else 1.015
    trailing_distance_ratio = config.get("trailing_distance_ratio", 0.02) if config else 0.02

    trailing_trigger = entrada * trailing_start_ratio  # Solo activa trailing si sube al menos un X%
    if max_price >= trailing_trigger:
        trailing_stop = max_price * (1 - trailing_distance_ratio)  # Nivel de caída para cerrar
        if precio_actual <= trailing_stop:
            return True, f"Trailing Stop activado — Máximo: {max_price:.2f}, Límite: {trailing_stop:.2f}, Precio actual: {precio_actual:.2f}"
        else:
            # Trailing activo pero aún no toca el nivel de salida
            return False, f"Trailing supervisando — Máx {max_price:.2f}, Límite {trailing_stop:.2f}"

    return False, ""
