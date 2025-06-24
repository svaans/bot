import pandas as pd
from indicators.atr import calcular_atr
from core.utils import configurar_logger

log = configurar_logger("salida_break_even")


def salida_break_even(orden: dict, df: pd.DataFrame, config: dict | None = None) -> dict:
    """Evalúa si se debe mover el Stop Loss a precio de entrada."""
    try:
        if df is None or len(df) < 15 or not {"close", "high", "low"}.issubset(df.columns):
            return {"cerrar": False}

        entrada = orden.get("precio_entrada")
        direccion = orden.get("direccion", "long")
        if entrada is None:
            return {"cerrar": False}

        precio_actual = df["close"].iloc[-1]
        atr_periodo = config.get("periodo_atr", 14) if config else 14
        atr = calcular_atr(df, atr_periodo)
        if atr is None:
            return {"cerrar": False}

        multiplicador = config.get("break_even_atr_mult", 1.5) if config else 1.5
        umbral = atr * multiplicador

        if direccion in ("long", "compra"):
            if precio_actual >= entrada + umbral:
                log.info(
                    f"🟡 Break-Even activado para {orden.get('symbol', 'SYM')} → SL movido a entrada: {entrada}"
                )
                return {"cerrar": False, "break_even": True, "nuevo_sl": entrada}
        else:
            if precio_actual <= entrada - umbral:
                log.info(
                    f"🟡 Break-Even activado para {orden.get('symbol', 'SYM')} → SL movido a entrada: {entrada}"
                )
                return {"cerrar": False, "break_even": True, "nuevo_sl": entrada}

        return {"cerrar": False}

    except Exception as e:
        log.warning(f"Error en salida_break_even: {e}")
        return {"cerrar": False}