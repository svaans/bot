import pandas as pd
from indicadores.atr import calcular_atr

def salida_takeprofit_atr(orden: dict, df: pd.DataFrame) -> dict:
    try:
        if len(df) < 20 or "close" not in df.columns:
            return {"cerrar": False, "razon": "Datos insuficientes"}

        direccion = orden.get("direccion", "long")
        precio_actual = df["close"].iloc[-1]
        entrada = orden.get("precio_entrada")
        atr = calcular_atr(df)

        if atr is None or entrada is None:
            return {"cerrar": False, "razon": "ATR o entrada no disponibles"}

        margen_tp = 2.5 * atr  # Take Profit a 2.5 ATR

        if direccion in ["long", "compra"]:
            tp_tecnico = entrada + margen_tp
            if precio_actual >= tp_tecnico:
                return {
                    "cerrar": True,
                    "razon": f"TP-ATR alcanzado (long) a {tp_tecnico:.4f}"
                }
        elif direccion == "venta":
            tp_tecnico = entrada - margen_tp
            if precio_actual <= tp_tecnico:
                return {
                    "cerrar": True,
                    "razon": f"TP-ATR alcanzado (short) a {tp_tecnico:.4f}"
                }

        return {"cerrar": False, "razon": "TP-ATR no alcanzado"}

    except Exception as e:
        return {"cerrar": False, "razon": f"Error TP-ATR: {e}"}
