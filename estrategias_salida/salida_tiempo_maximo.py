import pandas as pd
from datetime import datetime, timedelta
from core.logger import configurar_logger
from core.salida_utils import resultado_salida

log = configurar_logger("salida_tiempo_maximo")

def salida_tiempo_maximo(orden: dict, df: pd.DataFrame) -> dict:
    try:
        timestamp = orden.get("timestamp")
        if not timestamp:
            return resultado_salida(
                "Tecnico",
                False,
                "Sin timestamp de apertura",
            )

        # Convierte el timestamp ISO a objeto datetime
        if isinstance(timestamp, str):
            timestamp_dt = datetime.fromisoformat(timestamp)
        elif isinstance(timestamp, (int, float)):
            timestamp_dt = datetime.utcfromtimestamp(timestamp / 1000)
        else:
            return resultado_salida(
                "Tecnico",
                False,
                "Formato de timestamp no válido",
            )

        tiempo_maximo = timedelta(hours=4)  # límite de vida de una orden
        ahora = datetime.utcnow()
        tiempo_abierta = ahora - timestamp_dt

        if tiempo_abierta > tiempo_maximo:
            return resultado_salida(
                "Tecnico",
                True,
                f"Orden superó las {tiempo_maximo.total_seconds() // 3600:.0f}h",
                logger=log,
            )

        return resultado_salida(
            "Tecnico",
            False,
            f"Tiempo actual {tiempo_abierta}",
        )
    except Exception as e:
        return resultado_salida("Tecnico", False, f"Error en salida por tiempo: {e}")