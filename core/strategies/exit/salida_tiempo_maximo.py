import pandas as pd
from datetime import datetime, timedelta, timezone
from core.utils import configurar_logger
from core.strategies.exit.salida_utils import resultado_salida

log = configurar_logger("salida_tiempo_maximo")

def salida_tiempo_maximo(
    orden: dict, df: pd.DataFrame, config: dict | None = None
) -> dict:
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

        horas = config.get("duracion_max_horas", 4) if config else 4
        tiempo_maximo = timedelta(hours=horas)  # límite de vida de una orden
        ahora = datetime.now(timezone.utc)
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