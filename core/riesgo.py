import os
import json
from datetime import datetime
from core.logger import configurar_logger

log = configurar_logger("riesgo")

RUTA_ESTADO = "config/estado_riesgo.json"


def cargar_estado_riesgo():
    """Carga el estado de riesgo desde archivo, o inicializa si no existe o es inválido."""
    if not os.path.exists(RUTA_ESTADO):
        return {"fecha": "", "perdida_acumulada": 0.0}

    try:
        with open(RUTA_ESTADO, "r") as f:
            estado = json.load(f)
        if not isinstance(estado, dict):
            raise ValueError("❌ Formato inválido en estado de riesgo.")
        return estado
    except (OSError, json.JSONDecodeError) as e:
        log.warning(f"⚠️ Error al cargar estado de riesgo: {e}")
        return {"fecha": "", "perdida_acumulada": 0.0}


def guardar_estado_riesgo(estado: dict):
    """Guarda el estado actual de riesgo en disco."""
    try:
        with open(RUTA_ESTADO, "w") as f:
            json.dump(estado, f, indent=4)
        log.info(f"💾 Estado de riesgo actualizado: {estado}")
    except OSError as e:
        log.error(f"❌ No se pudo guardar estado de riesgo: {e}")
        raise


def actualizar_perdida(simbolo: str, perdida: float):
    """Registra una pérdida para el día actual (valor absoluto)."""
    estado = cargar_estado_riesgo()
    hoy = datetime.utcnow().date().isoformat()

    if estado.get("fecha") != hoy:
        estado = {"fecha": hoy, "perdida_acumulada": 0.0}

    estado["perdida_acumulada"] += abs(perdida)
    guardar_estado_riesgo(estado)

    log.info(f"📉 {simbolo}: pérdida registrada {perdida:.2f} | Total hoy: {estado['perdida_acumulada']:.2f}")


def riesgo_superado(umbral: float, capital_total: float) -> bool:
    """Evalúa si el umbral de pérdida diaria ha sido superado."""
    estado = cargar_estado_riesgo()
    hoy = datetime.utcnow().date().isoformat()

    if estado.get("fecha") != hoy:
        return False

    if capital_total <= 0:
        log.warning("⚠️ Capital total es 0. No se puede evaluar riesgo.")
        return False

    porcentaje_perdido = estado["perdida_acumulada"] / capital_total
    log.debug(f"Evaluación de riesgo: {porcentaje_perdido:.2%} perdido (umbral: {umbral:.2%})")
    return porcentaje_perdido >= umbral

