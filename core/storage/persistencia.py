from __future__ import annotations
import json
import os
from typing import Dict, Tuple

from core.utils.utils import configurar_logger

log = configurar_logger("persistencia_estado")

def guardar_estado(
    historial_cierres: Dict[str, dict],
    capital_por_simbolo: Dict[str, float],
    carpeta: str = "estado",
) -> None:
    """Guarda ``historial_cierres`` y ``capital_por_simbolo`` en ``carpeta``."""
    try:
        os.makedirs(carpeta, exist_ok=True)
        with open(os.path.join(carpeta, "historial_cierres.json"), "w") as f:
            json.dump(historial_cierres, f, indent=2)
        with open(os.path.join(carpeta, "capital.json"), "w") as f:
            json.dump(capital_por_simbolo, f, indent=2)
    except Exception as e:  # noqa: BLE001
        log.warning(f"⚠️ Error guardando estado persistente: {e}")


def cargar_estado(carpeta: str = "estado") -> Tuple[Dict[str, dict], Dict[str, float]]:
    """Carga historial de cierres y capital desde ``carpeta``."""
    historial: Dict[str, dict] = {}
    capital: Dict[str, float] = {}
    try:
        path_historial = os.path.join(carpeta, "historial_cierres.json")
        path_capital = os.path.join(carpeta, "capital.json")
        if os.path.exists(path_historial):
            with open(path_historial) as f:
                contenido = f.read()
            if contenido.strip():
                try:
                    data = json.loads(contenido)
                except json.JSONDecodeError as e:  # noqa: BLE001
                    log.warning(f"⚠️ Error leyendo historial_cierres.json: {e}")
                    data = {}
                if isinstance(data, dict):
                    historial.update(data)
        if os.path.exists(path_capital):
            with open(path_capital) as f:
                contenido = f.read()
            if contenido.strip():
                try:
                    data = json.loads(contenido)
                except json.JSONDecodeError as e:  # noqa: BLE001
                    log.warning(f"⚠️ Error leyendo capital.json: {e}")
                    data = {}
                if isinstance(data, dict):
                    capital.update({k: float(v) for k, v in data.items()})
    except Exception as e:  # noqa: BLE001
        log.warning(f"⚠️ Error cargando estado persistente: {e}")
    return historial, capital