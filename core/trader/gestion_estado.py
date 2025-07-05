"""Módulo para gestionar persistencia de estado del trader"""

from __future__ import annotations
import os
import json
from core.utils.utils import configurar_logger

# Importa la clase Trader para las anotaciones de tipo
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.trader.trader import Trader

log = configurar_logger("trader")

def guardar_estado_persistente(trader: Trader) -> None:
    """
    Guarda el historial de cierres y capital en la carpeta `estado/`
    """
    try:
        os.makedirs('estado', exist_ok=True)
        _save_json_file('estado/historial_cierres.json', trader.historial_cierres)
        _save_json_file('estado/capital.json', trader.capital_por_simbolo)
    except Exception as e:
        log.warning(f"⚠️ Error guardando estado persistente: {e}")


def cargar_estado_persistente(trader: Trader) -> None:
    """
    Carga el estado previo desde la carpeta `estado/`
    """
    try:
        data = _load_json_file('estado/historial_cierres.json')
        if data:
            trader.historial_cierres.update(data)
        data = _load_json_file('estado/capital.json')
        if data:
            trader.capital_por_simbolo.update({k: float(v) for k, v in data.items()})
    except Exception as e:
        log.warning(f"⚠️ Error cargando estado persistente: {e}")


def _load_json_file(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            contenido = f.read()
    except OSError as e:
        log.warning(f"⚠️ Error abriendo {path}: {e}")
        return {}
    if not contenido.strip():
        return {}
    try:
        data = json.loads(contenido)
    except json.JSONDecodeError as e:
        log.warning(f"⚠️ Error decodificando {path}: {e}")
        return {}
    return data if isinstance(data, dict) else {}


def _save_json_file(path: str, data: dict) -> None:
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except OSError as e:
        log.warning(f"⚠️ Error guardando {path}: {e}")
