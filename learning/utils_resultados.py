"""Utilidades para distribuir retornos de órdenes entre estrategias activas."""
from __future__ import annotations

from ast import literal_eval
import math
import json
from typing import Any, Dict, Mapping

def parsear_estrategias_activas(raw: Any) -> Dict[str, Any]:
    """Normaliza la representación de ``estrategias_activas`` a un ``dict``.

    Se aceptan los siguientes formatos:
      - ``dict`` ya parseado.
      - ``str`` con JSON o literales de Python.

    Cualquier error produce un diccionario vacío.
    """

    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        texto = raw.strip()
        if not texto:
            return {}
        for loader in (_cargar_json, _cargar_json_comillas_simples, _cargar_literal):
            data = loader(texto)
            if isinstance(data, dict):
                return data
    return {}


def _cargar_json(texto: str) -> Any:
    try:
        return json.loads(texto)
    except (json.JSONDecodeError, TypeError):
        return None


def _cargar_json_comillas_simples(texto: str) -> Any:
    try:
        return json.loads(texto.replace("'", '"'))
    except (json.JSONDecodeError, TypeError):
        return None


def _cargar_literal(texto: str) -> Any:
    try:
        return literal_eval(texto)
    except (ValueError, SyntaxError):
        return None


def extraer_pesos_estrategias(estrategias: Dict[str, Any]) -> Dict[str, float]:
    """Deriva pesos positivos para cada estrategia activa.

    Heurísticas aplicadas:
      - Valores booleanos ``True`` se consideran 1.0, ``False`` se ignoran.
      - Valores numéricos positivos se utilizan directamente.
      - Cadenas con números o valores tipo ``"true"``/``"false"`` se interpretan.
      - Diccionarios anidados buscan claves habituales (``peso``, ``weight``, ``score``).

    Si no se obtiene ningún peso positivo se devuelve un diccionario vacío.
    """

    pesos: Dict[str, float] = {}
    for nombre, valor in estrategias.items():
        peso = _normalizar_valor_peso(valor)
        if peso is not None and peso > 0:
            pesos[str(nombre)] = float(peso)
    return pesos


def _normalizar_valor_peso(valor: Any) -> float | None:
    if isinstance(valor, bool):
        return 1.0 if valor else None
    if isinstance(valor, (int, float)):
        return float(valor)
    if isinstance(valor, str):
        texto = valor.strip()
        if not texto:
            return None
        lower = texto.lower()
        if lower in {"true", "si", "sí", "on", "activo"}:
            return 1.0
        if lower in {"false", "off", "no", "0", "inactivo"}:
            return None
        try:
            return float(texto)
        except ValueError:
            return None
    if isinstance(valor, dict):
        for key in ("peso", "weight", "score", "valor", "value"):
            if key in valor:
                try:
                    return float(valor[key])
                except (TypeError, ValueError):
                    continue
        activo = valor.get("activo") or valor.get("active")
        if isinstance(activo, bool):
            return 1.0 if activo else None
        if isinstance(activo, (int, float)):
            return float(activo)
    return 1.0 if valor else None


def distribuir_retorno_por_estrategia(
    retorno_total: float,
    estrategias_activas: Any,
) -> Dict[str, float]:
    """Distribuye ``retorno_total`` entre las estrategias activas.

    Si existen pesos positivos, el retorno se reparte proporcionalmente.
    En caso contrario, se reparte en partes iguales entre las estrategias con
    valores *truthy*.
    """

    estrategias = parsear_estrategias_activas(estrategias_activas)
    if not estrategias:
        return {}

    pesos = extraer_pesos_estrategias(estrategias)
    if pesos:
        total_pesos = sum(pesos.values())
        if total_pesos <= 0:
            return {}
        return {
            nombre: retorno_total * peso / total_pesos
            for nombre, peso in pesos.items()
        }

    activas = [nombre for nombre, valor in estrategias.items() if valor]
    if not activas:
        return {}
    retorno_unitario = retorno_total / len(activas)
    return {nombre: retorno_unitario for nombre in activas}


__all__ = [
    "parsear_estrategias_activas",
    "extraer_pesos_estrategias",
    "distribuir_retorno_por_estrategia",
    "obtener_retorno_total_registro",
]


def obtener_retorno_total_registro(registro: Mapping[str, Any]) -> float:
    """Devuelve el retorno total usando ``retorno_total`` o ``retorno`` como respaldo."""

    for clave in ("retorno_total", "retorno"):
        if clave in registro:
            try:
                valor = float(registro[clave])
            except (TypeError, ValueError):
                continue
            if math.isnan(valor):
                continue
            return valor
    return 0.0