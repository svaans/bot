"""Funciones utilitarias compartidas en el bot."""
from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from .logger import configurar_logger

__all__ = [
    "configurar_logger",
    "leer_csv_seguro",
    "validar_dataframe",
    "intervalo_a_segundos",
    "timestamp_alineado",
    "is_valid_number",
    "guardar_orden_real",
    "round_decimal",
    "ESTADO_DIR",
]


ESTADO_DIR = Path(os.getenv("ESTADO_DIR", "estado")).expanduser()
ESTADO_DIR.mkdir(parents=True, exist_ok=True)


def leer_csv_seguro(path: str | os.PathLike[str], *, expected_cols: int | None = None, **kwargs: Any) -> pd.DataFrame:
    """Lee un CSV retornando DataFrame vacío ante fallos."""
    try:
        df = pd.read_csv(path, **kwargs)
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()
    if expected_cols is not None and df.shape[1] != expected_cols:
        return pd.DataFrame()
    return df


def validar_dataframe(df: Any, columnas: Iterable[str]) -> bool:
    if df is None or not isinstance(df, pd.DataFrame):
        return False
    if df.empty:
        return False
    return set(columnas).issubset(df.columns)


def intervalo_a_segundos(intervalo: str) -> int:
    mapping = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "8h": 28800,
        "12h": 43200,
        "1d": 86400,
        "3d": 259200,
        "1w": 604800,
        "1M": 2592000,
    }
    return mapping.get(intervalo, 60)


def timestamp_alineado(timestamp: int | float, intervalo: str) -> bool:
    base_ms = intervalo_a_segundos(intervalo) * 1000
    if base_ms <= 0:
        return False
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return False
    return ts % base_ms == 0


def is_valid_number(valor: Any, *, allow_none: bool = False) -> bool:
    if valor is None:
        return allow_none
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return False
    return not math.isnan(numero) and math.isfinite(numero)


def guardar_orden_real(symbol: str, data: Mapping[str, Any], *, carpeta: Path | None = None) -> Path:
    carpeta = carpeta or (ESTADO_DIR / "ordenes_reales")
    carpeta.mkdir(parents=True, exist_ok=True)
    fecha = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = carpeta / f"{symbol.upper()}_{fecha}.jsonl"
    with path.open("a", encoding="utf-8") as fh:
        json.dump(dict(data), fh, ensure_ascii=False)
        fh.write("\n")
    return path


def round_decimal(valor: float | Decimal, digits: int = 8) -> float:
    quant = Decimal(10) ** -digits
    dec = Decimal(str(valor)).quantize(quant, rounding=ROUND_HALF_UP)
    return float(dec)