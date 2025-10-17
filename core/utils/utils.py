"""Funciones utilitarias compartidas por el bot."""
from __future__ import annotations

import json
import math
import os
import tempfile
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from .logger import configurar_logger

__all__ = [
    "configurar_logger",
    "leer_csv_seguro",
    "validar_dataframe",
    "intervalo_a_segundos",
    "timestamp_alineado",
    "is_valid_number",
    "validar_integridad_velas",
    "guardar_orden_real",
    "round_decimal",
    "ESTADO_DIR",
]

ESTADO_DIR = Path(os.getenv("ESTADO_DIR", "estado")).expanduser()
ESTADO_DIR.mkdir(parents=True, exist_ok=True)

_IO_FALLBACK_DIR = Path(tempfile.gettempdir()) / "bot_io_fallbacks"

log = configurar_logger("utils_io")


def leer_csv_seguro(path: str | os.PathLike[str], *, expected_cols: int | None = None, **kwargs: Any) -> pd.DataFrame:
    """Lee un CSV devolviendo ``DataFrame`` vacío ante errores."""
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
    """Valida que ``df`` sea un ``DataFrame`` con las columnas solicitadas."""
    if df is None or not isinstance(df, pd.DataFrame):
        return False
    if df.empty:
        return False
    return set(columnas).issubset(df.columns)


def intervalo_a_segundos(intervalo: str) -> int:
    """Convierte un intervalo estilo Binance (``"1m"``) a segundos."""
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
    """Verifica que ``timestamp`` esté alineado al intervalo especificado."""
    base_ms = intervalo_a_segundos(intervalo) * 1000
    if base_ms <= 0:
        return False
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return False
    return ts % base_ms == 0


def is_valid_number(valor: Any, *, allow_none: bool = False) -> bool:
    """Comprueba que ``valor`` sea un número finito."""
    if valor is None:
        return allow_none
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return False
    return not math.isnan(numero) and math.isfinite(numero)

def validar_integridad_velas(
    symbol_or_velas: str | Iterable[Mapping[str, Any]] | Iterable[Sequence[Any]],
    intervalo: str,
    velas: Iterable[Mapping[str, Any]] | Iterable[Sequence[Any]] | None = None,
    logger: Any | None = None,
) -> bool:
    """Chequea que las velas estén ordenadas y sin huecos para un ``intervalo``.

    Permite tanto invocaciones `validar_integridad_velas(velas, intervalo)` como
    `validar_integridad_velas(symbol, intervalo, velas, logger)` para mantener
    compatibilidad con código legado.
    """

    if velas is None:
        if isinstance(symbol_or_velas, str):
            raise TypeError("Se esperaba iterable de velas como primer argumento")
        items = list(symbol_or_velas)  # type: ignore[arg-type]
        symbol = None
    else:
        symbol = str(symbol_or_velas) if symbol_or_velas is not None else None
        items = list(velas)

    if not items:
        return True

    paso = intervalo_a_segundos(intervalo) * 1000
    if paso <= 0:
        return False

    anterior: int | None = None
    for vela in items:
        timestamp: int | None = None
        if isinstance(vela, Mapping):
            for key in ("timestamp", "open_time", "close_time", "time"):
                raw = vela.get(key)
                if raw is not None:
                    try:
                        timestamp = int(float(raw))
                        break
                    except (TypeError, ValueError):
                        if logger:
                            logger.warning(f"{symbol or 'candles'}: timestamp inválido en vela")
                        return False
        else:
            if not vela:
                return False
            try:
                timestamp = int(float(vela[0]))
            except (TypeError, ValueError):
                if logger:
                    logger.warning(f"{symbol or 'candles'}: timestamp inválido en vela")
                return False

        if timestamp is None:
            return False

        if anterior is not None and timestamp - anterior != paso:
            if logger:
                logger.warning(
                    f"{symbol or 'candles'}: gap detectado ({timestamp - anterior} ms != {paso} ms)"
                )
            return False
        anterior = timestamp
    return True


def guardar_orden_real(symbol: str, data: Mapping[str, Any], *, carpeta: Path | None = None) -> Path:
    """Persiste datos de órdenes reales en un archivo JSONL."""
    carpeta = carpeta or (ESTADO_DIR / "ordenes_reales")
    carpeta.mkdir(parents=True, exist_ok=True)
    fecha = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = carpeta / f"{symbol.upper()}_{fecha}.jsonl"
    try:
        with path.open("a", encoding="utf-8") as fh:
            json.dump(dict(data), fh, ensure_ascii=False)
            fh.write("\n")
        return path
    except PermissionError as exc:
        log.error(
            "❌ Permiso denegado al escribir orden real",
            extra={"path": str(path), "error": str(exc)},
        )
        fallback = _IO_FALLBACK_DIR / path.name
        fallback.parent.mkdir(parents=True, exist_ok=True)
        with fallback.open("a", encoding="utf-8") as fh:
            json.dump(dict(data), fh, ensure_ascii=False)
            fh.write("\n")
        log.warning(
            "⚠️ Orden persistida en directorio temporal",
            extra={"path": str(fallback)},
        )
        return fallback
    except OSError as exc:
        log.error(
            "❌ Error de E/S al escribir orden real",
            extra={"path": str(path), "error": str(exc)},
        )
        fallback = _IO_FALLBACK_DIR / path.name
        fallback.parent.mkdir(parents=True, exist_ok=True)
        try:
            with fallback.open("a", encoding="utf-8") as fh:
                json.dump(dict(data), fh, ensure_ascii=False)
                fh.write("\n")
        except (OSError, PermissionError) as inner_exc:
            log.error(
                "❌ No fue posible persistir la orden",
                extra={"path": str(fallback), "error": str(inner_exc)},
            )
            raise
        log.warning(
            "⚠️ Orden persistida en fallback por error de E/S",
            extra={"path": str(fallback), "error_original": str(exc)},
        )
        return fallback


def round_decimal(valor: float | Decimal, digits: int = 8) -> float:
    """Redondea ``valor`` al número de decimales indicado."""
    quant = Decimal(10) ** -digits
    dec = Decimal(str(valor)).quantize(quant, rounding=ROUND_HALF_UP)
    return float(dec)