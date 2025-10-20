"""Gestión centralizada del historial de operaciones para entrenamiento.

Este módulo unifica la lectura del historial de operaciones tanto en
simulación como en producción. Los procesos de ingesta guardan órdenes en
``ordenes_reales`` o ``ordenes_simuladas`` mientras que componentes más
antiguos escriben en ``learning/ultimas_operaciones``. El cargador intenta
primero leer el origen oficial y, si no existe o está dañado, recurre al
espejo heredado para evitar divergencias entre datasets.

La interfaz expone la dataclase :class:`HistorialOperaciones` junto con la
función :func:`cargar_historial_operaciones` que retorna el ``DataFrame`` a
utilizar y la ruta que se empleó como fuente. Esto permite instrumentar logs y
tests sin duplicar lógica de descubrimiento de archivos.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import os

import pandas as pd
from dotenv import dotenv_values

from core.utils.utils import configurar_logger

log = configurar_logger("historial_operaciones")

CONFIG = dotenv_values("config/claves.env")
MODO_REAL = CONFIG.get("MODO_REAL", "False") == "True"

_LEARNING_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _LEARNING_DIR.parent


def _resolver_ruta_env(variable: str, predeterminada: Path) -> Path:
    """Devuelve una ruta basada en una variable de entorno opcional."""

    valor = os.getenv(variable)
    if valor:
        return Path(valor).expanduser().resolve()
    return predeterminada


CARPETA_ORDENES = _resolver_ruta_env(
    "CARPETA_ORDENES_PATH",
    (_REPO_ROOT / ("ordenes_reales" if MODO_REAL else "ordenes_simuladas")).resolve(),
)
CARPETA_ULTIMAS = _resolver_ruta_env(
    "CARPETA_ULTIMAS_PATH",
    (_LEARNING_DIR / "ultimas_operaciones").resolve(),
)


@dataclass(slots=True)
class HistorialOperaciones:
    """Pequeño contenedor con los datos y el origen del historial."""

    data: pd.DataFrame
    source: Path


def _normalizar_nombre_archivo(symbol: str) -> str:
    """Convierte ``symbol`` en el nombre de archivo estándar."""

    return symbol.replace("/", "_").upper() + ".parquet"


def _build_fuentes(symbol: str) -> Sequence[Path]:
    archivo = _normalizar_nombre_archivo(symbol)
    return (
        CARPETA_ORDENES / archivo,
        CARPETA_ULTIMAS / archivo,
    )


def _ordenar_dataframe(df: pd.DataFrame, columnas: Iterable[str]) -> pd.DataFrame:
    for columna in columnas:
        if columna in df.columns:
            return df.sort_values(columna).reset_index(drop=True)
    return df.reset_index(drop=True)


def cargar_historial_operaciones(
    symbol: str,
    *,
    max_operaciones: int | None = None,
    columnas_orden: Iterable[str] = ("timestamp", "fecha_cierre"),
) -> HistorialOperaciones:
    """Carga el historial de operaciones para ``symbol``.

    El orden de preferencia es:

    1. ``ordenes_reales`` o ``ordenes_simuladas`` según ``MODO_REAL``.
    2. ``learning/ultimas_operaciones`` como respaldo legado.

    Parameters
    ----------
    symbol:
        Símbolo de mercado (``BTC/USDT`` por ejemplo).
    max_operaciones:
        Si se indica, recorta el ``DataFrame`` a las últimas ``n`` filas tras
        ordenar por ``columnas_orden``.
    columnas_orden:
        Secuencia de columnas candidatas a utilizar para ordenar el historial.

    Raises
    ------
    FileNotFoundError
        Cuando no se encuentra ninguna fuente disponible.
    RuntimeError
        Si existen archivos pero todos presentan errores al leerse.
    """

    errores: list[str] = []
    for ruta in _build_fuentes(symbol):
        if not ruta.exists():
            continue
        try:
            df = pd.read_parquet(ruta)
        except Exception as exc:  # pragma: no cover - logging defensivo
            log.warning(
                "⚠️ Historial de operaciones dañado", extra={"ruta": str(ruta), "error": str(exc)}
            )
            errores.append(f"{ruta}: {exc}")
            continue
        df = _ordenar_dataframe(df, columnas_orden)
        if max_operaciones is not None and max_operaciones > 0:
            df = df.tail(max_operaciones).reset_index(drop=True)
        return HistorialOperaciones(data=df, source=ruta)

    if errores:
        raise RuntimeError(
            "No fue posible cargar el historial de operaciones: " + "; ".join(errores)
        )
    raise FileNotFoundError(
        f"No se encontró historial de operaciones para {symbol} en las rutas configuradas."
    )


__all__ = [
    "HistorialOperaciones",
    "CARPETA_ORDENES",
    "CARPETA_ULTIMAS",
    "cargar_historial_operaciones",
]
