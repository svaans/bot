"""Persistencia de operaciones cerradas para el ciclo de aprendizaje.

Puente entre el cierre de órdenes del runtime y ``learning/aprendizaje_continuo``:
el ciclo lee ``<repo>/ordenes_simuladas|ordenes_reales/<SYMBOL>.parquet`` (según
``MODO_REAL``), pero el runtime solo persistía cierres en SQLite y JSONL, por lo
que el aprendizaje no tenía datos de entrada. Este módulo escribe el registro
de cada operación cerrada en el parquet por símbolo que esperan
``analisis_resultados`` y ``aprendizaje_continuo``.

Notas de formato:
- ``timestamp`` se escribe en **segundos epoch** (float): es lo que asumen
  ``analizar_estrategias_en_ordenes`` (``pd.to_datetime(..., unit='s')``) y el
  filtro semanal de ``procesar_simbolo``.
- ``estrategias_activas`` se mantiene como JSON string (igual que
  ``Order.to_parquet_record``); valores dict/list restantes se serializan a
  JSON para que el registro sea siempre escribible en parquet.

Thread-safe (lock de módulo) y con escritura atómica (tmp + ``os.replace``).
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from core.utils.log_utils import format_exception_for_log
from core.utils.utils import configurar_logger

from . import historial_operaciones as _hist

log = configurar_logger("registro_aprendizaje", modo_silencioso=True)

_lock = threading.Lock()

UTC = timezone.utc


def _a_epoch_segundos(valor: Any) -> float:
    """Convierte ISO-8601/epoch a segundos epoch; ahora UTC si no es parseable."""

    if isinstance(valor, (int, float)) and not isinstance(valor, bool):
        return float(valor)
    if isinstance(valor, str) and valor:
        try:
            return datetime.fromisoformat(valor).timestamp()
        except ValueError:
            pass
    return datetime.now(UTC).timestamp()


def _sanear_valor(valor: Any) -> Any:
    """Serializa dict/list a JSON para mantener el registro parquet-compatible."""

    if isinstance(valor, (dict, list)):
        try:
            return json.dumps(valor, ensure_ascii=False)
        except (TypeError, ValueError):
            return str(valor)
    return valor


def registrar_cierre_para_aprendizaje(record: Mapping[str, Any]) -> Path | None:
    """Añade ``record`` (de ``Order.to_parquet_record``) al parquet de su símbolo.

    Devuelve la ruta escrita, o ``None`` si el registro no tiene símbolo o la
    escritura falla (se loguea; el cierre de la orden nunca debe fallar por esto).
    """

    symbol = str(record.get("symbol") or "").strip()
    if not symbol:
        log.warning("⚠️ Registro de cierre sin símbolo; no se persiste para aprendizaje.")
        return None

    fila = {k: _sanear_valor(v) for k, v in dict(record).items()}
    fila["timestamp"] = _a_epoch_segundos(
        record.get("fecha_cierre") or record.get("timestamp")
    )

    # Resuelto en tiempo de llamada para respetar monkeypatch en tests.
    carpeta = _hist.CARPETA_ORDENES
    archivo = _hist.normalizar_symbol_parquet_filename(symbol)
    destino = Path(carpeta) / archivo

    try:
        with _lock:
            destino.parent.mkdir(parents=True, exist_ok=True)
            nuevo = pd.DataFrame([fila])
            if destino.exists():
                try:
                    previo = pd.read_parquet(destino)
                    nuevo = pd.concat([previo, nuevo], ignore_index=True)
                except Exception as exc:
                    log.warning(
                        "⚠️ Historial parquet ilegible; se reescribe desde cero: %s",
                        format_exception_for_log(exc),
                    )
            fd, tmp_str = tempfile.mkstemp(dir=destino.parent, suffix=".tmp")
            os.close(fd)
            tmp = Path(tmp_str)
            try:
                nuevo.to_parquet(tmp, index=False)
                os.replace(tmp, destino)
            finally:
                tmp.unlink(missing_ok=True)
        return destino
    except Exception as exc:
        log.error(
            "❌ No se pudo persistir cierre para aprendizaje (%s): %s",
            symbol,
            format_exception_for_log(exc),
        )
        return None


__all__ = ["registrar_cierre_para_aprendizaje"]
