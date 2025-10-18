from __future__ import annotations

import csv
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import Dict, List
from uuid import uuid4

UTC = timezone.utc
_lock = Lock()


class AuditEvent(StrEnum):
    """Eventos auditables en el ciclo de vida de una operación."""

    ENTRY = "ENTRY"
    EXIT = "EXIT"
    PARTIAL_EXIT = "PARTIAL_EXIT"
    CANCEL = "CANCEL"
    REJECTION = "REJECTION"
    ADJUSTMENT = "ADJUSTMENT"
    UNKNOWN = "UNKNOWN"


class AuditResult(StrEnum):
    """Resultados normalizados para operaciones auditadas."""

    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    PARTIAL = "PARTIAL"
    UNKNOWN = "UNKNOWN"


_EVENT_ALIASES: Dict[str, AuditEvent] = {
    AuditEvent.ENTRY.value.lower(): AuditEvent.ENTRY,
    "apertura": AuditEvent.ENTRY,
    "entrada": AuditEvent.ENTRY,
    "open": AuditEvent.ENTRY,
    "entry": AuditEvent.ENTRY,
    AuditEvent.EXIT.value.lower(): AuditEvent.EXIT,
    "cierre": AuditEvent.EXIT,
    "exit": AuditEvent.EXIT,
    AuditEvent.PARTIAL_EXIT.value.lower(): AuditEvent.PARTIAL_EXIT,
    "cierre_parcial": AuditEvent.PARTIAL_EXIT,
    "partial_exit": AuditEvent.PARTIAL_EXIT,
    "partial-close": AuditEvent.PARTIAL_EXIT,
    AuditEvent.CANCEL.value.lower(): AuditEvent.CANCEL,
    "cancel": AuditEvent.CANCEL,
    "cancelacion": AuditEvent.CANCEL,
    "cancelación": AuditEvent.CANCEL,
    AuditEvent.REJECTION.value.lower(): AuditEvent.REJECTION,
    "entrada_rechazada": AuditEvent.REJECTION,
    "entrada rechazada": AuditEvent.REJECTION,
    "rechazo": AuditEvent.REJECTION,
    AuditEvent.ADJUSTMENT.value.lower(): AuditEvent.ADJUSTMENT,
}


_RESULT_ALIASES: Dict[str, AuditResult] = {
    AuditResult.SUCCESS.value.lower(): AuditResult.SUCCESS,
    "exitoso": AuditResult.SUCCESS,
    "success": AuditResult.SUCCESS,
    "ok": AuditResult.SUCCESS,
    AuditResult.FAILURE.value.lower(): AuditResult.FAILURE,
    "fallido": AuditResult.FAILURE,
    "failed": AuditResult.FAILURE,
    AuditResult.REJECTED.value.lower(): AuditResult.REJECTED,
    "rechazo": AuditResult.REJECTED,
    "rejected": AuditResult.REJECTED,
    AuditResult.CANCELLED.value.lower(): AuditResult.CANCELLED,
    "cancelado": AuditResult.CANCELLED,
    "cancelled": AuditResult.CANCELLED,
    AuditResult.PARTIAL.value.lower(): AuditResult.PARTIAL,
    "cierre_parcial": AuditResult.PARTIAL,
    "partial": AuditResult.PARTIAL,
    "partial_fill": AuditResult.PARTIAL,
}


AUDIT_COLUMNS: List[str] = [
    "timestamp",
    "operation_id",
    "order_id",
    "symbol",
    "evento",
    "resultado",
    "source",
    "estrategias_activas",
    "score",
    "rsi",
    "volumen_relativo",
    "tendencia",
    "razon",
    "capital_actual",
    "config_usada",
    "comentario",
]


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS auditoria (
    timestamp TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    order_id TEXT,
    symbol TEXT NOT NULL,
    evento TEXT NOT NULL,
    resultado TEXT NOT NULL,
    source TEXT,
    estrategias_activas TEXT,
    score REAL,
    rsi REAL,
    volumen_relativo REAL,
    tendencia TEXT,
    razon TEXT,
    capital_actual REAL,
    config_usada TEXT,
    comentario TEXT
)
"""


def _serializar(valor):
    if isinstance(valor, (dict, list)):
        try:
            return json.dumps(valor, ensure_ascii=False)
        except Exception:
            return str(valor)
    return valor


def _normalize_value(
    value: str | StrEnum | None,
    aliases: Dict[str, StrEnum],
    *,
    default: StrEnum,
) -> StrEnum:
    """Normaliza valores usando ``aliases`` y devuelve siempre ``default`` como fallback."""

    if isinstance(value, StrEnum):
        return value
    if value is None:
        return default
    normalized = _coerce_alias_key(value)
    return aliases.get(normalized, default)


def _coerce_alias_key(value: object) -> str:
    """Convierte un valor libre en clave para ``aliases``."""

    return str(value).strip().lower().replace(" ", "_")


def normalize_event(evento: str | AuditEvent | None) -> AuditEvent:
    """Normaliza ``evento`` asegurando uso de :class:`AuditEvent`."""

    return _normalize_value(evento, _EVENT_ALIASES, default=AuditEvent.UNKNOWN)  # type: ignore[arg-type]


def normalize_result(resultado: str | AuditResult | None) -> AuditResult:
    """Normaliza ``resultado`` asegurando uso de :class:`AuditResult`."""

    return _normalize_value(resultado, _RESULT_ALIASES, default=AuditResult.UNKNOWN)  # type: ignore[arg-type]


def registrar_auditoria(
    symbol: str,
    evento: str | AuditEvent,
    resultado: str | AuditResult,
    *,
    operation_id: str | None = None,
    order_id: str | None = None,
    source: str | None = None,
    estrategias_activas=None,
    score=None,
    rsi=None,
    volumen_relativo=None,
    tendencia=None,
    razon=None,
    capital_actual=None,
    config_usada=None,
    comentario=None,
    archivo: str = "informes/auditoria_bot.csv",
    formato: str = "csv",
) -> None:
    """Persiste decisiones críticas del bot utilizando escrituras incrementales.

    El formato ``csv`` utiliza ``csv.DictWriter`` para evitar lecturas completas
    de archivos en cada inserción. Para escenarios de mayor volumen, el formato
    ``sqlite`` permite inserciones ``INSERT`` transaccionales y consultas
    posteriores sin sobrecargar memoria.
    """

    registro = _crear_registro(
        symbol=symbol,
        operation_id=operation_id,
        order_id=order_id,
        source=source,
        evento=evento,
        resultado=resultado,
        estrategias_activas=estrategias_activas,
        score=score,
        rsi=rsi,
        volumen_relativo=volumen_relativo,
        tendencia=tendencia,
        razon=razon,
        capital_actual=capital_actual,
        config_usada=config_usada,
        comentario=comentario,
    )
    ruta_archivo = Path(archivo)
    if ruta_archivo.parent not in (Path(""), Path(".")):
        ruta_archivo.parent.mkdir(parents=True, exist_ok=True)
    formato_normalizado = formato.strip().lower()
    if formato_normalizado == "csv":
        with _lock:
            _append_csv(ruta_archivo, registro)
    elif formato_normalizado == "sqlite":
        with _lock:
            _append_sqlite(ruta_archivo, registro)
    else:
        raise ValueError(
            f"Formato no soportado para auditoría: {formato_normalizado}. "
            "Use 'csv' o 'sqlite'."
        )


def _crear_registro(**kwargs) -> Dict[str, object]:
    registro = {
        "timestamp": datetime.now(UTC).isoformat(),
        "operation_id": _resolve_operation_id(kwargs.get("operation_id")),
        "order_id": _normalize_optional(kwargs.get("order_id")),
        "symbol": kwargs["symbol"],
        "evento": normalize_event(kwargs["evento"]).value,
        "resultado": normalize_result(kwargs["resultado"]).value,
        "source": _normalize_source(kwargs.get("source")),
        "estrategias_activas": _serializar(kwargs.get("estrategias_activas")),
        "score": kwargs.get("score"),
        "rsi": kwargs.get("rsi"),
        "volumen_relativo": kwargs.get("volumen_relativo"),
        "tendencia": kwargs.get("tendencia"),
        "razon": _serializar(kwargs.get("razon")),
        "capital_actual": kwargs.get("capital_actual"),
        "config_usada": _serializar(kwargs.get("config_usada")),
        "comentario": kwargs.get("comentario"),
    }
    return registro


def _normalize_source(source: str | None) -> str | None:
    """Normaliza ``source`` en formato ``snake_case`` legible."""

    if source is None:
        return "unknown"
    cleaned = str(source).strip()
    if not cleaned:
        return "unknown"
    return cleaned.replace(" ", "_")


def _normalize_optional(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_operation_id(operation_id: str | None) -> str:
    """Genera un ``operation_id`` válido si no se provee."""

    candidate = _normalize_optional(operation_id)
    return candidate or str(uuid4())


def _append_csv(ruta_archivo: Path, registro: Dict[str, object]) -> None:
    archivo_existente = ruta_archivo.exists()
    with ruta_archivo.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=AUDIT_COLUMNS)
        if not archivo_existente:
            writer.writeheader()
        writer.writerow({col: registro.get(col) for col in AUDIT_COLUMNS})


def _append_sqlite(ruta_archivo: Path, registro: Dict[str, object]) -> None:
    with closing(sqlite3.connect(ruta_archivo)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(SQLITE_SCHEMA)
        valores = [registro.get(col) for col in AUDIT_COLUMNS]
        placeholders = ",".join(["?"] * len(AUDIT_COLUMNS))
        columnas = ",".join(AUDIT_COLUMNS)
        conn.execute(f"INSERT INTO auditoria ({columnas}) VALUES ({placeholders})", valores)
        conn.commit()
