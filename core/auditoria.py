from __future__ import annotations

import gzip
import hashlib
import json
import logging
import shutil
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import Dict, List
from uuid import uuid4

UTC = timezone.utc
_lock = Lock()
_AUDIT_BASE_DIR = Path("informes")

log = logging.getLogger(__name__)


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


CURRENT_SCHEMA_VERSION = "1.1"

AUDIT_COLUMNS: List[str] = [
    "timestamp",
    "schema_version",
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
    schema_version TEXT NOT NULL,
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

CHECKSUM_SCHEMA = """
CREATE TABLE IF NOT EXISTS auditoria_checksums (
    fecha TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    total_registros INTEGER NOT NULL,
    last_updated TEXT NOT NULL
)
"""


def _current_utc() -> datetime:
    """Retorna ``datetime`` timezone-aware para reutilización y pruebas."""

    return datetime.now(UTC)


def _resolver_ruta_archivo(
    archivo: str | None, formato_normalizado: str, momento_actual: datetime
) -> Path:
    if archivo:
        return Path(archivo)
    if formato_normalizado == "jsonl":
        fecha = momento_actual.strftime("%Y%m%d")
        return _AUDIT_BASE_DIR / fecha / f"auditoria_{fecha}.jsonl"
    if formato_normalizado == "sqlite":
        return _AUDIT_BASE_DIR / "auditoria.db"
    raise ValueError(
        f"Formato no soportado para auditoría: {formato_normalizado}. "
        "Use 'jsonl' o 'sqlite'."
    )


def _comprimir_auditoria_del_dia_anterior(momento_actual: datetime) -> None:
    fecha_anterior = (momento_actual - timedelta(days=1)).strftime("%Y%m%d")
    carpeta = _AUDIT_BASE_DIR / fecha_anterior
    archivo = carpeta / f"auditoria_{fecha_anterior}.jsonl"
    if not archivo.exists():
        return
    destino = archivo.with_suffix(".jsonl.gz")
    if destino.exists():
        try:
            archivo.unlink()
        except OSError as error:
            log.debug("No se pudo eliminar auditoría previa ya comprimida: %s", error)
        _limpiar_carpeta_si_vacia(carpeta)
        return
    try:
        with archivo.open("rb") as origen, gzip.open(destino, "wb") as salida:
            shutil.copyfileobj(origen, salida)
        archivo.unlink()
        _limpiar_carpeta_si_vacia(carpeta)
    except OSError as error:
        log.warning("No fue posible comprimir %s: %s", archivo, error)


def _limpiar_carpeta_si_vacia(carpeta: Path) -> None:
    try:
        if carpeta.exists() and not any(carpeta.iterdir()):
            carpeta.rmdir()
    except OSError as error:
        log.debug("No se pudo limpiar carpeta de auditoría %s: %s", carpeta, error)


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
    archivo: str | None = None,
    formato: str = "jsonl",
) -> None:
    """Persiste decisiones críticas del bot utilizando escrituras incrementales.

    El formato ``jsonl`` produce archivos compatibles con herramientas de
    streaming (``jq``, ``pandas``) sin perder estructuras anidadas. Cuando no se
    indica ``archivo`` se rota automáticamente a ``informes/YYYYMMDD`` y se
    comprime el día previo en ``.gz``. Para escenarios de mayor volumen, el
    formato ``sqlite`` permite inserciones ``INSERT`` transaccionales y consultas
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
    formato_normalizado = formato.strip().lower()
    momento_actual = _current_utc()
    ruta_archivo = _resolver_ruta_archivo(archivo, formato_normalizado, momento_actual)
    if ruta_archivo.parent not in (Path(""), Path(".")):
        ruta_archivo.parent.mkdir(parents=True, exist_ok=True)
    if formato_normalizado == "jsonl":
        with _lock:
            if archivo is None:
                _comprimir_auditoria_del_dia_anterior(momento_actual)
            _append_jsonl(ruta_archivo, registro)
    elif formato_normalizado == "sqlite":
        with _lock:
            _append_sqlite(ruta_archivo, registro)
    else:
        raise ValueError(
            f"Formato no soportado para auditoría: {formato_normalizado}. "
            "Use 'jsonl' o 'sqlite'."
        )


def _crear_registro(**kwargs) -> Dict[str, object]:
    registro = {
        "timestamp": _current_utc().isoformat(),
        "schema_version": CURRENT_SCHEMA_VERSION,
        "operation_id": _resolve_operation_id(kwargs.get("operation_id")),
        "order_id": _normalize_optional(kwargs.get("order_id")),
        "symbol": kwargs["symbol"],
        "evento": normalize_event(kwargs["evento"]).value,
        "resultado": normalize_result(kwargs["resultado"]).value,
        "source": _normalize_source(kwargs.get("source")),
        "estrategias_activas": kwargs.get("estrategias_activas"),
        "score": kwargs.get("score"),
        "rsi": kwargs.get("rsi"),
        "volumen_relativo": kwargs.get("volumen_relativo"),
        "tendencia": kwargs.get("tendencia"),
        "razon": kwargs.get("razon"),
        "capital_actual": kwargs.get("capital_actual"),
        "config_usada": kwargs.get("config_usada"),
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


def _append_jsonl(ruta_archivo: Path, registro: Dict[str, object]) -> None:
    with ruta_archivo.open("a", encoding="utf-8") as jsonfile:
        json_record = json.dumps(registro, ensure_ascii=False)
        jsonfile.write(json_record + "\n")


def _append_sqlite(ruta_archivo: Path, registro: Dict[str, object]) -> None:
    with closing(sqlite3.connect(ruta_archivo)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        _ensure_sqlite_schema(conn)
        valores = [_coerce_sqlite_value(registro.get(col)) for col in AUDIT_COLUMNS]
        placeholders = ",".join(["?"] * len(AUDIT_COLUMNS))
        columnas = ",".join(AUDIT_COLUMNS)
        with conn:
            conn.execute(
                f"INSERT INTO auditoria ({columnas}) VALUES ({placeholders})",
                valores,
            )
            _update_daily_checksum(conn, registro)
            conn.commit()


def _coerce_sqlite_value(value: object) -> object:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value

def _ensure_sqlite_schema(conn: sqlite3.Connection) -> None:
    conn.execute(SQLITE_SCHEMA)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_auditoria_symbol_timestamp "
        "ON auditoria(symbol, timestamp)"
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(auditoria)")}
    if "schema_version" not in columns:
        conn.execute(
            "ALTER TABLE auditoria ADD COLUMN schema_version TEXT NOT NULL DEFAULT '"
            f"{CURRENT_SCHEMA_VERSION}'"
        )
    conn.execute(CHECKSUM_SCHEMA)


def _update_daily_checksum(conn: sqlite3.Connection, registro: Dict[str, object]) -> None:
    timestamp = registro["timestamp"]
    fecha = timestamp[:10]
    digest = _hash_registro(registro)
    existing = conn.execute(
        "SELECT checksum, total_registros FROM auditoria_checksums WHERE fecha = ?",
        (fecha,),
    ).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO auditoria_checksums (fecha, checksum, total_registros, last_updated) "
            "VALUES (?, ?, ?, ?)",
            (fecha, digest, 1, timestamp),
        )
        return
    previous_checksum, total_registros = existing
    acumulado = _combinar_checksums(str(previous_checksum), digest)
    conn.execute(
        "UPDATE auditoria_checksums SET checksum = ?, total_registros = ?, last_updated = ? "
        "WHERE fecha = ?",
        (acumulado, int(total_registros) + 1, timestamp, fecha),
    )


def _hash_registro(registro: Dict[str, object]) -> str:
    serializado = json.dumps(registro, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serializado.encode("utf-8")).hexdigest()


def _combinar_checksums(valor_actual: str, nuevo: str) -> str:
    return hashlib.sha256(f"{valor_actual}{nuevo}".encode("utf-8")).hexdigest()


def verificar_integridad_sqlite(ruta_archivo: Path) -> bool:
    """Verifica que los registros de auditoría coincidan con los checksums diarios."""

    if not ruta_archivo.exists():
        raise FileNotFoundError(ruta_archivo)
    with closing(sqlite3.connect(ruta_archivo)) as conn:
        conn.row_factory = sqlite3.Row
        _ensure_sqlite_schema(conn)
        almacenados = {
            row["fecha"]: {
                "checksum": str(row["checksum"]),
                "total_registros": int(row["total_registros"]),
            }
            for row in conn.execute(
                "SELECT fecha, checksum, total_registros FROM auditoria_checksums"
            )
        }
        cursor = conn.execute(
            f"SELECT {', '.join(AUDIT_COLUMNS)} FROM auditoria ORDER BY timestamp"
        )
        calculados: dict[str, dict[str, object]] = {}
        for row in cursor:
            registro = {col: row[col] for col in AUDIT_COLUMNS}
            fecha = registro["timestamp"][:10]
            digest = _hash_registro(registro)
            existente = calculados.setdefault(fecha, {"checksum": None, "total": 0})
            if existente["checksum"] is None:
                existente["checksum"] = digest
            else:
                existente["checksum"] = _combinar_checksums(
                    str(existente["checksum"]), digest
                )
            existente["total"] = int(existente["total"]) + 1
        if not almacenados and not calculados:
            return True
        if set(almacenados) != set(calculados):
            return False
        for fecha, valores in calculados.items():
            registro_almacenado = almacenados.get(fecha)
            if registro_almacenado is None:
                return False
            if registro_almacenado["checksum"] != valores["checksum"]:
                return False
            if registro_almacenado["total_registros"] != valores["total"]:
                return False
        return True