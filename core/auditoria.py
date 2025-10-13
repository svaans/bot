import csv
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Dict, List

UTC = timezone.utc
_lock = Lock()


AUDIT_COLUMNS: List[str] = [
    "timestamp",
    "symbol",
    "evento",
    "resultado",
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
    symbol TEXT NOT NULL,
    evento TEXT NOT NULL,
    resultado TEXT NOT NULL,
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


def registrar_auditoria(
    symbol: str,
    evento: str,
    resultado: str,
    *,
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
        "symbol": kwargs["symbol"],
        "evento": kwargs["evento"],
        "resultado": kwargs["resultado"],
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
