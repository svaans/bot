"""Gestión centralizada de modos operativos del bot.

Este módulo introduce un modelo de modos con persistencia ligera en SQLite,
exposición de métricas Prometheus y un servicio asíncrono que permite cambiar
el modo de operación en caliente mediante comandos autenticados.

Los modos soportados son:

``real``
    Opera contra el entorno productivo de Binance y requiere claves válidas.

``paper_trading``
    Mantiene al trader en modo simulado sin enviar órdenes reales.

``staging``
    Utiliza los endpoints de pruebas (testnet) y sirve como entorno intermedio.

Para solicitar cambios dinámicos se inserta un comando en la base de datos
``estado/operational_mode.db`` empleando el CLI ``python -m core.operational_mode``.
El servicio interno valida el token SHA-256 proporcionado vía la variable de
entorno ``MODE_CHANGE_TOKEN`` o ``MODE_CHANGE_TOKEN_HASH`` y aplica las
transiciones pertinentes propagando el nuevo estado al trader y a la
configuración global.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import enum
import hashlib
import hmac
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from core.utils.logger import configurar_logger
from observability.metrics import (  # type: ignore[attr-defined]
    BOT_MODO_REAL,
    BOT_OPERATIONAL_MODE,
    BOT_OPERATIONAL_MODE_TRANSITIONS_TOTAL,
)

log = configurar_logger("operational_mode", modo_silencioso=True)


class OperationalMode(str, enum.Enum):
    """Valores normalizados de modo operativo."""

    REAL = "real"
    PAPER_TRADING = "paper_trading"
    STAGING = "staging"

    @property
    def is_real(self) -> bool:
        return self is OperationalMode.REAL

    @property
    def uses_testnet(self) -> bool:
        return self is OperationalMode.STAGING

    @classmethod
    def parse(
        cls,
        raw: str | None,
        *,
        default: "OperationalMode" | None = None,
    ) -> "OperationalMode":
        if raw:
            normalized = raw.strip().lower()
            for member in cls:
                if member.value == normalized:
                    return member
            log.warning("Modo operativo desconocido '%s'; usando default", raw)
        if default is not None:
            return default
        return OperationalMode.PAPER_TRADING

    @classmethod
    def from_bool(cls, modo_real: bool) -> "OperationalMode":
        return cls.REAL if modo_real else cls.PAPER_TRADING


@dataclass(slots=True)
class ModeState:
    """Instantánea persistida del modo actual."""

    mode: OperationalMode
    updated_at: float
    updated_by: str | None = None
    source: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass(slots=True)
class ModeCommand:
    """Representa un comando pendiente de aplicación."""

    command_id: int
    mode: OperationalMode
    token_hash: str
    actor: str | None = None
    reason: str | None = None
    requested_at: float | None = None


class OperationalModeRepository:
    """Persistencia ligera basada en SQLite para modos y comandos."""

    def __init__(self, db_path: str | os.PathLike[str] | None = None) -> None:
        default_path = Path(os.getenv("MODE_DB_PATH", Path("estado") / "operational_mode.db"))
        self._path = Path(db_path) if db_path is not None else default_path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS operational_mode_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    mode TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    updated_by TEXT,
                    source TEXT,
                    metadata TEXT
                );

                CREATE TABLE IF NOT EXISTS operational_mode_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    changed_at REAL NOT NULL,
                    from_mode TEXT,
                    to_mode TEXT NOT NULL,
                    actor TEXT,
                    reason TEXT,
                    trigger TEXT,
                    metadata TEXT
                );

                CREATE TABLE IF NOT EXISTS operational_mode_commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requested_at REAL NOT NULL,
                    mode TEXT NOT NULL,
                    actor TEXT,
                    reason TEXT,
                    token_hash TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    processed_at REAL,
                    result TEXT
                );
                """
            )

    def load_state(self) -> ModeState | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT mode, updated_at, updated_by, source, metadata FROM operational_mode_state WHERE id = 1"
            ).fetchone()
            if row is None:
                return None
            metadata = json.loads(row["metadata"]) if row["metadata"] else None
            return ModeState(
                mode=OperationalMode.parse(row["mode"], default=OperationalMode.PAPER_TRADING),
                updated_at=float(row["updated_at"]),
                updated_by=row["updated_by"],
                source=row["source"],
                metadata=metadata,
            )

    def save_state(self, state: ModeState) -> None:
        payload = json.dumps(state.metadata, ensure_ascii=False) if state.metadata else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO operational_mode_state (id, mode, updated_at, updated_by, source, metadata)
                VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    mode=excluded.mode,
                    updated_at=excluded.updated_at,
                    updated_by=excluded.updated_by,
                    source=excluded.source,
                    metadata=excluded.metadata
                """,
                (state.mode.value, state.updated_at, state.updated_by, state.source, payload),
            )

    def record_transition(
        self,
        *,
        from_mode: OperationalMode | None,
        to_mode: OperationalMode,
        actor: str | None,
        reason: str | None,
        trigger: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        payload = json.dumps(dict(metadata), ensure_ascii=False) if metadata else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO operational_mode_history (
                    changed_at, from_mode, to_mode, actor, reason, trigger, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    time.time(),
                    from_mode.value if from_mode else None,
                    to_mode.value,
                    actor,
                    reason,
                    trigger,
                    payload,
                ),
            )

    def fetch_pending_commands(self, limit: int = 5) -> list[ModeCommand]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, mode, actor, reason, token_hash, requested_at
                FROM operational_mode_commands
                WHERE status = 'pending'
                ORDER BY requested_at ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        commands: list[ModeCommand] = []
        for row in rows:
            commands.append(
                ModeCommand(
                    command_id=int(row["id"]),
                    mode=OperationalMode.parse(row["mode"], default=OperationalMode.PAPER_TRADING),
                    token_hash=str(row["token_hash"]),
                    actor=row["actor"],
                    reason=row["reason"],
                    requested_at=float(row["requested_at"]),
                )
            )
        return commands

    def mark_command(self, command_id: int, *, status: str, result: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE operational_mode_commands
                SET status = ?, processed_at = ?, result = ?
                WHERE id = ?
                """,
                (status, time.time(), result, command_id),
            )

    def enqueue_command(
        self,
        *,
        mode: OperationalMode,
        token_hash: str,
        actor: str | None,
        reason: str | None,
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO operational_mode_commands (requested_at, mode, actor, reason, token_hash)
                VALUES (?, ?, ?, ?, ?)
                """,
                (time.time(), mode.value, actor, reason, token_hash),
            )
            return int(cursor.lastrowid)


class OperationalModeTracker:
    """Actualiza métricas y persistencia ante cambios de modo."""

    def __init__(self, repository: OperationalModeRepository | None = None) -> None:
        self._repository = repository or OperationalModeRepository()

    def _publish_metrics(self, mode: OperationalMode) -> None:
        for candidate in OperationalMode:
            value = 1.0 if candidate is mode else 0.0
            try:
                BOT_OPERATIONAL_MODE.labels(mode=candidate.value).set(value)  # type: ignore[call-arg]
            except Exception:  # pragma: no cover - Prometheus opcional
                pass
        try:
            BOT_MODO_REAL.set(1.0 if mode.is_real else 0.0)  # type: ignore[call-arg]
        except Exception:  # pragma: no cover - Prometheus opcional
            pass

    def bootstrap(self, mode: OperationalMode, *, source: str = "startup") -> None:
        self._publish_metrics(mode)
        state = ModeState(mode=mode, updated_at=time.time(), source=source, metadata=None)
        self._repository.save_state(state)

    def record_transition(
        self,
        *,
        from_mode: OperationalMode | None,
        to_mode: OperationalMode,
        actor: str | None,
        reason: str | None,
        trigger: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self._publish_metrics(to_mode)
        self._repository.record_transition(
            from_mode=from_mode,
            to_mode=to_mode,
            actor=actor,
            reason=reason,
            trigger=trigger,
            metadata=metadata,
        )
        try:
            BOT_OPERATIONAL_MODE_TRANSITIONS_TOTAL.labels(  # type: ignore[call-arg]
                from_mode=(from_mode.value if from_mode else "unknown"),
                to_mode=to_mode.value,
                trigger=trigger,
            ).inc()
        except Exception:  # pragma: no cover - Prometheus opcional
            pass
        self._repository.save_state(
            ModeState(
                mode=to_mode,
                updated_at=time.time(),
                updated_by=actor,
                source=trigger,
                metadata=dict(metadata) if metadata else None,
            )
        )


class OperationalModeService:
    """Servicio asíncrono que aplica cambios de modo bajo demanda."""

    def __init__(
        self,
        *,
        config: Any,
        trader: Any | None,
        event_bus: Any | None = None,
        repository: OperationalModeRepository | None = None,
        poll_interval: float = 5.0,
    ) -> None:
        self._config = config
        self._trader = trader
        self._event_bus = event_bus
        self._repository = repository or OperationalModeRepository()
        self._tracker = OperationalModeTracker(self._repository)
        self._poll_interval = max(1.0, float(poll_interval))
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        initial_mode = self._extract_mode(config)
        self._current_mode = initial_mode
        self._expected_token_hash = self._load_expected_hash()
        self._tracker.bootstrap(initial_mode, source="startup")

    @staticmethod
    def _extract_mode(config: Any) -> OperationalMode:
        candidate = getattr(config, "modo_operativo", None)
        if isinstance(candidate, OperationalMode):
            return candidate
        bool_candidate = bool(getattr(config, "modo_real", False))
        return OperationalMode.from_bool(bool_candidate)

    def _load_expected_hash(self) -> str | None:
        raw_hash = os.getenv("MODE_CHANGE_TOKEN_HASH")
        if raw_hash:
            return raw_hash.strip().lower()
        token = os.getenv("MODE_CHANGE_TOKEN")
        if not token:
            return None
        return hashlib.sha256(token.encode()).hexdigest()

    def start(self) -> asyncio.Task[None]:
        if self._task is None or self._task.done():
            self._stopping.clear()
            self._task = asyncio.create_task(self._run(), name="operational-mode-service")
        return self._task

    async def stop(self) -> None:
        self._stopping.set()
        task = self._task
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(Exception):
            await task
        self._task = None

    async def _run(self) -> None:
        try:
            while not self._stopping.is_set():
                await self._process_pending_commands()
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=self._poll_interval)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            pass

    async def _process_pending_commands(self) -> None:
        commands = self._repository.fetch_pending_commands()
        if not commands:
            return
        for command in commands:
            await self._handle_command(command)

    async def _handle_command(self, command: ModeCommand) -> None:
        expected_hash = self._expected_token_hash
        if not expected_hash:
            log.warning("Comando %s rechazado: token no configurado", command.command_id)
            self._repository.mark_command(command.command_id, status="rejected", result="token_unavailable")
            return
        if not hmac.compare_digest(expected_hash, command.token_hash.lower()):
            log.warning("Token inválido para comando de modo %s", command.command_id)
            self._repository.mark_command(command.command_id, status="rejected", result="invalid_token")
            return

        new_mode = command.mode
        if new_mode == self._current_mode:
            self._repository.mark_command(command.command_id, status="skipped", result="already_active")
            return

        error = self._validate_mode(new_mode)
        if error:
            log.error("Cambio de modo %s→%s rechazado: %s", self._current_mode, new_mode, error)
            self._repository.mark_command(command.command_id, status="rejected", result=error)
            return

        previous = self._current_mode
        self._apply_mode(new_mode)
        metadata = {"command_id": command.command_id}
        if command.requested_at is not None:
            metadata["requested_at"] = command.requested_at
        self._tracker.record_transition(
            from_mode=previous,
            to_mode=new_mode,
            actor=command.actor,
            reason=command.reason,
            trigger="command",
            metadata=metadata,
        )
        self._repository.mark_command(command.command_id, status="applied", result="ok")
        log.info(
            "Modo operativo actualizado de %s a %s por %s", previous.value, new_mode.value, command.actor or "desconocido"
        )

    def _validate_mode(self, mode: OperationalMode) -> str | None:
        cfg = self._config
        api_key = getattr(cfg, "api_key", None)
        api_secret = getattr(cfg, "api_secret", None)
        if mode.is_real:
            if not api_key or not api_secret:
                return "api_credentials_missing"
        if mode.uses_testnet:
            rest = os.getenv("BINANCE_STAGING_REST_URL")
            ws = os.getenv("BINANCE_STAGING_WS_URL")
            if not rest or not ws:
                return "staging_endpoints_missing"
        risk_daily = float(getattr(cfg, "umbral_riesgo_diario", 0.0) or 0.0)
        if risk_daily <= 0.0:
            return "invalid_risk_limit"
        return None

    def _apply_mode(self, mode: OperationalMode) -> None:
        previous = getattr(self, "_current_mode", None)
        self._current_mode = mode
        os.environ["MODO_OPERATIVO"] = mode.value
        os.environ["MODO_REAL"] = "true" if mode.is_real else "false"
        cfg = self._config
        if dataclasses.is_dataclass(cfg):
            try:
                cfg = dataclasses.replace(cfg, modo_real=mode.is_real, modo_operativo=mode)
            except TypeError:
                cfg = dataclasses.replace(cfg, modo_real=mode.is_real)
        else:
            with contextlib.suppress(Exception):
                setattr(cfg, "modo_real", mode.is_real)
            with contextlib.suppress(Exception):
                setattr(cfg, "modo_operativo", mode)
        self._config = cfg
        if self._trader is not None:
            with contextlib.suppress(Exception):
                setattr(self._trader, "modo_real", mode.is_real)
            with contextlib.suppress(Exception):
                setattr(self._trader, "operational_mode", mode)
            with contextlib.suppress(Exception):
                setattr(self._trader, "config", cfg)
        try:
            import config.config as config_module  # type: ignore

            setattr(config_module, "cfg", cfg)
            setattr(config_module, "MODO_REAL", mode.is_real)
            setattr(config_module, "MODO_OPERATIVO", mode)
        except Exception:
            log.debug("No se pudo actualizar config.config tras cambio de modo", exc_info=True)
        if self._event_bus is not None:
            payload = {
                "from": previous.value if isinstance(previous, OperationalMode) else previous,
                "to": mode.value,
                "modo_real": mode.is_real,
            }
            with contextlib.suppress(Exception):
                self._event_bus.emit("operational_mode.changed", payload)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def submit_mode_change(
    mode: str,
    *,
    token: str,
    actor: str | None = None,
    reason: str | None = None,
    repository: OperationalModeRepository | None = None,
) -> int:
    """Inserta un comando de cambio de modo en la cola persistente."""

    repo = repository or OperationalModeRepository()
    operational_mode = OperationalMode.parse(mode, default=OperationalMode.PAPER_TRADING)
    token_hash = _hash_token(token)
    return repo.enqueue_command(mode=operational_mode, token_hash=token_hash, actor=actor, reason=reason)


def _build_cli() -> Any:
    import argparse

    parser = argparse.ArgumentParser(description="Gestiona el modo operativo del bot")
    parser.add_argument("mode", choices=[mode.value for mode in OperationalMode], help="Modo objetivo")
    parser.add_argument("token", help="Token secreto configurado en el bot")
    parser.add_argument("--actor", dest="actor", default=None, help="Identificador del solicitante")
    parser.add_argument("--reason", dest="reason", default=None, help="Motivo del cambio")
    parser.add_argument(
        "--db", dest="db_path", default=None, help="Ruta personalizada para la base de datos de control"
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(list(argv) if argv is not None else None)
    repository = OperationalModeRepository(db_path=args.db_path)
    command_id = submit_mode_change(
        args.mode,
        token=args.token,
        actor=args.actor,
        reason=args.reason,
        repository=repository,
    )
    print(f"Comando registrado con id {command_id}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI manual
    raise SystemExit(main())


__all__ = [
    "OperationalMode",
    "OperationalModeService",
    "OperationalModeRepository",
    "OperationalModeTracker",
    "ModeState",
    "ModeCommand",
    "submit_mode_change",
]