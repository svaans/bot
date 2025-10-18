"""Coordinación de persistencia de estado crítico entre módulos."""

from __future__ import annotations

import json
import os
import threading
import time
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, MutableMapping

from core.utils.logger import configurar_logger

PersistenceDump = Callable[[], Mapping[str, Any] | MutableMapping[str, Any] | list | None]
PersistenceLoad = Callable[[Any], None]


@dataclass(slots=True)
class PersistenceEntry:
    """Mantiene las rutinas de persistencia registradas por un módulo."""

    dump: PersistenceDump
    load: PersistenceLoad | None = None
    priority: int = 0


@dataclass(slots=True)
class CriticalState:
    """Representa el estado persistido en disco."""

    timestamp: float
    reason: str | None
    entries: Dict[str, Any]


_STATE_FILE_ENV = "CRITICAL_STATE_PATH"
_STATE_FILE_DEFAULT = Path("estado/critical_state.json")

_log = configurar_logger("critical_state", modo_silencioso=True)

_registry: Dict[str, PersistenceEntry] = {}
_cached_entries: Dict[str, Any] = {}
_state_lock = threading.RLock()
_state_loaded = False


def _state_file() -> Path:
    raw = os.getenv(_STATE_FILE_ENV)
    if raw:
        try:
            path = Path(raw)
        except Exception:  # pragma: no cover - defensivo
            return _STATE_FILE_DEFAULT
        return path
    return _STATE_FILE_DEFAULT


def _load_state_file() -> None:
    global _state_loaded
    if _state_loaded:
        return
    with _state_lock:
        if _state_loaded:
            return
        path = _state_file()
        try:
            with path.open("r", encoding="utf-8") as fh:
                raw_data = json.load(fh)
        except FileNotFoundError:
            _cached_entries.clear()
            _state_loaded = True
            return
        except json.JSONDecodeError as exc:  # pragma: no cover - formato corrupto
            _log.warning("Estado crítico ilegible (%s): %s", path, exc)
            _cached_entries.clear()
            _state_loaded = True
            return
        except OSError as exc:  # pragma: no cover - error de E/S
            _log.warning("No se pudo leer estado crítico (%s): %s", path, exc)
            _cached_entries.clear()
            _state_loaded = True
            return

        entries = {}
        if isinstance(raw_data, Mapping):
            payload = raw_data.get("entries") if "entries" in raw_data else raw_data
            if isinstance(payload, Mapping):
                entries = {str(key): value for key, value in payload.items()}
        _cached_entries.clear()
        _cached_entries.update(entries)
        _state_loaded = True


def restore_critical_state() -> Dict[str, Any]:
    """Carga el estado persistido y lo ofrece a las entradas registradas."""

    _load_state_file()
    with _state_lock:
        snapshot = {key: deepcopy(value) for key, value in _cached_entries.items()}
        for name, entry in list(_registry.items()):
            data = snapshot.get(name)
            if data is None:
                continue
            if entry.load is None:
                continue
            try:
                entry.load(deepcopy(data))
            except Exception:  # pragma: no cover - defensivo
                _log.exception("Fallo restaurando estado crítico para %s", name)
        return snapshot


def get_persisted_state(name: str, default: Any | None = None) -> Any | None:
    """Devuelve una copia del estado persistido para ``name`` si existe."""

    _load_state_file()
    with _state_lock:
        if name not in _cached_entries:
            return default
        return deepcopy(_cached_entries[name])


def register_state(
    name: str,
    *,
    dump: PersistenceDump,
    load: PersistenceLoad | None = None,
    priority: int = 0,
) -> None:
    """Registra un módulo en la coordinación de persistencia."""

    if not name:
        raise ValueError("El nombre de estado no puede ser vacío")
    _load_state_file()
    with _state_lock:
        _registry[name] = PersistenceEntry(dump=dump, load=load, priority=int(priority))
        if load is None:
            return
        data = _cached_entries.get(name)
        if data is None:
            return
        try:
            load(deepcopy(data))
        except Exception:  # pragma: no cover - defensivo
            _log.exception("Fallo restaurando estado crítico para %s", name)


def unregister_state(name: str) -> None:
    """Elimina un módulo del registro de persistencia."""

    with _state_lock:
        _registry.pop(name, None)


def persist_critical_state(*, reason: str | None = None) -> CriticalState | None:
    """Serializa todos los estados registrados y los guarda en disco."""

    _load_state_file()
    path = _state_file()
    with _state_lock:
        entries: Dict[str, Any] = {}
        for name, entry in sorted(
            _registry.items(), key=lambda item: item[1].priority, reverse=True
        ):
            try:
                data = entry.dump()
            except Exception:
                _log.exception("Fallo exportando estado crítico para %s", name)
                continue
            if data in (None, {}):
                continue
            try:
                entries[name] = json.loads(json.dumps(data))
            except TypeError:
                _log.warning(
                    "Estado crítico no serializable para %s; se omite", name
                )
                continue
        payload = {
            "timestamp": time.time(),
            "reason": reason,
            "entries": entries,
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_suffix(".tmp")
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)
                fh.flush()
                os.fsync(fh.fileno())
            tmp_path.replace(path)
        except OSError as exc:
            _log.error("No se pudo persistir estado crítico en %s: %s", path, exc)
            return None
        _cached_entries.clear()
        _cached_entries.update(entries)
        return CriticalState(timestamp=payload["timestamp"], reason=reason, entries=entries)


__all__ = [
    "CriticalState",
    "PersistenceEntry",
    "get_persisted_state",
    "persist_critical_state",
    "register_state",
    "restore_critical_state",
    "unregister_state",
]