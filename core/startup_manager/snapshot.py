"""Gestión de snapshots y verificaciones auxiliares."""

from __future__ import annotations

import asyncio
import errno
import json
import os
import secrets
import time
from importlib import import_module
from pathlib import Path
from typing import Any, Optional

import aiohttp

from core.utils.log_utils import format_exception_for_log

SNAPSHOT_PATH = Path('estado/startup_snapshot.json')


def _current_snapshot_path() -> Path:
    module = import_module("core.startup_manager")
    path = getattr(module, "SNAPSHOT_PATH", SNAPSHOT_PATH)
    if not isinstance(path, Path):
        path = Path(path)
    return path


class SnapshotMixin:
    """Persistencia de estado y comprobaciones de salud."""

    trader: Any
    config: Any
    log: Any
    _previous_snapshot: Optional[dict[str, Any]]
    _restart_alert_seconds: float

    def _resolve_restart_alert_threshold(self) -> float:
        raw = os.getenv("STARTUP_RESTART_ALERT_SECONDS")
        try:
            value = float(raw) if raw else 300.0
        except (TypeError, ValueError):
            value = 300.0
        return max(value, 0.0)

    def _read_snapshot(self) -> dict[str, Any] | None:
        path = _current_snapshot_path()
        try:
            with path.open('r', encoding='utf-8') as fh:
                data = json.load(fh)
        except FileNotFoundError:
            return None
        except json.JSONDecodeError as exc:
            self.log.warning(
                "Snapshot previo ilegible: %s",
                format_exception_for_log(exc),
            )
            return None
        except Exception as exc:  # pragma: no cover
            self.log.debug("Fallo al leer snapshot previo: %s", exc, exc_info=True)
            return None
        if not isinstance(data, dict):
            self.log.warning("Snapshot previo con formato inesperado: %s", type(data).__name__)
            return None
        return data

    def _inspect_previous_snapshot(self) -> None:
        self._previous_snapshot = None
        snapshot = self._read_snapshot()
        if not snapshot:
            return
        self._previous_snapshot = snapshot

        symbols = snapshot.get('symbols')
        if not isinstance(symbols, list):
            symbols = [symbols] if symbols is not None else []
        modo_real = bool(snapshot.get('modo_real', False))
        modo_operativo = snapshot.get('modo_operativo')
        timestamp = snapshot.get('timestamp')
        age: float | None
        if isinstance(timestamp, (int, float)):
            age = max(0.0, time.time() - float(timestamp))
        else:
            age = None

        log_payload = {
            'symbols': symbols,
            'modo_real': modo_real,
            'modo_operativo': modo_operativo,
            'age_seconds': age,
        }
        self.log.info(
            'Snapshot previo detectado',
            extra={'startup_snapshot': log_payload},
        )

        if (
            age is not None
            and self._restart_alert_seconds > 0
            and age < self._restart_alert_seconds
        ):
            self.log.warning(
                'Reinicio detectado %.1f segundos después del snapshot previo; revisar estabilidad.',
                age,
                extra={'startup_snapshot': log_payload},
            )

    def _snapshot(self) -> None:
        assert self.config is not None
        data = {
            'symbols': getattr(self.config, 'symbols', []),
            'modo_real': getattr(self.config, 'modo_real', False),
            'modo_operativo': getattr(self.config, 'modo_operativo', None),
            'timestamp': time.time(),
        }
        trader = self.trader
        if trader is not None:
            persistencia = getattr(trader, 'persistencia', None)
            export_fn = getattr(persistencia, 'export_state', None)
            if callable(export_fn):
                try:
                    estado = export_fn()
                except Exception:
                    self.log.debug(
                        'No se pudo exportar estado de PersistenciaTecnica para snapshot',
                        exc_info=True,
                    )
                else:
                    if estado:
                        data['persistencia_tecnica'] = estado
        try:
            path = _current_snapshot_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.log.error(
                'No se pudo guardar snapshot: %s',
                format_exception_for_log(e),
            )

    def _restore_persistencia_state(self) -> None:
        trader = self.trader
        if trader is None:
            return
        snapshot = self._previous_snapshot
        if not snapshot:
            return
        estado = snapshot.get('persistencia_tecnica')
        if not estado:
            return
        persistencia = getattr(trader, 'persistencia', None)
        if persistencia is None:
            return
        load_fn = getattr(persistencia, 'load_state', None)
        if callable(load_fn):
            try:
                load_fn(estado)
            except Exception:
                self.log.debug(
                    'No se pudo restaurar estado de PersistenciaTecnica desde snapshot',
                    exc_info=True,
                )

    async def _check_clock_drift(self) -> bool:
        """Comprueba desfase local vs Binance. Solo fuerza fallo si el drift es alto.

        Antes, cualquier excepción no tipada devolvía False y pasabas a modo papel
        aunque ``MODO_REAL=true`` en ``claves.env``; además el uso de ``session.get``
        era frágil entre versiones de aiohttp.
        """
        max_drift = float(os.getenv("CLOCK_DRIFT_MAX_SECONDS", "10.0"))
        timeout = aiohttp.ClientTimeout(total=5)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get("https://api.binance.com/api/v3/time") as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            server_ms = data.get("serverTime")
            if not server_ms:
                self.log.warning(
                    "Respuesta /api/v3/time sin serverTime; omitiendo comprobación estricta de reloj."
                )
                return True
            server = float(server_ms) / 1000.0
            drift = abs(server - time.time())
            if drift >= max_drift:
                self.log.error(
                    "Desfase de reloj frente a Binance: %.3fs (máximo %.3fs). "
                    "Sincroniza la hora en Windows o define CLOCK_DRIFT_MAX_SECONDS.",
                    drift,
                    max_drift,
                )
                return False
            if drift >= 0.5:
                self.log.warning(
                    "Desfase de reloj frente a Binance: %.3fs; las peticiones firmadas "
                    "usan compensación automática (CCXT). Conviene activar sincronización NTP.",
                    drift,
                )
            return True
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.log.warning(
                "No se pudo obtener la hora de Binance: %s. Omitiendo verificación de reloj.",
                e,
            )
            return True
        except Exception as e:
            self.log.warning(
                "Error inesperado en comprobación de reloj (%s): %s. "
                "Se continúa sin forzar modo papel (respeta MODO_REAL del entorno).",
                type(e).__name__,
                e,
            )
            return True

    async def _check_storage(self) -> bool:
        """Comprueba que el directorio del snapshot sea creable y escribible.

        Usa un nombre de fichero único y reintentos al borrar para evitar falsos
        negativos en Windows (archivo bloqueado / WinError 32) frente al nombre
        fijo ``tmp_check`` y procesos concurrentes.
        """
        path = _current_snapshot_path()
        try:
            directory = path.resolve().parent
        except OSError as exc:
            self.log.error(
                "Storage: ruta de snapshot inválida (%s): %s",
                path,
                format_exception_for_log(exc),
            )
            return False
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            self.log.error(
                "Storage: no se pudo crear el directorio %s: %s",
                directory,
                format_exception_for_log(exc),
            )
            return False
        tmp = directory / f".storage_probe_{secrets.token_hex(8)}"
        try:
            tmp.write_text("ok", encoding="utf-8")
        except OSError as exc:
            self.log.error(
                "Storage: no se pudo escribir en %s (permisos, ruta o cuota): %s",
                directory,
                format_exception_for_log(exc),
            )
            return False
        for attempt in range(8):
            try:
                tmp.unlink()
                return True
            except OSError as exc:
                win_share = getattr(exc, "winerror", None) == 32
                busy = exc.errno in (errno.EACCES, errno.EPERM, errno.EBUSY) or win_share
                if not busy:
                    self.log.error(
                        "Storage: no se pudo borrar el fichero temporal %s: %s",
                        tmp,
                        format_exception_for_log(exc),
                    )
                    return False
                if attempt >= 7:
                    self.log.warning(
                        "Storage: escritura verificada en %s pero no se pudo borrar "
                        "el temporal %s tras reintentos (%s). Puedes eliminar ese "
                        "archivo si queda huérfano.",
                        directory,
                        tmp,
                        format_exception_for_log(exc),
                    )
                    return True
                time.sleep(0.05 * (attempt + 1))
        return True
