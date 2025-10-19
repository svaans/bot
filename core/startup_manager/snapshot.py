"""Gestión de snapshots y verificaciones auxiliares."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import time
from importlib import import_module
from pathlib import Path
from typing import Any, Optional

import aiohttp

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
            self.log.warning("Snapshot previo ilegible: %s", exc)
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
            self.log.error(f'No se pudo guardar snapshot: {e}')

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
        try:
            async with aiohttp.ClientSession() as session:
                request = session.get("https://api.binance.com/api/v3/time", timeout=5)
                ctx = await request if inspect.isawaitable(request) else request
                if hasattr(ctx, "__aenter__"):
                    async with ctx as resp:
                        data = await resp.json()
                elif hasattr(ctx, "json"):
                    data = await ctx.json()
                else:
                    text = await ctx.text() if hasattr(ctx, "text") else "{}"
                    data = json.loads(text or "{}")
            server = data.get("serverTime", 0) / 1000
            drift = abs(server - time.time())
            return drift < 0.5
        except Exception as e:
            client_error = getattr(aiohttp, "ClientError", ())
            if not isinstance(client_error, tuple):
                client_error = (client_error,)
            tolerable = client_error + (asyncio.TimeoutError,)
            if isinstance(e, tolerable):
                self.log.warning(
                    "No se pudo obtener la hora de Binance: %s. Omitiendo verificación de reloj.",
                    e,
                )
                return True
            return False

    async def _check_storage(self) -> bool:
        try:
            path = _current_snapshot_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.parent / 'tmp_check'
            tmp.write_text('ok')
            tmp.unlink()
            return True
        except Exception:
            return False
