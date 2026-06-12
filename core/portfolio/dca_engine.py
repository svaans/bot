"""DCA Engine: inversión periódica automática por símbolo.

Revolut Robo-Advisor compra en cada período independientemente del timing
de mercado. Aquí implementamos lo mismo: cada N días (configurable),
si el intervalo se cumplió → permitir entrada aunque no haya señal técnica.

Persiste último DCA ejecutado en estado/dca_state.json
Thread-safe. Sin dependencias externas.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from core.utils.logger import configurar_logger

log = configurar_logger("dca_engine", modo_silencioso=True)

_lock = threading.Lock()
_STATE_PATH = Path("estado/dca_state.json")
UTC = timezone.utc


def _leer_estado() -> dict[str, str]:
    try:
        return json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _guardar_estado(estado: dict[str, str]) -> None:
    try:
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _STATE_PATH.write_text(json.dumps(estado, indent=2), encoding="utf-8")
    except Exception as exc:
        log.warning("dca_engine: no se pudo persistir estado: %s", exc)


def dca_permite_entrada(symbol: str, config: Any) -> bool:
    """True si el intervalo DCA se cumplió para este símbolo.

    No bloquea — si hay error retorna False (conservador).
    Bypasses señales técnicas pero NO los filtros macro/riesgo.
    """
    try:
        enabled = bool(getattr(config, "dca_enabled", False) or
                      (hasattr(config, "get") and config.get("dca_enabled", False)))
        if not enabled:
            return False

        interval_days = int(getattr(config, "dca_interval_days",
                            (config.get("dca_interval_days", 7)
                             if hasattr(config, "get") else 7)))

        with _lock:
            estado = _leer_estado()
            last_str = estado.get(symbol)

            if last_str is None:
                log.info("dca_engine: %s — primer DCA → permitido", symbol)
                return True

            last_dt = datetime.fromisoformat(last_str)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=UTC)

            ahora = datetime.now(UTC)
            dias_desde = (ahora - last_dt).days

            if dias_desde >= interval_days:
                log.info(
                    "dca_engine: %s — %d días desde último DCA (intervalo=%d) → permitido",
                    symbol, dias_desde, interval_days,
                )
                return True

            log.debug(
                "dca_engine: %s — %d/%d días → no toca DCA aún",
                symbol, dias_desde, interval_days,
            )
            return False
    except Exception as exc:
        log.warning("dca_engine: error evaluando DCA para %s: %s", symbol, exc)
        return False


def registrar_dca_ejecutado(symbol: str) -> None:
    """Registra que el DCA se ejecutó hoy para este símbolo."""
    with _lock:
        estado = _leer_estado()
        estado[symbol] = datetime.now(UTC).isoformat()
        _guardar_estado(estado)
        log.info("dca_engine: %s DCA registrado en %s", symbol, estado[symbol])


def dias_hasta_proximo_dca(symbol: str, interval_days: int = 7) -> int:
    """Retorna cuántos días faltan para el próximo DCA de este símbolo."""
    with _lock:
        estado = _leer_estado()
        last_str = estado.get(symbol)
        if last_str is None:
            return 0
        last_dt = datetime.fromisoformat(last_str)
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=UTC)
        dias_desde = (datetime.now(UTC) - last_dt).days
        return max(0, interval_days - dias_desde)


def resetear_dca(symbol: str | None = None) -> None:
    """Resetea el estado DCA de un símbolo o de todos si symbol=None."""
    with _lock:
        estado = _leer_estado()
        if symbol:
            estado.pop(symbol, None)
        else:
            estado.clear()
        _guardar_estado(estado)
