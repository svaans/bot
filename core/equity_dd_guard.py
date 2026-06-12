"""Equity Drawdown Guard: reduce riesgo_maximo_diario cuando el portfolio
está en drawdown respecto a su máximo histórico.

Lee el capital disponible de estado/capital.json (CapitalRepository) y
persiste el pico en estado/equity_peak.json. Thread-safe, sin dependencias
externas.

Activar en producción con EQUITY_DD_FILTER_ENABLED=true (o
equity_dd_filter_enabled: True en ProductionConfig). El umbral por defecto
es 10% (equity_dd_reduccion_umbral=0.10) y el factor de reducción 0.5
(riesgo a la mitad cuando DD ≥ umbral).
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

from core.utils.utils import configurar_logger

log = configurar_logger("equity_dd_guard", modo_silencioso=True)

_lock = threading.Lock()
_PEAK_PATH = Path("estado/equity_peak.json")
_CAPITAL_PATH = Path("estado/capital.json")


def _leer_capital_disponible() -> float:
    try:
        data = json.loads(_CAPITAL_PATH.read_text(encoding="utf-8"))
        return float(data.get("disponible_global", 0.0))
    except Exception:
        return 0.0


def _leer_pico() -> float:
    try:
        data = json.loads(_PEAK_PATH.read_text(encoding="utf-8"))
        return float(data.get("peak", 0.0))
    except Exception:
        return 0.0


def _guardar_pico(peak: float) -> None:
    try:
        _PEAK_PATH.parent.mkdir(parents=True, exist_ok=True)
        _PEAK_PATH.write_text(json.dumps({"peak": round(peak, 4)}), encoding="utf-8")
    except Exception as exc:
        log.warning("equity_dd_guard: no se pudo persistir pico: %s", exc)


def factor_reduccion_riesgo(
    dd_umbral: float = 0.10,
    factor: float = 0.5,
) -> float:
    """Retorna 0.5 si el portfolio está en DD ≥ dd_umbral, 1.0 si no.

    Actualiza el pico persistido si el capital actual lo supera.
    Devuelve 1.0 (sin cambio) si no puede leer el capital.
    """
    with _lock:
        capital = _leer_capital_disponible()
        if capital <= 0.0:
            return 1.0
        pico = _leer_pico()
        if capital > pico:
            _guardar_pico(capital)
            pico = capital
        dd = (pico - capital) / pico if pico > 0 else 0.0
        if dd >= dd_umbral:
            log.info(
                "equity_dd_guard: DD=%.1f%% ≥ umbral %.0f%% → riesgo ×%.1f",
                dd * 100, dd_umbral * 100, factor,
            )
            return factor
        return 1.0
