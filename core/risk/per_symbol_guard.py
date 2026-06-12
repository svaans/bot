"""Per-Symbol Loss Streak Guard: reduce riesgo de un símbolo específico tras
una racha de pérdidas consecutivas.

Complementa al equity_dd_guard (portfolio-level) con granularidad por símbolo.
Si un símbolo acumula N pérdidas consecutivas, su riesgo_maximo_diario se
multiplica por factor (0.5) para las siguientes operaciones.

El contador se reinicia cuando el símbolo cierra un trade ganador.

Persistencia: estado/per_symbol_losses.json
  {"BTC/USDT": 2, "ETH/USDT": 0, ...}

Thread-safe. Sin dependencias externas.

Activar con PER_SYMBOL_GUARD_ENABLED=true (o per_symbol_guard_enabled: True).
Umbral por defecto: 2 pérdidas consecutivas (per_symbol_losses_umbral=2).
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

from core.utils.utils import configurar_logger

log = configurar_logger("per_symbol_guard", modo_silencioso=True)

_lock = threading.Lock()
_STATE_PATH = Path("estado/per_symbol_losses.json")


def _leer_estado() -> dict[str, int]:
    try:
        return json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _guardar_estado(estado: dict[str, int]) -> None:
    try:
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _STATE_PATH.write_text(
            json.dumps({k: v for k, v in estado.items() if v > 0}),
            encoding="utf-8",
        )
    except Exception as exc:
        log.warning("per_symbol_guard: no se pudo persistir estado: %s", exc)


def registrar_resultado(symbol: str, ganador: bool) -> None:
    """Registra el resultado de un trade cerrado.

    Llama desde el cierre de posición (order_manager o similar).
    - ganador=True  → resetea el contador de pérdidas del símbolo
    - ganador=False → incrementa el contador
    """
    with _lock:
        estado = _leer_estado()
        if ganador:
            if estado.get(symbol, 0) > 0:
                log.info("per_symbol_guard: %s trade ganador → racha reseteada", symbol)
            estado[symbol] = 0
        else:
            anterior = estado.get(symbol, 0)
            estado[symbol] = anterior + 1
            log.info(
                "per_symbol_guard: %s pérdida %d consecutiva",
                symbol, estado[symbol],
            )
        _guardar_estado(estado)


def factor_reduccion_simbolo(
    symbol: str,
    umbral: int = 2,
    factor: float = 0.5,
) -> float:
    """Retorna factor de reducción (0.5 si racha ≥ umbral, 1.0 si no).

    No modifica el estado — solo consulta.
    """
    with _lock:
        estado = _leer_estado()
        racha = estado.get(symbol, 0)
        if racha >= umbral:
            log.info(
                "per_symbol_guard: %s racha=%d ≥ umbral=%d → riesgo ×%.1f",
                symbol, racha, umbral, factor,
            )
            return factor
        return 1.0


def obtener_racha(symbol: str) -> int:
    """Retorna el número de pérdidas consecutivas actuales de un símbolo."""
    with _lock:
        return _leer_estado().get(symbol, 0)


def resetear_simbolo(symbol: str) -> None:
    """Fuerza el reseteo del contador (útil tras mantenimiento o ajuste manual)."""
    with _lock:
        estado = _leer_estado()
        estado.pop(symbol, None)
        _guardar_estado(estado)
