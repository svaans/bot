# core/vela/helpers.py — utilidades del pipeline (vela, df, fastpath, skip)
from __future__ import annotations

import contextlib
import hashlib
import math
import os
from collections import deque
from typing import Any, Deque, Dict, Mapping, Optional, Tuple

import pandas as pd

from core.vela.metrics_definitions import DEFAULT_MIN_BARS

COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")


def _attach_timeframe(df: pd.DataFrame, timeframe: Optional[str]) -> None:
    """Adjunta el timeframe al DataFrame sin contaminar columnas."""

    if not timeframe:
        return

    try:
        object.__setattr__(df, "tf", timeframe)
    except Exception:
        with contextlib.suppress(Exception):
            df.attrs["tf"] = timeframe


def _attach_symbol(df: pd.DataFrame, symbol: Optional[str]) -> None:
    """Expone el símbolo asociado al ``DataFrame`` mediante ``attrs``."""

    if not symbol:
        return

    normalized = str(symbol).upper()
    try:
        object.__setattr__(df, "symbol", normalized)
    except Exception:
        with contextlib.suppress(Exception):
            df.attrs["symbol"] = normalized


def _resolve_min_bars(trader: Any, default: int = DEFAULT_MIN_BARS) -> int:
    """Determina el mínimo de velas requerido para evaluar estrategias."""

    candidatos: list[Any] = []

    for attr in ("min_bars", "min_buffer_candles", "min_velas"):
        candidatos.append(getattr(trader, attr, None))

    cfg = getattr(trader, "config", None)
    if cfg is not None:
        for attr in ("min_bars", "min_buffer_candles", "min_velas", "min_velas_evaluacion"):
            candidatos.append(getattr(cfg, attr, None))

    for valor in candidatos:
        if valor is None:
            continue
        try:
            entero = int(valor)
        except (TypeError, ValueError):
            continue
        if entero > 0:
            return entero

    return default


def _is_num(x: Any) -> bool:
    try:
        return not (x is None or isinstance(x, bool) or math.isnan(float(x)) or math.isinf(float(x)))
    except Exception:
        return False


def _resolve_entrada_cooldown_tras_crear_failed_sec(trader: Any, symbol: str) -> float:
    """Segundos de pausa tras un ``crear_failed`` (config + overrides por símbolo)."""

    cfg = getattr(trader, "config", None)
    if cfg is None:
        return 0.0
    sym_u = str(symbol or "").strip().upper()
    per = getattr(cfg, "entrada_cooldown_tras_crear_failed_por_symbol", None)
    if isinstance(per, dict) and sym_u in per:
        try:
            return max(0.0, float(per[sym_u]))
        except (TypeError, ValueError):
            pass
    try:
        return max(0.0, float(getattr(cfg, "entrada_cooldown_tras_crear_failed_sec", 0.0) or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _entry_open_notify_dedup_key(
    symbol: str,
    side: str,
    timeframe_label: str | None,
    propuesta: Mapping[str, Any],
    vela: Mapping[str, Any] | None,
) -> str | None:
    """Clave estable para deduplicar el aviso de apertura en NotificationManager."""

    ts = propuesta.get("timestamp")
    if ts is None and vela is not None:
        ts = (
            vela.get("close_time")
            or vela.get("event_time")
            or vela.get("open_time")
            or vela.get("timestamp")
        )
    if ts is None:
        return None
    try:
        ts_norm = int(ts)
    except (TypeError, ValueError):
        ts_norm = ts
    sym_u = str(symbol).strip().upper()
    tf = str(timeframe_label or "unknown").strip()
    return f"entry_open:{sym_u}:{tf}:{ts_norm}:{str(side).lower()}"


def _format_entry_open_notification(
    side: str,
    symbol: str,
    precio: float,
    propuesta: Mapping[str, Any],
    *,
    timeframe_label: str | None,
) -> str:
    """Texto breve para Telegram/log con contexto de la decisión de entrada."""

    parts: list[str] = [f"Abrir {side} {symbol} @ {precio:.6f}"]
    score = propuesta.get("score")
    if score is not None:
        try:
            parts.append(f"score={float(score):.4f}")
        except (TypeError, ValueError):
            parts.append(f"score={score}")
    if timeframe_label:
        parts.append(f"tf={timeframe_label}")
    meta = propuesta.get("meta")
    if isinstance(meta, Mapping):
        mp = meta.get("match_parcial")
        if mp is not None:
            try:
                parts.append(f"match={float(mp):.3f}")
            except (TypeError, ValueError):
                parts.append(f"match={mp}")
        po = meta.get("persistencia_ok")
        if po is not None:
            parts.append(f"persist_ok={bool(po)}")
    return " | ".join(parts)


def _normalize_flag(value: Any) -> Optional[bool]:
    """Convierte un valor potencialmente heterogéneo a booleano."""

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        # Algunos feeds utilizan 0/1 o 0.0/1.0 para indicar cierre
        if math.isnan(float(value)):
            return None
        return bool(int(value))
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"", "none"}:
            return None
        if normalized in {"true", "1", "yes", "y", "closed", "final"}:
            return True
        if normalized in {"false", "0", "no", "n", "open", "partial"}:
            return False
    return None


def _validar_candle(c: dict) -> Tuple[bool, str]:
    # Campos mínimos
    for k in COLUMNS:
        if k not in c:
            return False, f"missing_{k}"
        if not _is_num(c[k]):
            return False, f"nan_{k}"
    # Estado de cierre (no evaluar velas incompletas)
    for flag_name in ("is_closed", "is_final", "final"):
        flag_value = _normalize_flag(c.get(flag_name))
        if flag_value is False:
            return False, "incomplete"
    strict_closed = os.getenv("PROCESAR_VELA_STRICT_CLOSED", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if strict_closed:
        any_true = False
        any_key = False
        for flag_name in ("is_closed", "is_final", "final"):
            if flag_name not in c:
                continue
            any_key = True
            if _normalize_flag(c.get(flag_name)) is True:
                any_true = True
                break
        if not any_key:
            return False, "closed_flag_missing"
        if not any_true:
            return False, "incomplete"
    # Orden temporal
    if int(c["timestamp"]) <= 0:
        return False, "bad_ts"
    # OHLC mínimos
    o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
    if not (l <= o <= h and l <= cl <= h and h >= l and o > 0 and cl > 0):
        return False, "bad_ohlc"
    return True, ""


def _hash_buffer(items: Deque[dict]) -> Tuple[int, int]:
    """Pequeño hash barato: (len, last_ts)."""
    if not items:
        return (0, 0)
    return (len(items), int(items[-1].get("timestamp", 0)))


def _dataframe_fingerprint(df: Optional[pd.DataFrame]) -> Tuple[int, int]:
    """Genera una huella ligera basada en len/timestamp para invalidar caches."""

    if df is None:
        return (0, 0)

    length = len(df)
    if length == 0:
        return (0, 0)

    if "timestamp" not in df.columns:
        return (length, 0)

    try:
        last_value = df["timestamp"].iloc[-1]
    except Exception:
        return (length, 0)

    try:
        last_ts = int(float(last_value))
    except (TypeError, ValueError):
        last_ts = 0

    return (length, last_ts)


def _get_fastpath_mode(state: Any) -> str:
    """Obtiene el modo de fastpath almacenado en el estado del trader."""

    default_mode = "normal"
    if state is None:
        return default_mode

    try:
        if hasattr(state, "fastpath_mode"):
            value = getattr(state, "fastpath_mode", default_mode)
            return str(value or default_mode)
    except Exception:
        pass

    if isinstance(state, dict):
        value = state.get("fastpath_mode", default_mode)
        return str(value or default_mode)

    try:
        value = getattr(state, "fastpath_mode", default_mode)
        return str(value or default_mode)
    except Exception:
        return default_mode


def _set_fastpath_mode(state: Any, mode: str) -> None:
    """Actualiza el modo de fastpath en el estado del trader si es posible."""

    if state is None:
        return

    normalized = str(mode)

    try:
        if hasattr(state, "fastpath_mode"):
            setattr(state, "fastpath_mode", normalized)
            return
    except Exception:
        pass

    if isinstance(state, dict):
        state["fastpath_mode"] = normalized
        return

    try:
        setattr(state, "fastpath_mode", normalized)
    except Exception:
        return


def _resolve_trace_id(payload: Mapping[str, Any] | None) -> Optional[str]:
    """Genera un identificador determinístico para la vela recibida."""

    if not isinstance(payload, Mapping):
        return None

    symbol = str(payload.get("symbol") or "").upper()
    if not symbol:
        return None

    ts_candidate = (
        payload.get("timestamp")
        or payload.get("close_time")
        or payload.get("open_time")
        or payload.get("event_time")
    )

    if ts_candidate is None:
        return None

    try:
        ts_int = int(float(ts_candidate))
    except (TypeError, ValueError):
        return None

    if ts_int <= 0:
        return None

    base = f"{symbol}:{ts_int}".encode("utf-8")
    try:
        return hashlib.blake2b(base, digest_size=12).hexdigest()
    except Exception:  # pragma: no cover - errores improbables de hashlib
        return None


def _mark_skip(
    target: dict,
    reason: str,
    details: Optional[dict] = None,
    *,
    gate: Optional[str] = None,
    score: Any | None = None,
) -> None:
    """Adjunta metadatos de skip al diccionario de la vela."""

    if not isinstance(target, dict):
        return

    target["_df_skip_reason"] = reason
    payload: dict[str, Any] | None = None
    if isinstance(details, dict):
        payload = dict(details)

    if payload is None:
        payload = {}

    gate_value = gate or payload.get("gate")
    if gate_value is None and reason:
        gate_value = reason
    if gate_value is not None:
        payload["gate"] = str(gate_value)

    score_value: Any | None = score
    if score_value is None and "score" in payload:
        score_value = payload.get("score")
    if score_value is not None:
        if _is_num(score_value):
            payload["score"] = float(score_value)
        else:
            payload["score"] = score_value

    if payload:
        target["_df_skip_details"] = payload
    else:
        target.pop("_df_skip_details", None)

    trace_id = _resolve_trace_id(target)

    if trace_id:
        target["_df_trace_id"] = trace_id
    else:
        target.pop("_df_trace_id", None)


def _normalize_timestamp(value: Any) -> Optional[float]:
    """Normaliza timestamps en segundos (acepta ms)."""

    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    if numeric >= 1e11:
        return numeric / 1000.0
    return numeric
