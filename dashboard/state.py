"""Almacén de estado en tiempo real para el dashboard web."""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

_lock = threading.Lock()

_state: Dict[str, Any] = {
    "uptime_start": time.time(),
    "ws_connected": False,
    "ws_failure_reason": None,
    "consumer_states": {},
    "queue_sizes": {},
    "orders_abiertas": 0,
    "bg_tasks_alive": 0,
    "stop_event_set": False,
    "intervalo": "5m",
    "last_heartbeat": None,
    "capital": {},
    "disponible_global": 0.0,
    "modo_operativo": "unknown",
    "reconnect_count": 0,
}

_signals: Deque[Dict[str, Any]] = deque(maxlen=200)
_orders: List[Dict[str, Any]] = []
_activity: Deque[Dict[str, Any]] = deque(maxlen=150)
_last_hb_activity_ts: float = 0.0
# Último estado de evaluación por símbolo (se actualiza en cada vela evaluada)
_symbol_eval: Dict[str, Dict[str, Any]] = {}


def update_from_heartbeat(record: logging.LogRecord) -> None:
    """Actualiza el estado desde un LogRecord del heartbeat del trader."""
    global _last_hb_activity_ts
    with _lock:
        _state["ws_connected"] = bool(getattr(record, "ws_connected", False) or False)
        _state["ws_failure_reason"] = getattr(record, "ws_failure_reason", None)
        raw_states = getattr(record, "consumer_states", {}) or {}
        _state["consumer_states"] = {str(k): str(v) for k, v in raw_states.items()}
        raw_queues = getattr(record, "queue_sizes", {}) or {}
        _state["queue_sizes"] = {str(k): int(v) for k, v in raw_queues.items()}
        _state["orders_abiertas"] = int(getattr(record, "orders_abiertas", 0) or 0)
        _state["bg_tasks_alive"] = int(getattr(record, "bg_tasks_alive", 0) or 0)
        _state["stop_event_set"] = bool(getattr(record, "stop_event_set", False) or False)
        iv = getattr(record, "intervalo", None)
        if iv:
            _state["intervalo"] = str(iv)
        now = time.time()
        _state["last_heartbeat"] = now
        # Entrada en log de actividad (throttled ~60 s)
        if now - _last_hb_activity_ts >= 55:
            _last_hb_activity_ts = now
            n_healthy = sum(1 for v in _state["consumer_states"].values() if v == "HEALTHY")
            n_total = len(_state["consumer_states"])
            orders_n = _state["orders_abiertas"]
            bg_n = _state["bg_tasks_alive"]
            _activity.appendleft({
                "ts": now,
                "msg": (
                    f"\U0001f493 Heartbeat: {n_healthy}/{n_total} HEALTHY, "
                    f"ordenes={orders_n}, BG={bg_n}"
                ),
                "level": "muted",
            })


def on_bot_event(evt: str, data: dict) -> None:
    """Captura eventos del bot para mostrar en el dashboard."""
    ts = time.time()
    activity_entry: Optional[Dict[str, Any]] = None

    if evt in ("entry_candidate", "entry_skip", "entry_gate_blocked", "entry_timeout",
               "entry_error"):
        result_map = {
            "entry_candidate": ("APROBADA", "success"),
            "entry_skip": ("FILTRADA", "warning"),
            "entry_gate_blocked": ("BLOQUEADA", "danger"),
            "entry_timeout": ("TIMEOUT", "muted"),
            "entry_error": ("ERROR", "danger"),
        }
        label, css = result_map.get(evt, ("?", "muted"))
        score = data.get("score")
        symbol = str(data.get("symbol", "?"))
        side = str(data.get("side") or data.get("direccion") or "?")
        reason = str(data.get("reason", ""))
        entry = {
            "ts": ts,
            "event": evt,
            "symbol": symbol,
            "side": side,
            "score": round(float(score), 3) if score is not None else None,
            "threshold": data.get("umbral") or data.get("threshold"),
            "reason": reason,
            "result_label": label,
            "result_css": css,
        }
        with _lock:
            _signals.appendleft(entry)
        # Actualizar estado por símbolo
        sym_eval: Dict[str, Any] = {"ts": ts, "result": label, "result_css": css, "reason": reason}
        if evt == "entry_skip" and reason == "engine_denegado":
            sym_eval.update({
                "score_total": data.get("score_total"),
                "umbral": data.get("umbral"),
                "score_tecnico": data.get("score_tecnico"),
                "rsi": data.get("rsi"),
                "slope": data.get("slope"),
                "momentum": data.get("momentum"),
                "tendencia": data.get("tendencia"),
                "diversidad": data.get("diversidad"),
                "regimen_volatilidad": data.get("regimen_volatilidad"),
                "motivo_engine": data.get("motivo_engine"),
                "estrategias_activas": data.get("estrategias_activas") or {},
                "validaciones_fallidas": data.get("validaciones_fallidas") or [],
                "contradicciones": data.get("contradicciones", False),
            })
        elif evt == "entry_candidate":
            sym_eval.update({
                "score_total": entry.get("score"),
                "side": side,
            })
        with _lock:
            _symbol_eval[symbol] = sym_eval

        # Actividad
        emoji_map = {
            "entry_candidate": "✅",
            "entry_skip": "⚡",
            "entry_gate_blocked": "\U0001f512",
            "entry_timeout": "⏱",
            "entry_error": "❌",
        }
        score_str = f" score={entry['score']}" if entry["score"] is not None else ""
        side_str = f" {side}" if side and side != "?" else ""
        reason_str = f" ({reason.replace('_', ' ')})" if reason else ""
        activity_entry = {
            "ts": ts,
            "msg": f"{emoji_map.get(evt, '•')} {symbol}{side_str} {label}{score_str}{reason_str}",
            "level": css,
        }

    elif evt in ("ws_connected", "datafeed.ws.drop"):
        with _lock:
            if evt == "datafeed.ws.drop":
                _state["reconnect_count"] = _state.get("reconnect_count", 0) + 1
        if evt == "ws_connected":
            activity_entry = {"ts": ts, "msg": "\U0001f7e2 WebSocket conectado", "level": "success"}
        else:
            reason = str(data.get("reason", "")).replace("_", " ")
            reason_str = f" ({reason})" if reason else ""
            activity_entry = {"ts": ts, "msg": f"\U0001f534 WebSocket caido{reason_str}", "level": "danger"}

    elif evt == "order_opened":
        order_entry = {
            "symbol": str(data.get("symbol", "?")),
            "side": str(data.get("side") or data.get("direccion") or "?"),
            "price": data.get("precio_entrada") or data.get("price"),
            "qty": data.get("cantidad") or data.get("qty"),
            "sl": data.get("stop_loss"),
            "tp": data.get("take_profit"),
            "ts": ts,
        }
        with _lock:
            _orders.append(order_entry)
        symbol = order_entry["symbol"]
        side = order_entry["side"].upper()
        price = order_entry["price"]
        price_str = f" @ €{float(price):.4f}" if price else ""
        activity_entry = {
            "ts": ts,
            "msg": f"\U0001f4c8 {symbol} {side} ABIERTA{price_str}",
            "level": "success",
        }

    elif evt in ("order_closed", "exit_processed"):
        symbol = str(data.get("symbol", "?"))
        with _lock:
            _orders[:] = [o for o in _orders if o.get("symbol") != symbol]
        pnl = data.get("pnl") or data.get("resultado") or data.get("profit")
        pnl_str = f" PnL: €{float(pnl):.2f}" if pnl is not None else ""
        lvl = "success" if (pnl is not None and float(pnl) >= 0) else "danger"
        activity_entry = {
            "ts": ts,
            "msg": f"\U0001f4c9 {symbol} CERRADA{pnl_str}",
            "level": lvl,
        }

    if activity_entry is not None:
        with _lock:
            _activity.appendleft(activity_entry)


def _read_last_prices() -> Dict[str, Dict[str, Any]]:
    """Lee el último OHLCV por símbolo desde el BufferManager global."""
    try:
        from core.vela.buffers import get_buffer_manager  # noqa: PLC0415
        bm = get_buffer_manager()
        result: Dict[str, Dict[str, Any]] = {}
        for sym, tf_dict in (bm._estados or {}).items():
            for st in tf_dict.values():
                buf = getattr(st, "buffer", None)
                if not buf:
                    continue
                last = buf[-1]
                if not isinstance(last, dict):
                    continue
                close = last.get("close")
                if close is None:
                    continue
                result[sym] = {
                    "close": float(close),
                    "open": float(last.get("open") or 0),
                    "high": float(last.get("high") or 0),
                    "low": float(last.get("low") or 0),
                    "volume": float(last.get("volume") or 0),
                }
                break  # solo el primer timeframe
        return result
    except Exception:
        return {}


def _read_price_history(n: int = 40) -> Dict[str, list]:
    """Lee las últimas ``n`` velas (close) por símbolo desde el BufferManager.

    Alimenta la gráfica de precios del dashboard. Best-effort: cualquier fallo
    devuelve lo acumulado hasta el momento.
    """
    try:
        from core.vela.buffers import get_buffer_manager  # noqa: PLC0415
        bm = get_buffer_manager()
        result: Dict[str, list] = {}
        for sym, tf_dict in (bm._estados or {}).items():
            for st in tf_dict.values():
                buf = getattr(st, "buffer", None)
                if not buf:
                    continue
                closes = []
                for vela in list(buf)[-n:]:
                    if isinstance(vela, dict) and vela.get("close") is not None:
                        closes.append(round(float(vela["close"]), 6))
                if closes:
                    result[sym] = closes
                break  # solo el primer timeframe
        return result
    except Exception:
        return {}


def _read_capital() -> tuple[dict, float]:
    """Lee capital desde disco (solo si ha cambiado)."""
    try:
        path = Path("estado/capital.json")
        if path.exists():
            d = json.loads(path.read_text(encoding="utf-8"))
            return d.get("capital_por_simbolo", {}), float(d.get("disponible_global", 0.0))
    except Exception:
        pass
    return {}, 0.0


_bot_ref = None


def set_bot_ref(bot: object) -> None:
    """Guarda una referencia débil al bot para leer estado en tiempo real."""
    global _bot_ref
    _bot_ref = bot


def _read_live_ws_state() -> tuple[bool, dict, dict]:
    """Lee ws_connected, consumer_states y queue_sizes directo del feed si está disponible."""
    bot = _bot_ref
    if bot is None:
        return False, {}, {}
    try:
        feed = getattr(bot, "feed", None)
        if feed is None:
            return False, {}, {}
        ws_connected = bool(getattr(getattr(feed, "ws_connected_event", None), "is_set", lambda: False)())
        consumer_states = {}
        states = getattr(feed, "_consumer_state", {}) or {}
        for sym, st in states.items():
            consumer_states[str(sym)] = getattr(st, "name", str(st))
        queue_sizes = {}
        for sym, q in (getattr(feed, "_queues", {}) or {}).items():
            try:
                queue_sizes[str(sym)] = int(q.qsize())
            except Exception:
                pass
        return ws_connected, consumer_states, queue_sizes
    except Exception:
        return False, {}, {}


def get_snapshot() -> dict:
    """Devuelve un snapshot JSON-serializable del estado actual."""
    capital, disponible = _read_capital()
    ws_live, cs_live, qs_live = _read_live_ws_state()
    prices = _read_last_prices()
    price_history = _read_price_history()
    with _lock:
        uptime = int(time.time() - _state["uptime_start"])
        ws = ws_live if _bot_ref is not None else _state["ws_connected"]
        cs = cs_live if cs_live else _state["consumer_states"]
        qs = qs_live if qs_live else _state["queue_sizes"]
        symbol_eval = dict(_symbol_eval)
        return {
            **_state,
            "ws_connected": ws,
            "consumer_states": cs,
            "queue_sizes": qs,
            "uptime_seconds": uptime,
            "capital": capital,
            "disponible_global": disponible,
            "signals": list(_signals),
            "orders": list(_orders),
            "activity": list(_activity),
            "symbol_eval": symbol_eval,
            "prices": prices,
            "price_history": price_history,
        }
