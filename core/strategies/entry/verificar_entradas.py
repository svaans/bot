"""
Pipeline de verificación de entradas.

Contrato esperado por Trader.evaluar_condiciones_de_entrada(...)
----------------------------------------------------------------
async def verificar_entrada(trader, symbol, df, estado, on_event=None) -> dict | None

Devuelve un diccionario listo para consumo por el módulo de órdenes, o None si
no se validan condiciones. No abre órdenes aquí: solo evalúa.

Notas de diseño
---------------
- Tolerante a dependencias opcionales (engine, persistencia, riesgo).
- Respeta umbrales de Config (score técnico, distancias mínimas, overrides).
- Emite eventos vía `on_event(evt, data)` si se provee.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import asyncio
from dataclasses import dataclass, field
import time
from typing import Any, Callable, Optional

import pandas as pd

# Importar utilidades (no críticas si no están)
try:
    from core.utils.utils import validar_dataframe
except Exception:  # pragma: no cover
    def validar_dataframe(df: pd.DataFrame, columnas: list[str]) -> bool:  # fallback mínimo
        return df is not None and not df.empty and all(col in df.columns for col in columnas)

# Logging estructurado
from core.utils.logger import configurar_logger
from core.utils.log_utils import safe_extra
from core.metrics_helpers import safe_inc, safe_set
from core.utils.metrics_compat import Counter, Gauge
from core.utils.feature_flags import is_flag_enabled
from core.funding_rate import FundingResult, obtener_funding

try:  # pragma: no cover - métricas opcionales
    from core.metrics import registrar_funding_signal_decoration
except Exception:  # pragma: no cover - entorno degradado
    def registrar_funding_signal_decoration(*_args: Any, **_kwargs: Any) -> None:  # type: ignore[override]
        return None

try:  # pragma: no cover - fallback cuando las métricas no están disponibles
    from core.metrics import registrar_decision
except Exception:  # pragma: no cover
    def registrar_decision(*_args: Any, **_kwargs: Any) -> None:
        return None

# Import corregido: esta función vive en persistencia_tecnica
try:
    from core.persistencia_tecnica import coincidencia_parcial
except Exception:  # pragma: no cover
    def coincidencia_parcial(*_args, **_kwargs) -> float:
        return 0.0


ColumnsBase = ["timestamp", "open", "high", "low", "close", "volume"]


log = configurar_logger("entry_verifier", modo_silencioso=True)


def _metrics_extended_enabled() -> bool:
    return is_flag_enabled("metrics.extended.enabled")


class EntryDecision:
    """Decisión de entrada lista para consumo por el módulo de órdenes."""

    __slots__ = (
        "symbol",
        "timeframe",
        "permitida",
        "score",
        "threshold",
        "usar_score",
        "reasons",
        "meta",
        "raw",
        "score_missing",
    )

    def __init__(
        self,
        symbol: str,
        timeframe: str | None,
        permitida: bool,
        score: float,
        threshold: float,
        usar_score: bool,
        reasons: Optional[list[str]] = None,
        meta: Optional[dict[str, Any]] = None,
        raw: Optional[dict[str, Any]] = None,
        score_missing: bool = False,
    ) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.permitida = permitida
        self.score = score
        self.threshold = threshold
        self.usar_score = usar_score
        self.reasons = list(reasons) if reasons else []
        self.meta = dict(meta) if meta else {}
        self.raw = dict(raw) if raw else {}
        self.score_missing = score_missing

    def add_reason(self, reason: str) -> None:
        if reason and reason not in self.reasons:
            self.reasons.append(reason)

    def to_payload(self) -> dict[str, Any]:
        payload = dict(self.raw)
        payload["score"] = self.score
        existing_meta = dict(payload.get("meta") or {})
        existing_meta.update(self.meta)
        payload["meta"] = existing_meta
        return payload


DECISION_TOTAL = Counter(
    "decision_total",
    "Decisiones finales de entrada por símbolo y timeframe",
    ["symbol", "timeframe", "outcome"],
)

GATE_SCORE_DECISIONS_TOTAL = Counter(
    "gate_score_decisions_total",
    "Resultados del gate de score técnico",
    ["symbol", "timeframe", "pass"],
)

GATE_PERSISTENCIA_DECISIONS_TOTAL = Counter(
    "gate_persistencia_decisions_total",
    "Resultados del gate de persistencia",
    ["symbol", "timeframe", "pass"],
)

GATE_SCORE_LAST_VALUE = Gauge(
    "gate_score_last_value",
    "Último score observado en el gate de score",
    ["symbol", "timeframe"],
)

ENTRY_TIMEOUT_TOTAL = Counter(
    "entry_timeout_total",
    "Entradas descartadas por timeout al evaluar condiciones",
    ["symbol", "timeframe"],
)


def _emit(on_event: Optional[Callable[[str, dict], None]], evt: str, data: dict) -> None:
    if callable(on_event):
        try:
            on_event(evt, data)
        except Exception:
            pass


def _timeout_para_symbol(trader: Any, symbol: str) -> int:
    cfg = getattr(trader, "config", None)
    if cfg is None:
        return 15
    por_symbol = getattr(cfg, "timeout_evaluar_condiciones_por_symbol", {}) or {}
    if isinstance(por_symbol, dict) and symbol in por_symbol:
        try:
            return int(por_symbol[symbol])
        except Exception:
            pass
    return int(getattr(cfg, "timeout_evaluar_condiciones", 15))


def _min_dist_pct(trader: Any, symbol: str) -> float:
    cfg = getattr(trader, "config", None)
    base = 0.0005
    if cfg is None:
        return base
    base = float(getattr(cfg, "min_dist_pct", base) or base)
    overrides = getattr(cfg, "min_dist_pct_overrides", {}) or {}
    try:
        return float(overrides.get(symbol, base))
    except Exception:
        return base


def _build_niveles(precio: float, min_pct: float) -> tuple[float, float]:
    """
    Construye niveles SL/TP simples alrededor de `precio` respetando distancia mínima.
    Política: SL a -min_pct y TP a +2*min_pct (RR ~2:1).
    """
    sl = precio * (1 - min_pct)
    tp = precio * (1 + 2 * min_pct)
    return (sl, tp)


def _sanear_df(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura tipos básicos y ordena por timestamp ascendente."""
    use = df.copy()
    # Coerción mínima
    for c in ColumnsBase:
        if c not in use.columns:
            continue
        if c == "timestamp":
            use[c] = pd.to_numeric(use[c], errors="coerce").astype("Int64")
        else:
            use[c] = pd.to_numeric(use[c], errors="coerce")
    use = use.dropna(subset=["timestamp", "close"]).copy()
    if "timestamp" in use:
        use = use.sort_values("timestamp", kind="mergesort")
    return use


def _metric_labels(symbol: str, timeframe: str | None) -> dict[str, str]:
    return {"symbol": str(symbol).upper(), "timeframe": str(timeframe or "unknown")}


def _resolve_override(overrides: Any, symbol: str, timeframe: str | None) -> Any:
    if not isinstance(overrides, dict):
        return None
    symbol_key = str(symbol).upper()
    tf_key = str(timeframe or "").lower()
    candidates: list[str] = []
    if tf_key:
        candidates.extend(
            [
                f"{symbol_key}:{tf_key}",
                f"{symbol_key}@{tf_key}",
                f"{symbol_key}/{tf_key}",
            ]
        )
    candidates.append(symbol_key)
    for key in candidates:
        if key in overrides:
            return overrides[key]
    return None


def _resolve_timeframe(df: pd.DataFrame, trader: Any) -> str | None:
    timeframe = getattr(df, "tf", None)
    if timeframe:
        return str(timeframe)
    attrs = getattr(df, "attrs", None)
    if isinstance(attrs, dict):
        tf_attr = attrs.get("tf")
        if tf_attr:
            return str(tf_attr)
    cfg = getattr(trader, "config", None)
    if cfg is not None:
        tf_cfg = getattr(cfg, "intervalo_velas", None)
    if tf_cfg:
        return str(tf_cfg)
    return None


def _normalize_score(value: Any, default: float = -1.0) -> tuple[float, bool]:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return float(default), True
    if not math.isfinite(score):
        return float(default), True
    return score, False


def _inc_score_metric(symbol: str, timeframe: str | None, pass_value: str) -> None:
    labels = (str(symbol).upper(), str(timeframe or "unknown"), pass_value)
    try:
        GATE_SCORE_DECISIONS_TOTAL.labels(*labels).inc()
    except Exception:
        safe_inc(GATE_SCORE_DECISIONS_TOTAL)


def _inc_persist_metric(symbol: str, timeframe: str | None, pass_value: str) -> None:
    labels = (str(symbol).upper(), str(timeframe or "unknown"), pass_value)
    try:
        GATE_PERSISTENCIA_DECISIONS_TOTAL.labels(*labels).inc()
    except Exception:
        safe_inc(GATE_PERSISTENCIA_DECISIONS_TOTAL)


def _inc_timeout_metric(symbol: str, timeframe: str | None) -> None:
    safe_inc(ENTRY_TIMEOUT_TOTAL, **_metric_labels(symbol, timeframe))



def _resolve_score_threshold(cfg: Any, symbol: str, timeframe: str | None) -> float:
    base = 0.0
    if cfg is not None:
        base = float(getattr(cfg, "umbral_score_tecnico", base) or 0.0)
        override = _resolve_override(getattr(cfg, "umbral_score_overrides", {}), symbol, timeframe)
        if override is not None:
            try:
                return float(override)
            except (TypeError, ValueError):
                pass
    return float(base)


def _resolve_usar_score(cfg: Any, symbol: str, timeframe: str | None) -> bool:
    default = True
    if cfg is not None:
        default = bool(getattr(cfg, "usar_score_tecnico", default))
        override = _resolve_override(getattr(cfg, "usar_score_overrides", {}), symbol, timeframe)
        if override is not None:
            return bool(override)
    return bool(default)


def _resolve_persistencia_strict(cfg: Any, symbol: str, timeframe: str | None) -> bool:
    default = False
    if cfg is not None:
        default = bool(getattr(cfg, "persistencia_strict", default))
        override = _resolve_override(getattr(cfg, "persistencia_strict_overrides", {}), symbol, timeframe)
        if override is not None:
            return bool(override)
    return bool(default)


def _apply_score_gate(
    decision: EntryDecision,
    cfg: Any,
    *,
    symbol: str,
    timeframe: str | None,
) -> tuple[bool, dict[str, Any]]:
    threshold = _resolve_score_threshold(cfg, symbol, timeframe)
    usar_score = _resolve_usar_score(cfg, symbol, timeframe)
    decision.threshold = threshold
    decision.usar_score = usar_score

    gauge_labels = _metric_labels(symbol, timeframe)
    safe_set(GATE_SCORE_LAST_VALUE, decision.score, **gauge_labels)

    tf_label = str(timeframe or "unknown")
    log_extra = {
        "symbol": symbol,
        "timeframe": tf_label,
        "score": decision.score,
        "umbral": threshold,
        "usar_score": usar_score,
    }

    if not usar_score:
        _inc_score_metric(symbol, timeframe, "true")
        return True, log_extra

    if decision.score_missing:
        safe_set(GATE_SCORE_LAST_VALUE, -1.0, **gauge_labels)
        _inc_score_metric(symbol, timeframe, "false")
        log.warning("gate.score_missing", extra=safe_extra({**log_extra, "score": -1.0}))
        decision.meta.setdefault("score_missing", True)
        decision.score = -1.0
        return False, log_extra

    if not math.isfinite(decision.score):
        safe_set(GATE_SCORE_LAST_VALUE, -1.0, **gauge_labels)
        _inc_score_metric(symbol, timeframe, "false")
        log.warning("gate.score_invalid", extra=safe_extra({**log_extra, "score": -1.0}))
        decision.meta.setdefault("score_missing", True)
        decision.score = -1.0
        return False, log_extra

    if decision.score < threshold:
        _inc_score_metric(symbol, timeframe, "false")
        return False, log_extra

    _inc_score_metric(symbol, timeframe, "true")
    return True, log_extra


def _apply_persistencia_gate(
    trader: Any,
    decision: EntryDecision,
    *,
    symbol: str,
    timeframe: str | None,
    initial_ok: bool,
) -> tuple[bool, bool, bool]:
    cfg = getattr(trader, "config", None)
    strict = _resolve_persistencia_strict(cfg, symbol, timeframe)
    persist_ok = bool(initial_ok)
    tf_label = str(timeframe or "unknown")
    base_extra = {"symbol": symbol, "timeframe": tf_label}

    repo = getattr(trader, "repo", None)
    save_fn = getattr(repo, "save_evaluacion", None)
    if callable(save_fn):
        snapshot = decision.to_payload()
        try:
            save_fn(symbol=symbol, timeframe=timeframe, decision=snapshot)
        except Exception as exc:
            persist_ok = False
            log.warning(
                "persistencia.error",
                extra=safe_extra(
                    {
                        **base_extra,
                        "exc_type": type(exc).__name__,
                        "exc_msg": str(exc),
                    }
                ),
            )

    log.info(
        "persistencia.check",
        extra=safe_extra({**base_extra, "persistencia_ok": persist_ok}),
    )

    allow = persist_ok or not strict
    _inc_persist_metric(symbol, timeframe, "true" if allow else "false")

    if not persist_ok and not strict:
        log.warning(
            "persistencia.degraded",
            extra=safe_extra({**base_extra, "persistencia_ok": persist_ok, "strict": False}),
        )

    return allow, persist_ok, strict


def _finalize_decision(decision: EntryDecision) -> None:
    tf_label = str(decision.timeframe or "unknown")
    outcome = "permitida" if decision.permitida else "rechazada"
    reasons = decision.reasons or ["ok"]
    payload = {
        "symbol": decision.symbol,
        "timeframe": tf_label,
        "permitida": bool(decision.permitida),
        "score": float(decision.score),
        "umbral": float(decision.threshold),
        "usar_score": bool(decision.usar_score),
        "razones": reasons,
        "meta": decision.meta,
    }
    safe_inc(DECISION_TOTAL, symbol=decision.symbol, timeframe=tf_label, outcome=outcome)
    if _metrics_extended_enabled():
        action_label = "entry_permitida" if decision.permitida else "entry_rechazada"
        registrar_decision(decision.symbol, action_label)
    log.debug("decision.final", extra=safe_extra(payload))


async def _evaluar_engine(trader: Any, symbol: str, df: pd.DataFrame, estado: Any, on_event=None) -> Optional[dict]:
    """
    Llama al motor de estrategias si está disponible.
    Debe devolver un dict con al menos: {'side': 'long'|'short', 'score': float, ...}
    """
    engine = getattr(trader, "engine", None)
    if engine is None:
        _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "engine_missing"})
        return None
    candidatos = (
        "evaluar_entrada",
        "verificar_entrada",
        "evaluar_condiciones_de_entrada",
    )

    fn = None
    for attr in candidatos:
        maybe = getattr(engine, attr, None)
        if callable(maybe):
            fn = maybe
            break

    if fn is None:
        _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "engine_no_fn"})
        return None
    
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        signature = None

    def _call(call_args: tuple[Any, ...], call_kwargs: dict[str, Any]) -> Any:
        kwargs = dict(call_kwargs)
        if signature is not None:
            if "on_event" in signature.parameters and on_event is not None:
                kwargs.setdefault("on_event", on_event)
            return fn(*call_args, **kwargs)

        if on_event is not None and "on_event" not in kwargs:
            try:
                return fn(*call_args, on_event=on_event, **kwargs)
            except TypeError:
                pass
        return fn(*call_args, **kwargs)

    necesita_trader = False
    if signature is not None:
        for param in signature.parameters.values():
            if (
                param.kind
                in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
                and param.name == "trader"
            ):
                necesita_trader = True
                break

    def _attempt(*args: Any, **kwargs: Any) -> Callable[[], Any]:
        call_args = tuple(args)
        call_kwargs = dict(kwargs)

        def _runner() -> Any:
            return _call(call_args, call_kwargs)

        return _runner
        
    intentos: list[Callable[[], Any]] = []
    if necesita_trader:
        intentos.append(_attempt(trader, symbol, df, estado))
        intentos.append(_attempt(trader, symbol, df, estado=estado))
    intentos.append(_attempt(symbol, df, estado))
    intentos.append(_attempt(symbol, df, estado=estado))
    if necesita_trader:
        intentos.append(_attempt(trader, symbol, df))
    intentos.append(_attempt(symbol, df))

    for idx, intento in enumerate(intentos):
        try:
            resultado = intento()
        except TypeError:
            if signature is None and idx == 0:
                # Si la firma era inaccesible y falta el trader, probamos una vez
                # con el trader como primer argumento.
                try:
                    resultado = fn(trader, symbol, df)  # type: ignore[misc]
                except TypeError:
                    continue
            else:
                continue


        try:
            if inspect.isawaitable(resultado):
                return await resultado
            return resultado
        except asyncio.CancelledError:
            raise
        except Exception:
            _emit(on_event, "entry_error", {"symbol": symbol, "reason": "engine_exception"})
            return None

    _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "engine_signature"})
    return None


def _aplicar_persistencia(trader: Any, symbol: str, resultado: dict, on_event=None) -> dict:
    """
    Aplica persistencia técnica si existe. No asume firma exacta; intenta
    `es_persistente` y registra evento informativo.
    """
    pers = getattr(trader, "persistencia", None)
    if pers is None:
        return resultado
    try:
        # Usamos etiqueta basada en la dirección para el contador (long/short)
        etiqueta = str(resultado.get("side", "na")).lower()
        es_ok = False
        if hasattr(pers, "es_persistente"):
            es_ok = bool(pers.es_persistente(symbol, etiqueta))  # type: ignore
        if not es_ok and hasattr(pers, "actualizar"):
            # incrementa contador y reevalúa
            pers.actualizar(symbol, etiqueta)  # type: ignore
            if hasattr(pers, "es_persistente"):
                es_ok = bool(pers.es_persistente(symbol, etiqueta))  # type: ignore
        resultado["persistencia_ok"] = es_ok
        _emit(on_event, "entry_persistencia", {"symbol": symbol, "ok": es_ok, "etiqueta": etiqueta})
    except Exception:
        resultado["persistencia_ok"] = False
    return resultado


def _validar_distancias(precio: float, sl: float, tp: float, min_pct: float) -> bool:
    if precio <= 0:
        return False
    delta_min = precio * min_pct
    # Evita falsos negativos por errores de redondeo en coma flotante
    # cuando la distancia calculada es prácticamente igual al umbral.
    tolerancia = max(delta_min * 1e-9, 1e-12)

    distancia_sl = abs(precio - sl)
    distancia_tp = abs(tp - precio)

    return (distancia_sl + tolerancia >= delta_min) and (distancia_tp + tolerancia >= delta_min)

async def verificar_entrada(
    trader: Any,
    symbol: str,
    df: pd.DataFrame,
    estado: Any,
    *,
    on_event: Optional[Callable[[str, dict], None]] = None,
) -> Optional[dict]:
    """
    Evalúa condiciones de entrada para `symbol` sobre `df`.

    Retorna un dict con campos:
      - symbol, side ('long'|'short'), precio_entrada, stop_loss, take_profit
      - score (float), persistencia_ok (bool), timestamp (última vela)
      - meta: dict con detalles internos (puedes extenderlo)
    o None si no procede.
    """
    symbol_norm = str(symbol or "").upper()
    ts_value: Any = None
    log.debug(
        "verificar_entrada.enter",
        extra={
            "symbol": symbol_norm,
            "timestamp": ts_value,
            "stage": "verificar_entrada",
        },
    )

    def _reject(reason: str, *, extra: Optional[dict] = None) -> Optional[dict]:
        payload = {
            "symbol": symbol_norm,
            "timestamp": ts_value,
            "stage": "verificar_entrada",
            "decision": "rechazada",
            "reason": reason,
        }
        if extra:
            payload.update(extra)
        log.debug("verificar_entrada.exit", extra=payload)
        return None
    
    def _reject_with_skip(
        reason: str, *, extra: Optional[dict] = None
    ) -> Optional[dict]:
        payload = {"symbol": symbol, "reason": reason}
        if extra:
            payload.update(extra)
        _emit(on_event, "entry_skip", payload)
        return _reject(reason, extra=payload)

    def _approve(resultado: dict) -> dict:
        payload = {
            "symbol": symbol_norm,
            "timestamp": ts_value,
            "stage": "verificar_entrada",
            "decision": "permitida",
            "reason": "ok",
            "side": resultado.get("side"),
            "score": resultado.get("score"),
        }
        log.debug("verificar_entrada.exit", extra=payload)
        return resultado
    # Chequeos básicos
    if not isinstance(df, pd.DataFrame) or df.empty:
        _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "empty_df"})
        return _reject("empty_df")
    if not validar_dataframe(df, ColumnsBase):
        _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "invalid_columns"})
        return _reject("invalid_columns")
        
    timeframe_value = _resolve_timeframe(df, trader)
    df = _sanear_df(df)
    if df.empty:
        _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "sanitized_empty"})
        return _reject("sanitized_empty")

    # Última vela
    last = df.iloc[-1]
    precio = float(last["close"])
    try:
        ts_value = int(last["timestamp"])
    except (TypeError, ValueError):
        ts_candidate = last.get("timestamp")
        ts_value = None if pd.isna(ts_candidate) else ts_candidate

    # Respeta la puerta de entrada del Trader (capital, riesgo, cooldown, etc.)
    gate = getattr(trader, "_puede_evaluar_entradas", None)
    if callable(gate) and not gate(symbol):
        payload = {"symbol": symbol, "reason": "gate_blocked"}
        _emit(on_event, "entry_gate_blocked", payload)
        return _reject("gate_blocked", extra=payload)

    # Timeout configurable por símbolo
    timeout = _timeout_para_symbol(trader, symbol)

    # Evaluación del motor dentro del timeout
    try:
        resultado_engine = await asyncio.wait_for(
            _evaluar_engine(trader, symbol, df, estado, on_event=on_event),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        payload = {
            "symbol": symbol,
            "timeout": timeout,
            "reason": "timeout",
            "timeframe": timeframe_value,
        }
        _inc_timeout_metric(symbol, timeframe_value)
        log.warning("verificar_entrada.timeout", extra=safe_extra(payload))
        _emit(on_event, "entry_timeout", payload)
        return _reject("timeout", extra=payload)

    if not resultado_engine:
        return _reject("engine_no_result")

    # Normalización mínima del resultado del engine
    side = str(resultado_engine.get("side", "long")).lower()
    if side not in ("long", "short"):
        side = "long"

    cfg = getattr(trader, "config", None)
    if cfg is None:
        return _reject_with_skip("config_missing")
    contradicciones = bool(resultado_engine.get("contradicciones", False))

    # Reglas de contradicción
    bloquea_contra = bool(getattr(cfg, "contradicciones_bloquean_entrada", True))
    if bloquea_contra and contradicciones:
        extra = {"contradicciones": True}
        return _reject_with_skip("contradicciones", extra=extra)


    # Persistencia técnica (si está activada/instanciada)
    resultado_engine = _aplicar_persistencia(trader, symbol, resultado_engine, on_event=on_event)
    persistencia_ok_inicial = bool(resultado_engine.get("persistencia_ok", True))

    # Distancias mínimas SL/TP
    min_pct = _min_dist_pct(trader, symbol)
    sl, tp = _build_niveles(precio, min_pct)
    if not _validar_distancias(precio, sl, tp, min_pct):
        extra = {"min_dist_pct": min_pct}
        return _reject_with_skip("distancias", extra=extra)

    # Enriquecimiento opcional con coincidencia parcial (histórico↔pesos)
    try:
        # Si el engine provee estructuras para esta comparación, úsalo
        historial = resultado_engine.get("historial")  # p. ej. dict de señales recientes
        pesos = resultado_engine.get("pesos")          # p. ej. ponderaciones por indicador
        match = coincidencia_parcial(historial or {}, pesos or {})
    except Exception:
        match = 0.0

    score_value, score_missing = _normalize_score(resultado_engine.get("score"))
    propuesta = {
        "symbol": symbol,
        "side": side,
        "precio_entrada": precio,
        "stop_loss": sl,
        "take_profit": tp,
        "score": score_value,
        "timestamp": ts_value,
        "persistencia_ok": persistencia_ok_inicial,
        "meta": {
            "min_dist_pct": min_pct,
            "contradicciones": contradicciones,
            "match_parcial": match,
        },
    }

    decision = EntryDecision(
        symbol=symbol_norm,
        timeframe=timeframe_value,
        permitida=True,
        score=score_value,
        threshold=0.0,
        usar_score=True,
        meta=dict(propuesta["meta"]),
        raw=dict(propuesta),
        score_missing=score_missing,
    )

    await _apply_signal_decorators(trader, decision, symbol=symbol_norm, side=side)
    propuesta["meta"].update(decision.meta)
    propuesta["score"] = decision.score
    decision.raw["score"] = decision.score
    if isinstance(decision.raw.get("meta"), dict):
        decision.raw["meta"].update(decision.meta)
    else:
        decision.raw["meta"] = dict(decision.meta)

    score_pass, _ = _apply_score_gate(
        decision,
        cfg,
        symbol=symbol_norm,
        timeframe=timeframe_value,
    )
    decision.meta.setdefault("usar_score", decision.usar_score)
    decision.meta.setdefault("umbral_score", decision.threshold)
    if not score_pass:
        decision.permitida = False
        decision.add_reason("score_missing" if decision.meta.get("score_missing") else "score_bajo")
        payload = {
            "symbol": symbol,
            "reason": "score_bajo",
            "score": decision.score,
            "umbral": decision.threshold,
            "usar_score": decision.usar_score,
        }
        _emit(on_event, "entry_skip", payload)
        _finalize_decision(decision)
        return _reject("score_bajo", extra=payload)

    allow_persist, persist_ok, strict_flag = _apply_persistencia_gate(
        trader,
        decision,
        symbol=symbol_norm,
        timeframe=timeframe_value,
        initial_ok=persistencia_ok_inicial,
    )
    decision.meta["persistencia_ok"] = persist_ok
    decision.raw["persistencia_ok"] = persist_ok
    if not allow_persist:
        decision.permitida = False
        decision.add_reason("persistencia")
        payload = {
            "symbol": symbol,
            "reason": "persistencia",
            "persistencia_ok": persist_ok,
            "strict": strict_flag,
        }
        _emit(on_event, "entry_skip", payload)
        _finalize_decision(decision)
        return _reject("persistencia", extra=payload)
    if not persist_ok:
        decision.meta.setdefault("persistencia_warning", True)
        decision.add_reason("persistencia_warning")

    _emit(on_event, "entry_candidate", {"symbol": symbol, "side": side, "score": decision.score})
    decision.add_reason("ok")
    final_payload = decision.to_payload()
    _finalize_decision(decision)
    return _approve(final_payload)



_FUNDING_CACHE_ATTR = "_funding_cache_store"


def _funding_feature_enabled(trader: Any) -> bool:
    cfg = getattr(trader, "config", None)
    if cfg is not None and bool(getattr(cfg, "funding_enabled", False)):
        return True
    return is_flag_enabled("funding.enabled")


def _funding_cache(trader: Any) -> dict[str, tuple[float, FundingResult]]:
    cache = getattr(trader, _FUNDING_CACHE_ATTR, None)
    if cache is None:
        cache = {}
        setattr(trader, _FUNDING_CACHE_ATTR, cache)
    return cache


def _funding_cache_ttl(trader: Any) -> float:
    cfg = getattr(trader, "config", None)
    raw = getattr(cfg, "funding_cache_ttl", 300) if cfg is not None else 300
    try:
        ttl = float(raw)
    except (TypeError, ValueError):
        return 300.0
    return max(0.0, ttl)


async def _obtener_funding_cached(trader: Any, symbol: str) -> FundingResult:
    cache = _funding_cache(trader)
    ttl = _funding_cache_ttl(trader)
    now = time.monotonic()
    entry = cache.get(symbol)
    if entry is not None:
        expires_at, cached_result = entry
        if expires_at >= now:
            return cached_result
    result = await obtener_funding(symbol)
    if ttl > 0:
        cache[symbol] = (now + ttl, result)
    else:
        cache.pop(symbol, None)
    return result


def _resolve_funding_symbol(cfg: Any, symbol: str) -> str:
    overrides = getattr(cfg, "funding_symbol_overrides", {}) if cfg is not None else {}
    resolved = None
    if isinstance(overrides, dict):
        resolved = overrides.get(str(symbol).upper())
    if resolved:
        return str(resolved)
    return str(symbol).upper()


async def _apply_signal_decorators(
    trader: Any,
    decision: EntryDecision,
    *,
    symbol: str,
    side: str,
) -> None:
    if not _funding_feature_enabled(trader):
        return
    try:
        await _apply_funding_decorator(trader, decision, symbol=symbol, side=side)
    except asyncio.CancelledError:  # pragma: no cover - respeta cancelaciones
        raise
    except Exception:
        log.exception(
            "funding.decorator_error",
            extra=safe_extra({"symbol": symbol, "side": side}),
        )


async def _apply_funding_decorator(
    trader: Any,
    decision: EntryDecision,
    *,
    symbol: str,
    side: str,
) -> None:
    cfg = getattr(trader, "config", None)
    query_symbol = _resolve_funding_symbol(cfg, symbol)
    if not query_symbol:
        registrar_funding_signal_decoration(symbol, side, "missing_symbol")
        return

    result = await _obtener_funding_cached(trader, query_symbol)
    meta_funding: dict[str, Any] = {
        "symbol": symbol,
        "query_symbol": query_symbol,
        "mapped_symbol": result.mapped_symbol,
        "segment": result.segment,
        "source": result.source,
        "available": result.available,
        "reason": result.reason,
        "fetched_at": result.fetched_at.isoformat(),
    }
    decision.meta.setdefault("funding", {}).update(meta_funding)

    if not result.available or result.rate is None:
        registrar_funding_signal_decoration(symbol, side, "missing")
        log.info(
            "funding.missing",
            extra=safe_extra(
                {
                    "symbol": symbol,
                    "query_symbol": query_symbol,
                    "reason": result.reason or "unknown",
                }
            ),
        )
        return

    rate = float(result.rate)
    decision.meta["funding"]["rate"] = rate

    warning_threshold = getattr(cfg, "funding_warning_threshold", 0.0005) if cfg is not None else 0.0005
    try:
        warning_threshold = abs(float(warning_threshold))
    except (TypeError, ValueError):
        warning_threshold = 0.0005

    direction_cost = (side.lower() == "long" and rate > 0) or (side.lower() == "short" and rate < 0)
    direction_label = "pay" if direction_cost else "receive"
    decision.meta["funding"]["direction"] = direction_label

    magnitude = abs(rate)
    outcome = "info"
    if warning_threshold > 0 and magnitude >= warning_threshold:
        decision.meta["funding"]["warning"] = True
        log.info(
            "funding.warning",
            extra=safe_extra(
                {
                    "symbol": symbol,
                    "side": side,
                    "rate": rate,
                    "direction": direction_label,
                }
            ),
        )
        outcome = "warning"
    else:
        decision.meta["funding"].setdefault("warning", False)

    penalty_enabled = bool(getattr(cfg, "funding_score_penalty_enabled", False)) if cfg is not None else False
    penalty_value = getattr(cfg, "funding_score_penalty", 0.0) if cfg is not None else 0.0
    try:
        penalty_value = float(penalty_value)
    except (TypeError, ValueError):
        penalty_value = 0.0

    bonus_value = getattr(cfg, "funding_score_bonus", 0.0) if cfg is not None else 0.0
    try:
        bonus_value = float(bonus_value)
    except (TypeError, ValueError):
        bonus_value = 0.0

    adjustment_meta: dict[str, Any] = {}
    adjusted = False
    if direction_cost and penalty_enabled and penalty_value > 0:
        decision.score -= penalty_value
        adjustment_meta["penalty"] = penalty_value
        adjusted = True
        outcome = "penalized"
    elif not direction_cost and bonus_value > 0:
        decision.score += bonus_value
        adjustment_meta["bonus"] = bonus_value
        adjusted = True
        outcome = "boosted"

    if adjusted:
        decision.meta.setdefault("funding_adjustment", {}).update(adjustment_meta)

    registrar_funding_signal_decoration(symbol, side, outcome)



