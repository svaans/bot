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

# Import corregido: esta función vive en persistencia_tecnica
try:
    from core.persistencia_tecnica import coincidencia_parcial
except Exception:  # pragma: no cover
    def coincidencia_parcial(*_args, **_kwargs) -> float:
        return 0.0


ColumnsBase = ["timestamp", "open", "high", "low", "close", "volume"]


log = configurar_logger("entry_verifier", modo_silencioso=True)


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
        
    intentos: list[Callable[[], Any]] = []

    if on_event is not None:
        intentos.append(lambda: fn(trader, symbol, df, estado, on_event=on_event))
    intentos.append(lambda: fn(trader, symbol, df, estado))
    if on_event is not None:
        intentos.append(lambda: fn(symbol, df, estado, on_event=on_event))
    intentos.append(lambda: fn(symbol, df, estado))
    if on_event is not None:
        intentos.append(lambda: fn(trader, symbol, df, on_event=on_event))
        intentos.append(lambda: fn(symbol, df, on_event=on_event))
    intentos.append(lambda: fn(trader, symbol, df))
    intentos.append(lambda: fn(symbol, df))

    for intento in intentos:
        try:
            resultado = intento()
        except TypeError:
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


def _validar_score(cfg: Any, resultado: dict) -> bool:
    usar_score = bool(getattr(cfg, "usar_score_tecnico", True))
    if not usar_score:
        return True
    umbral = float(getattr(cfg, "umbral_score_tecnico", 2.0))
    score = float(resultado.get("score", float("nan")))
    return not math.isnan(score) and score >= umbral


def _validar_distancias(precio: float, sl: float, tp: float, min_pct: float) -> bool:
    if precio <= 0:
        return False
    return (abs(precio - sl) >= precio * min_pct) and (abs(tp - precio) >= precio * min_pct)


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
        _emit(on_event, "entry_gate_blocked", {"symbol": symbol})
        return _reject("gate_blocked")

    # Timeout configurable por símbolo
    timeout = _timeout_para_symbol(trader, symbol)

    # Evaluación del motor dentro del timeout
    try:
        resultado_engine = await asyncio.wait_for(
            _evaluar_engine(trader, symbol, df, estado, on_event=on_event),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        _emit(on_event, "entry_timeout", {"symbol": symbol, "timeout": timeout})
        return _reject("timeout", extra={"timeout": timeout})

    if not resultado_engine:
        return _reject("engine_no_result")

    # Normalización mínima del resultado del engine
    side = str(resultado_engine.get("side", "long")).lower()
    if side not in ("long", "short"):
        side = "long"
    score = float(resultado_engine.get("score", float("nan")))
    contradicciones = bool(resultado_engine.get("contradicciones", False))

    cfg = getattr(trader, "config", None)
    if cfg is None:
        _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "config_missing"})
        return _reject("config_missing")

    # Reglas de contradicción
    bloquea_contra = bool(getattr(cfg, "contradicciones_bloquean_entrada", True))
    if bloquea_contra and contradicciones:
        _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "contradicciones"})
        return _reject("contradicciones")

    # Validación de score técnico
    if not _validar_score(cfg, resultado_engine):
        _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "score_bajo"})
        return _reject("score_bajo")

    # Persistencia técnica (si está activada/instanciada)
    resultado_engine = _aplicar_persistencia(trader, symbol, resultado_engine, on_event=on_event)
    if not resultado_engine.get("persistencia_ok", True):
        _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "persistencia"})
        return _reject("persistencia")

    # Distancias mínimas SL/TP
    min_pct = _min_dist_pct(trader, symbol)
    sl, tp = _build_niveles(precio, min_pct)
    if not _validar_distancias(precio, sl, tp, min_pct):
        _emit(on_event, "entry_skip", {"symbol": symbol, "reason": "distancias"})
        return _reject("distancias")

    # Enriquecimiento opcional con coincidencia parcial (histórico↔pesos)
    try:
        # Si el engine provee estructuras para esta comparación, úsalo
        historial = resultado_engine.get("historial")  # p. ej. dict de señales recientes
        pesos = resultado_engine.get("pesos")          # p. ej. ponderaciones por indicador
        match = coincidencia_parcial(historial or {}, pesos or {})
    except Exception:
        match = 0.0

    propuesta = {
        "symbol": symbol,
        "side": side,
        "precio_entrada": precio,
        "stop_loss": sl,
        "take_profit": tp,
        "score": score,
        "timestamp": ts_value,
        "persistencia_ok": bool(resultado_engine.get("persistencia_ok", True)),
        "meta": {
            "min_dist_pct": min_pct,
            "contradicciones": contradicciones,
            "match_parcial": match,
        },
    }

    _emit(on_event, "entry_candidate", {"symbol": symbol, "side": side, "score": score})
    return _approve(propuesta)



