# core/vela/pipeline_procesar.py — handler principal de vela cerrada y salidas previas
from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

import pandas as pd

from core.metrics_helpers import safe_inc, safe_set
from core.orders.order_open_status import OrderOpenStatus
from core.utils.log_utils import format_exception_for_log
from core.utils.logger import configurar_logger
from indicadores.incremental import (
    actualizar_atr_incremental,
    actualizar_momentum_incremental,
    actualizar_rsi_incremental,
)

from core.vela.buffers import _resolve_buffer_manager
from core.vela.circuit_breaker import (
    OrderCircuitBreakerOpen,
    _resolve_order_circuit_store,
)
from core.vela.helpers import (
    _entry_open_notify_dedup_key,
    _format_entry_open_notification,
    _get_fastpath_mode,
    _is_num,
    _mark_skip,
    _normalize_timestamp,
    _resolve_entrada_cooldown_tras_crear_failed_sec,
    _resolve_min_bars,
    _resolve_trace_id,
    _set_fastpath_mode,
    _validar_candle,
)
from core.vela.order_open import _abrir_orden

log = configurar_logger("procesar_vela")


async def _maybe_verificar_salidas_vela(trader: Any, symbol: str, df: Any, cfg: Any) -> None:
    """Evalúa reglas de salida (SL/TP, trailing, etc.) si hay posición abierta.

    Se ejecuta antes del fastpath de entradas para que los cierres no queden
    bloqueados cuando se omiten evaluaciones de entrada por presión.
    """
    orders = getattr(trader, "orders", None)
    obtener = getattr(orders, "obtener", None) if orders is not None else None
    if not callable(obtener):
        return
    try:
        if obtener(symbol) is None:
            return
    except Exception:
        log.debug("salidas.skip_obtener", extra={"symbol": symbol}, exc_info=True)
        return
    if not isinstance(df, pd.DataFrame) or df.empty:
        return
    try:
        from core.strategies.exit.verificar_salidas import verificar_salidas
    except Exception:
        log.warning(
            "salidas.import_failed",
            extra={"symbol": symbol, "error": "verificar_salidas no importable"},
        )
        return
    timeout_raw = getattr(cfg, "timeout_verificar_salidas", None) if cfg is not None else None
    if timeout_raw is None:
        timeout_sec = 20.0
    else:
        try:
            timeout_sec = float(timeout_raw)
        except (TypeError, ValueError):
            timeout_sec = 20.0
    timeout_sec = max(5.0, min(timeout_sec, 120.0))
    try:
        await asyncio.wait_for(verificar_salidas(trader, symbol, df), timeout=timeout_sec)
    except asyncio.TimeoutError:
        log.warning(
            "verificar_salidas.timeout",
            extra={"symbol": symbol, "timeout_sec": timeout_sec},
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("verificar_salidas.error", extra={"symbol": symbol})


async def procesar_vela(trader: Any, vela: dict) -> None:
    """
    Handler de vela cerrada.
    Requisitos de `trader`:
      - atributos: config, spread_guard (opcional), orders (OrderManager), estado (dict) si quieres estados extra
      - método: evaluar_condiciones_de_entrada(symbol, df, estado)
      - método opcional: enqueue_notification(mensaje, nivel, dedup_key=..., **meta)
      - con posición abierta: ``verificar_salidas`` (salidas) vía ``_cerrar_y_reportar`` / ``_piramidar`` en :class:`core.trader.trader.Trader`
    """
    t0 = time.perf_counter()
    symbol = ""
    timeframe_label = "unknown"
    timeframe_hint: Optional[str] = None
    if isinstance(vela, dict):
        raw_symbol = vela.get("symbol") or vela.get("par") or ""
        symbol = str(raw_symbol).upper() if raw_symbol else ""
        config_interval = getattr(getattr(trader, "config", None), "intervalo_velas", None)
        timeframe_hint = (
            vela.get("timeframe")
            or vela.get("interval")
            or vela.get("tf")
            or config_interval
        )
        if timeframe_hint:
            timeframe_label = str(timeframe_hint)
    stage_durations: dict[str, float] = {}
    stage_ends: dict[str, float] = {}
    parse_end: float | None = None
    gating_end: float | None = None
    strategy_end: float | None = None
    bus = getattr(trader, "event_bus", None) or getattr(trader, "bus", None)
    orders = getattr(trader, "orders", None)
    buf = _resolve_buffer_manager(trader)
    # Import en runtime: evita ciclo al cargar módulos y respeta monkeypatch en ``core.vela.pipeline``.
    import core.vela.pipeline as _pm

    def _mark_stage(stage: str, start_time: float | None) -> float:
        existing = stage_ends.get(stage)
        if existing is not None:
            return existing
        end_time = time.perf_counter()
        base = start_time if start_time is not None else t0
        duration = max(0.0, end_time - base)
        stage_durations[stage] = duration
        stage_ends[stage] = end_time
        return end_time

    def _ensure_gating_end() -> float:
        nonlocal parse_end, gating_end
        if parse_end is None:
            parse_end = _mark_stage("parse", t0)

        if orders is not None and hasattr(orders, "actualizar_mark_to_market"):
            close_value = vela.get("close")
            if close_value is None:
                close_value = vela.get("c")
            try:
                precio_mark = float(close_value) if close_value is not None else None
            except (TypeError, ValueError):
                precio_mark = None
            if precio_mark is not None and precio_mark > 0:
                try:
                    orders.actualizar_mark_to_market(symbol, precio_mark)
                except Exception:
                    log.debug(
                        "mark_to_market.error",
                        extra={"symbol": symbol, "stage": "procesar_vela"},
                        exc_info=True,
                    )
        if gating_end is None:
            gating_end = _mark_stage("gating", parse_end)
        return gating_end

    def _ensure_strategy_end() -> float:
        nonlocal parse_end, gating_end, strategy_end
        if gating_end is None:
            _ensure_gating_end()
        if strategy_end is None:
            strategy_end = _mark_stage("strategy", gating_end)
        return strategy_end

    try:
        if not isinstance(vela, dict):
            parse_end = _mark_stage("parse", t0)
            return

        vela.pop("_df_skip_reason", None)
        vela.pop("_df_skip_details", None)
        # 1) Validaciones mínimas y normalización
        if not symbol:
            parse_end = _mark_stage("parse", t0)
            safe_inc(_pm.CANDLES_IGNORADAS, reason="no_symbol")
            _mark_skip(vela, "no_symbol")
            return

        ok, reason = _validar_candle(vela)
        if not ok:
            parse_end = _mark_stage("parse", t0)
            safe_inc(_pm.CANDLES_IGNORADAS, reason=reason)
            _mark_skip(vela, reason)
            return

        if parse_end is None:
            parse_end = _mark_stage("parse", t0)

        # 2) Control por spread (si hay guardia y si el dato viene en la vela)
        spread_ratio = vela.get("spread_ratio") or vela.get("spread")
        sg = getattr(trader, "spread_guard", None)
        if sg is not None and spread_ratio is None:
            safe_inc(
                _pm.SPREAD_GUARD_MISSING,
                symbol=symbol or "unknown",
                timeframe=(timeframe_label or "unknown"),
            )
            log.warning(
                "spread_guard_missing",
                extra={
                    "symbol": symbol or "unknown",
                    "timeframe": timeframe_label or "unknown",
                },
            )
        if spread_ratio is not None and sg is not None:
            try:
                if hasattr(sg, "allows"):
                    if not sg.allows(symbol, float(spread_ratio)):
                        _ensure_gating_end()
                        safe_inc(
                            _pm.ENTRADAS_RECHAZADAS_V2,
                            symbol=symbol,
                            timeframe=timeframe_label,
                            reason="spread_guard",
                        )
                        _mark_skip(
                            vela,
                            "spread_guard",
                            {"ratio": float(spread_ratio)},
                        )
                        lock = buf.get_lock(symbol, timeframe_hint)
                        async with lock:
                            buf.append(symbol, vela, timeframe=timeframe_hint)
                        return
                elif hasattr(sg, "permite_entrada"):
                    if not bool(sg.permite_entrada(symbol, {}, 0.0)):
                        _ensure_gating_end()
                        safe_inc(
                            _pm.ENTRADAS_RECHAZADAS_V2,
                            symbol=symbol,
                            timeframe=timeframe_label,
                            reason="spread_guard",
                        )
                        _mark_skip(vela, "spread_guard")
                        lock = buf.get_lock(symbol, timeframe_hint)
                        async with lock:
                            buf.append(symbol, vela, timeframe=timeframe_hint)
                        return
            except Exception as exc:
                try:
                    _pm.SPREAD_GUARD_ERRORS.labels(symbol=symbol or "unknown").inc()
                except Exception:
                    pass
                log.warning(
                    "spread_guard.error",
                    extra={
                        "symbol": symbol or "unknown",
                        "timeframe": timeframe_label,
                        "error": format_exception_for_log(exc),
                    },
                    exc_info=True,
                )

        # 3) Append al buffer por símbolo (protegido por lock)
        buffer_timeframe = timeframe_hint
        lock = buf.get_lock(symbol, buffer_timeframe)
        async with lock:
            buf.append(symbol, vela, timeframe=buffer_timeframe)
            df = buf.dataframe(symbol, timeframe=buffer_timeframe)
            symbol_state = buf.state(symbol, timeframe=buffer_timeframe)

        if df is None or df.empty:
            _ensure_gating_end()
            safe_inc(_pm.CANDLES_IGNORADAS, reason="empty_df")
            _mark_skip(vela, "empty_df")
            return

        # Compartir el estado del buffer con el resto del pipeline.
        if "buffer_state" not in vela:
            vela["buffer_state"] = symbol_state

        timeframe = getattr(df, "tf", None)
        if timeframe:
            timeframe = str(timeframe)
            timeframe_label = timeframe
        else:
            if timeframe_hint:
                timeframe = str(timeframe_hint)
                timeframe_label = timeframe

        cfg = getattr(trader, "config", None)
        estado_trader = getattr(trader, "estado", None)
        estado_symbol: Any | None = None
        if isinstance(estado_trader, dict):
            estado_symbol = estado_trader.setdefault(symbol, {})
        elif estado_trader is not None:
            try:
                estado_symbol = getattr(estado_trader, symbol)
            except Exception:
                estado_symbol = None
            if estado_symbol is None:
                nuevo_estado: dict[str, Any] = {}
                try:
                    setattr(estado_trader, symbol, nuevo_estado)
                    estado_symbol = nuevo_estado
                except Exception:
                    estado_symbol = nuevo_estado
        incremental_enabled = bool(
            getattr(cfg, "indicadores_incremental_enabled", False)
            or getattr(trader, "indicadores_incremental_enabled", False)
        )
        if incremental_enabled:
            incremental_cache = symbol_state.indicators_state
            if isinstance(estado_symbol, dict):
                estado_symbol["indicadores_cache"] = incremental_cache
            elif estado_symbol is not None:
                try:
                    setattr(estado_symbol, "indicadores_cache", incremental_cache)
                except Exception:
                    pass
            try:
                actualizar_rsi_incremental(symbol_state, df=df)
                actualizar_momentum_incremental(symbol_state, df=df)
                actualizar_atr_incremental(symbol_state, df=df)
            except Exception as exc:
                log.debug(
                    "incremental_indicators_failed",
                    extra={
                        "symbol": symbol,
                        "timeframe": timeframe_label,
                        "error": format_exception_for_log(exc),
                    },
                )

        ready_checker = getattr(trader, "is_symbol_ready", None)
        if callable(ready_checker) and not ready_checker(symbol, timeframe_label):
            _ensure_gating_end()
            safe_inc(_pm.CANDLES_IGNORADAS, reason="backfill_pending")
            _mark_skip(vela, "backfill_pending")
            return

        min_needed = _resolve_min_bars(trader)

        try:
            faltan = max(0, min_needed - len(df)) if min_needed > 0 else 0
            safe_set(
                _pm.WARMUP_RESTANTE,
                float(faltan),
                symbol=symbol,
                timeframe=(timeframe_label or "unknown"),
            )
        except Exception:
            pass

        try:
            last_ts = _normalize_timestamp(df.iloc[-1]["timestamp"])
            if last_ts is not None:
                edad = max(0.0, time.time() - last_ts)
                safe_set(
                    _pm.LAST_BAR_AGE,
                    float(edad),
                    symbol=symbol,
                    timeframe=(timeframe_label or "unknown"),
                )
        except Exception:
            pass

        # 4b) Salidas (SL/TP, trailing, contexto macro) si hay posición — antes del fastpath de entradas
        await _maybe_verificar_salidas_vela(trader, symbol, df, cfg)

        # 5) Fast-path si estás bajo presión con histéresis configurable
        fast_enabled = bool(getattr(cfg, "trader_fastpath_enabled", True))
        # Debe coincidir con config.Config (default False): sin atributo, no se salta por fastpath.
        if fast_enabled and getattr(cfg, "trader_fastpath_skip_entries", False):
            threshold = int(getattr(cfg, "trader_fastpath_threshold", 350))
            resume_threshold_cfg = getattr(cfg, "trader_fastpath_resume_threshold", None)
            if resume_threshold_cfg is None:
                recovery = int(getattr(cfg, "trader_fastpath_recovery", 0))
                resume_threshold = max(0, threshold - recovery) if recovery else threshold
            else:
                resume_threshold = int(resume_threshold_cfg)

            resume_threshold = min(resume_threshold, threshold)
            fastpath_mode = _get_fastpath_mode(estado_symbol)
            updated_mode = fastpath_mode
            buffer_len = len(df)
            if buffer_len >= threshold:
                updated_mode = "fastpath"
            elif buffer_len <= resume_threshold:
                updated_mode = "normal"

            if updated_mode != fastpath_mode:
                _set_fastpath_mode(estado_symbol, updated_mode)
                fastpath_mode = updated_mode
            else:
                fastpath_mode = updated_mode

            if fastpath_mode == "fastpath":
                _ensure_gating_end()
                safe_inc(
                    _pm.ENTRADAS_RECHAZADAS_V2,
                    symbol=symbol,
                    timeframe=timeframe_label,
                    reason="fastpath_skip_entries",
                )
                _mark_skip(
                    vela,
                    "fastpath_skip_entries",
                    {
                        "buffer_len": buffer_len,
                        "threshold": threshold,
                        "resume_threshold": resume_threshold,
                        "fastpath_mode": fastpath_mode,
                    },
                )
                return

        # 6) Evaluar condiciones de entrada (delegado al Trader)
        timeframe = timeframe or timeframe_label
        timeframe_label = str(timeframe or timeframe_label or "unknown")
        log.debug(
            "pre-eval",
            extra={
                "symbol": symbol,
                "timeframe": timeframe,
                "buffer_len": len(df),
                "min_needed": min_needed,
            },
        )
        log.debug(
            "go_evaluate",
            extra={
                "symbol": symbol,
                "timeframe": timeframe,
                "reason": "closed_bar",
            },
        )
        try:
            safe_inc(
                _pm.EVAL_INTENTOS_TOTAL,
                symbol=symbol,
                timeframe=timeframe,
                etapa="entrada",
            )
        except Exception:
            pass
        _ensure_gating_end()
        try:
            propuesta = await trader.evaluar_condiciones_de_entrada(symbol, df, estado_symbol)
        finally:
            _ensure_strategy_end()
        _ensure_gating_end()
        skip_reason = getattr(trader, "_last_eval_skip_reason", None)
        skip_details = getattr(trader, "_last_eval_skip_details", None)
        score = None
        if isinstance(propuesta, dict):
            score = propuesta.get("score")
        log.debug(
            "entrada_verificada",
            extra={
                "symbol": symbol,
                "timeframe": timeframe,
                "permitida": bool(propuesta),
                "score": score,
            },
        )
        if not propuesta:
            derived_score: Any | None = score
            if derived_score is None and isinstance(skip_details, dict):
                derived_score = skip_details.get("score")
            if skip_reason == "pipeline_missing":
                log.debug(
                    "pipeline_missing_info",
                    extra={"symbol": symbol, "timeframe": timeframe_label},
                )
                _mark_skip(
                    vela,
                    skip_reason,
                    skip_details,
                    gate=skip_reason,
                    score=derived_score,
                )
                return
            if skip_reason:
                _mark_skip(
                    vela,
                    skip_reason,
                    skip_details,
                    gate=skip_reason,
                    score=derived_score,
                )
            else:
                provider = getattr(trader, "_verificar_entrada_provider", None)
                details = {"provider": provider} if provider else None
                _mark_skip(vela, "no_signal", details, score=derived_score)
            return

        side = str(propuesta.get("side", "long")).lower()
        safe_inc(_pm.ENTRADAS_CANDIDATAS, symbol=symbol, side=side)

        # 7) Validación final
        precio = float(propuesta.get("precio_entrada", df.iloc[-1]["close"]))
        if not _is_num(precio) or precio <= 0:
            safe_inc(
                _pm.ENTRADAS_RECHAZADAS_V2,
                symbol=symbol,
                timeframe=timeframe_label,
                reason="bad_price",
            )
            _mark_skip(vela, "bad_price", score=score)
            return

        # 8) Apertura de orden
        if orders is None or not hasattr(orders, "crear"):
            safe_inc(
                _pm.ENTRADAS_RECHAZADAS_V2,
                symbol=symbol,
                timeframe=timeframe_label,
                reason="orders_missing",
            )
            _mark_skip(vela, "orders_missing", score=score)
            return

        sl = float(propuesta.get("stop_loss", 0.0))
        tp = float(propuesta.get("take_profit", 0.0))
        meta = dict(propuesta.get("meta", {}))
        meta.update({"score": propuesta.get("score")})

        trace_id: Optional[str] = None
        if isinstance(vela, dict):
            trace_id = vela.get("_df_trace_id")
            if not trace_id:
                trace_id = _resolve_trace_id(vela)
                if trace_id:
                    vela["_df_trace_id"] = trace_id
        if trace_id:
            meta.setdefault("trace_id", trace_id)

        try:
            obtener = getattr(orders, "obtener", None)
            ya = obtener(symbol) if callable(obtener) else None
            if ya is not None:
                safe_inc(
                    _pm.ENTRADAS_RECHAZADAS_V2,
                    symbol=symbol,
                    timeframe=timeframe_label,
                    reason="ya_abierta",
                )
                _mark_skip(vela, "ya_abierta", score=score)
                return
        except Exception:
            pass

        try:
            status = await _abrir_orden(
                orders,
                symbol,
                side,
                precio,
                sl,
                tp,
                meta,
                trace_id=trace_id,
                circuit_store=_resolve_order_circuit_store(trader),
            )
            if status not in (OrderOpenStatus.OPENED, OrderOpenStatus.PENDING_REGISTRATION):
                safe_inc(
                    _pm.ENTRADAS_RECHAZADAS_V2,
                    symbol=symbol,
                    timeframe=timeframe_label,
                    reason="crear_failed",
                )
                _mark_skip(vela, "crear_failed", {"status": getattr(status, "value", str(status))}, score=score)
                cooldown_sec = _resolve_entrada_cooldown_tras_crear_failed_sec(trader, symbol)
                if cooldown_sec > 0:
                    reg = getattr(trader, "registrar_cooldown_entrada", None)
                    if callable(reg):
                        reg(symbol, cooldown_sec)
                        log.debug(
                            "crear_failed.entrada_cooldown",
                            extra={
                                "symbol": symbol,
                                "seconds": cooldown_sec,
                            },
                        )
                return
            safe_inc(_pm.ENTRADAS_ABIERTAS, symbol=symbol, side=side)
            notify = getattr(trader, "enqueue_notification", None)
            if callable(notify):
                msg = _format_entry_open_notification(
                    side,
                    symbol,
                    precio,
                    propuesta,
                    timeframe_label=timeframe_label,
                )
                dk = _entry_open_notify_dedup_key(
                    symbol,
                    side,
                    timeframe_label,
                    propuesta,
                    vela if isinstance(vela, dict) else None,
                )
                if dk:
                    notify(msg, "INFO", dedup_key=dk)
                else:
                    notify(msg, "INFO")
        except OrderCircuitBreakerOpen as exc:
            safe_inc(
                _pm.ENTRADAS_RECHAZADAS_V2,
                symbol=symbol,
                timeframe=timeframe_label,
                reason="orders_circuit_open",
            )
            retry_after = max(0.0, float(exc.retry_after))
            details = {
                "retry_after": retry_after,
                "failures": exc.state.failures,
                "side": side,
            }
            _mark_skip(
                vela,
                "orders_circuit_open",
                details,
                gate="orders_circuit_open",
                score=score,
            )
            log.warning(
                "orders_circuit_open",
                extra={
                    "symbol": symbol,
                    "side": side,
                    "retry_after": retry_after,
                    "failures": exc.state.failures,
                },
            )
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            safe_inc(_pm.HANDLER_EXCEPTIONS)
            log.exception("Error abriendo orden para %s: %s", symbol, e)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        safe_inc(_pm.HANDLER_EXCEPTIONS)
        log.exception("Excepción en procesar_vela: %s", e)
    finally:
        try:
            total_secs = max(0.0, time.perf_counter() - t0)
            accounted = (
                float(stage_durations.get("parse", 0.0) or 0.0)
                + float(stage_durations.get("gating", 0.0) or 0.0)
                + float(stage_durations.get("strategy", 0.0) or 0.0)
            )
            stage_durations["remainder"] = max(0.0, total_secs - accounted)
        except Exception:
            pass
        if isinstance(vela, dict):
            try:
                vela["_df_stage_durations"] = {
                    str(stage): float(duration)
                    for stage, duration in stage_durations.items()
                }
            except Exception:
                vela["_df_stage_durations"] = dict(stage_durations)
        try:
            if symbol:
                safe_inc(
                    _pm.CANDLES_PROCESSED_TOTAL,
                    symbol=symbol,
                    timeframe=(timeframe_label or "unknown"),
                )
        except Exception:
            pass
        try:
            stage_metric_map = {
                "parse": _pm.PARSE_LATENCY,
                "gating": _pm.GATING_LATENCY,
                "strategy": _pm.STRATEGY_LATENCY,
            }
            emit_fn = getattr(bus, "emit", None)
            for stage in ("parse", "gating", "strategy", "remainder"):
                duration = stage_durations.get(stage)
                if duration is None:
                    continue
                metric = stage_metric_map.get(stage)
                if metric is not None:
                    metric.observe(duration)
                if callable(emit_fn):
                    payload = {
                        "stage": stage,
                        "duration": duration,
                        "symbol": symbol,
                        "timeframe": timeframe_label,
                    }
                    try:
                        emit_fn("procesar_vela.latency", payload)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            _pm.EVAL_LATENCY.observe(time.perf_counter() - t0)
        except Exception:
            pass
