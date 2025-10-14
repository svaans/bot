"""Lógica de procesamiento de velas para ``TraderLite``."""

from __future__ import annotations

import inspect
from typing import Any, Awaitable, Callable, Dict, List, Optional

from core.metrics import (
    BARS_IN_FUTURE_TOTAL,
    BARS_OUT_OF_RANGE_TOTAL,
    EVAL_ATTEMPTS_TOTAL,
    TRADER_PROCESAR_VELA_CALLS_TOTAL,
    TRADER_SKIPS_TOTAL,
    WAITING_CLOSE_STREAK,
)
from core.utils.log_utils import safe_extra
from core.utils.utils import configurar_logger

from ._utils import _normalize_timestamp, _reason_none, tf_seconds


log = configurar_logger("trader_modular", modo_silencioso=True)


class TraderLiteProcessingMixin:
    """Mezcla con utilidades de procesamiento de velas."""

    def _after_procesar_vela(self, symbol: str) -> None:
        """Hook que permite a subclases ejecutar lógica adicional."""

        return None

    def _build_handler_invoker(
        self, handler: Callable[..., Awaitable[None]]
    ) -> Callable[[dict], Awaitable[None]]:
        signature = inspect.signature(handler)
        trader_aliases = {"trader", "bot", "manager"}
        candle_aliases = ("vela", "candle", "kline", "bar", "candle_data")

        positional_params = [
            p
            for p in signature.parameters.values()
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]

        first_positional_name = positional_params[0].name if positional_params else None
        needs_trader = any(alias in signature.parameters for alias in trader_aliases)
        if not needs_trader and len(positional_params) >= 2:
            if first_positional_name not in candle_aliases:
                needs_trader = True

        async def _invoke(candle: dict) -> None:
            args: List[Any] = []
            kwargs: Dict[str, Any] = {}
            candle_assigned = False

            for name, param in signature.parameters.items():
                if param.kind == inspect.Parameter.VAR_POSITIONAL:
                    continue
                if name == "self":
                    continue
                if name in trader_aliases:
                    if param.kind in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    ):
                        args.append(self)
                    else:
                        kwargs[name] = self
                    continue
                if name in candle_aliases:
                    if param.kind in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    ):
                        args.append(candle)
                    else:
                        kwargs[name] = candle
                    candle_assigned = True
                    continue
                if not candle_assigned and param.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                ) and param.default is inspect._empty:
                    args.append(candle)
                    candle_assigned = True
                    continue
                if (
                    not candle_assigned
                    and param.kind == inspect.Parameter.KEYWORD_ONLY
                    and param.default is inspect._empty
                ):
                    kwargs[name] = candle
                    candle_assigned = True
                    continue
                if (
                    param.default is inspect._empty
                    and param.kind
                    not in (
                        inspect.Parameter.VAR_POSITIONAL,
                        inspect.Parameter.VAR_KEYWORD,
                    )
                ):
                    raise TypeError(
                        "candle_handler requiere parámetro obligatorio "
                        f"'{name}' sin valor por defecto; usa un partial o "
                        "ajusta la firma para incluir solo (trader, vela)."
                    )

            if needs_trader and all(alias not in signature.parameters for alias in trader_aliases):
                args.insert(0, self)

            if not candle_assigned:
                if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values()):
                    kwargs.setdefault("vela", candle)
                    candle_assigned = True
                else:
                    raise TypeError(
                        "candle_handler no define un parámetro para la vela; "
                        "asegúrate de aceptar (trader, vela) o solo la vela."
                    )

            await handler(*args, **kwargs)

        return _invoke

    @staticmethod
    def _extract_symbol(candle: dict, *, fallback: str = "") -> str:
        if not isinstance(candle, dict):
            return str(fallback).upper()
        for key in ("symbol", "s"):
            value = candle.get(key)
            if value:
                return str(value).upper()
        return str(fallback).upper()

    @staticmethod
    def _extract_timestamp(candle: dict) -> Any:
        if not isinstance(candle, dict):
            return None
        for key in ("timestamp", "close_time", "closeTime", "open_time", "openTime"):
            value = candle.get(key)
            if value is None:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return value
        return None

    @staticmethod
    def _set_skip_reason(candle: dict, reason: str | None, details: Optional[dict] = None) -> None:
        if not isinstance(candle, dict):
            return
        if reason:
            previous = candle.get("_df_skip_reason")
            candle["_df_skip_reason"] = reason
            if details:
                candle["_df_skip_details"] = details
            elif "_df_skip_details" in candle:
                candle.pop("_df_skip_details", None)
            if previous != reason:
                TRADER_SKIPS_TOTAL.labels(reason=reason).inc()
        else:
            candle.pop("_df_skip_reason", None)
            candle.pop("_df_skip_details", None)

    def _update_estado_con_candle(self, candle: dict) -> bool:
        sym = self._extract_symbol(candle)
        ts = self._extract_timestamp(candle)
        self._set_skip_reason(candle, None)
        candle.pop("_df_eval_context", None)
        log.debug(
            "update_estado.enter",
            extra=safe_extra(
                {
                    "symbol": sym,
                    "timestamp": ts,
                    "timeframe": candle.get("timeframe")
                    or candle.get("interval")
                    or candle.get("tf"),
                    "stage": "Trader._update_estado_con_candle",
                }
            ),
        )
        if not sym or sym not in self.estado:
            log.debug(
                "update_estado.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": candle.get("timeframe")
                        or candle.get("interval")
                        or candle.get("tf"),
                        "stage": "Trader._update_estado_con_candle",
                        "result": "passthrough",
                    }
                ),
            )
            return True

        timeframe_raw = (
            candle.get("timeframe")
            or candle.get("interval")
            or candle.get("tf")
            or getattr(self.config, "intervalo_velas", None)
        )
        timeframe = str(timeframe_raw or "") or str(getattr(self.config, "intervalo_velas", ""))
        candle["symbol"] = sym
        if timeframe:
            candle["timeframe"] = timeframe

        if self._backfill_service is not None:
            should_process, normalized = self._backfill_service.handle_live_candle(sym, timeframe, candle)
            if not should_process:
                self._set_skip_reason(candle, "backfill_not_ready")
                log.debug(
                    "update_estado.skip",
                    extra=safe_extra(
                        {
                            "symbol": sym,
                            "timestamp": ts,
                            "timeframe": timeframe,
                            "stage": "Trader._update_estado_con_candle",
                            "reason": "backfill_not_ready",
                        }
                    ),
                )
                log.debug(
                    "update_estado.exit",
                    extra=safe_extra(
                        {
                            "symbol": sym,
                            "timestamp": ts,
                            "timeframe": timeframe,
                            "stage": "Trader._update_estado_con_candle",
                            "result": "skipped",
                        }
                    ),
                )
                return False
            if normalized is not None:
                for key in ("open_time", "close_time", "timeframe", "timestamp"):
                    if key not in candle and key in normalized:
                        candle[key] = normalized[key]
                timeframe = str(candle.get("timeframe", timeframe))
                ts = self._extract_timestamp(candle)

        if not self.is_symbol_ready(sym, timeframe):
            self._set_skip_reason(candle, "symbol_not_ready")
            log.debug(
                "update_estado.skip",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": timeframe,
                        "stage": "Trader._update_estado_con_candle",
                        "reason": "symbol_not_ready",
                    }
                ),
            )
            log.debug(
                "update_estado.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": timeframe,
                        "stage": "Trader._update_estado_con_candle",
                        "result": "skipped",
                    }
                ),
            )
            return False

        estado = self.estado[sym]

        tf_label = timeframe or str(getattr(self.config, "intervalo_velas", ""))
        if not tf_label:
            tf_label = "unknown"
        interval_secs = tf_seconds(tf_label)
        min_bars = self._resolve_min_bars_requirement()
        buffer_len_after = len(estado.buffer) + 1

        log.debug(
            "procesar_vela.pre-eval",
            extra=safe_extra(
                {
                    "symbol": sym,
                    "tf": tf_label,
                    "buffer_len": buffer_len_after,
                    "min_needed": min_bars,
                }
            ),
        )

        def _first_not_none(*values: Any) -> Any:
            for value in values:
                if value is not None:
                    return value
            return None

        bar_open_raw = _first_not_none(
            candle.get("open_time"),
            candle.get("openTime"),
            candle.get("timestamp"),
        )
        event_raw = _first_not_none(
            candle.get("event_time"),
            candle.get("eventTime"),
            candle.get("E"),
            candle.get("close_time"),
            candle.get("closeTime"),
            candle.get("timestamp"),
        )
        bar_close_raw = _first_not_none(
            candle.get("close_time"),
            candle.get("closeTime"),
            candle.get("event_time"),
        )

        bar_open_sec = _normalize_timestamp(bar_open_raw)
        event_sec = _normalize_timestamp(event_raw)
        bar_close_sec = _normalize_timestamp(bar_close_raw)

        timing_ctx: Dict[str, Any] = {}
        reason = _reason_none(
            sym,
            tf_label,
            buffer_len_after,
            min_bars,
            bar_open_sec,
            event_sec,
            interval_secs=interval_secs or None,
            skew_allow=self._skew_allow_secs,
            bar_close_ts=bar_close_sec,
            context=timing_ctx,
        )
        if timing_ctx.get("interval_secs") is None and interval_secs:
            timing_ctx["interval_secs"] = interval_secs
        timing_ctx.setdefault("skew_allow_secs", self._skew_allow_secs)

        def _safe_int(value: Any) -> int | None:
            try:
                if value is None:
                    return None
                return int(float(value))
            except (TypeError, ValueError):
                return None

        timing_log: Dict[str, Any] = {
            "symbol": sym,
            "timestamp": ts,
            "timeframe": tf_label,
            "interval_secs": timing_ctx.get("interval_secs"),
            "bar_open_raw_ms": _safe_int(bar_open_raw),
            "event_raw_ms": _safe_int(event_raw),
            "bar_close_raw_ms": _safe_int(bar_close_raw),
            "bar_open_sec": timing_ctx.get("bar_open_ts"),
            "event_sec": timing_ctx.get("bar_event_ts"),
            "bar_close_sec": timing_ctx.get("bar_close_ts"),
            "elapsed_secs": timing_ctx.get("elapsed_secs"),
            "buffer_len": buffer_len_after,
            "min_bars": min_bars,
            "skew_allow_secs": timing_ctx.get("skew_allow_secs"),
            "is_closed": bool(candle.get("is_closed", False)),
            "decision": reason,
            "stage": "Trader._update_estado_con_candle",
        }
        source_stream = candle.get("stream") or candle.get("source_stream")
        if source_stream:
            timing_log["source_stream"] = source_stream
        subscription = candle.get("subscription") or candle.get("channel")
        if subscription:
            timing_log["subscription"] = subscription
        log.debug("update_estado.timing", extra=safe_extra(timing_log))

        metric_timeframe = tf_label or "unknown"
        metric_labels = {"symbol": sym, "timeframe": metric_timeframe}
        streak_key = (sym, metric_timeframe.lower())

        if reason in {"bar_in_future", "bar_ts_out_of_range"}:
            metric = BARS_IN_FUTURE_TOTAL if reason == "bar_in_future" else BARS_OUT_OF_RANGE_TOTAL
            metric.labels(**metric_labels).inc()
            self._waiting_close_streak[streak_key] = 0
            WAITING_CLOSE_STREAK.labels(**metric_labels).set(0)
            details = {
                "timing": timing_ctx,
                "buffer_len": buffer_len_after,
                "min_needed": min_bars,
            }
            self._set_skip_reason(candle, reason, details)
            log.debug(
                "update_estado.skip",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": metric_timeframe,
                        "stage": "Trader._update_estado_con_candle",
                        "reason": reason,
                        "details": details,
                    }
                ),
            )
            log.debug(
                "update_estado.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": metric_timeframe,
                        "stage": "Trader._update_estado_con_candle",
                        "result": "skipped",
                    }
                ),
            )
            return False

        if not estado.candle_filter.accept(candle):
            stats = dict(estado.candle_filter.estadisticas)
            self._set_skip_reason(candle, "candle_filter", {"stats": stats})
            log.debug(
                "update_estado.skip",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": timeframe,
                        "stage": "Trader._update_estado_con_candle",
                        "reason": "candle_filter",
                        "estadisticas": stats,
                    }
                ),
            )
            log.debug(
                "update_estado.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": timeframe,
                        "stage": "Trader._update_estado_con_candle",
                        "result": "skipped",
                    }
                ),
            )
            return False

        estado.ultimo_timestamp = candle.get("timestamp", estado.ultimo_timestamp)
        estado.buffer.append(candle)
        try:
            self.supervisor.tick_data(sym)
        except Exception:
            log.exception("Error notificando tick_data al supervisor")

        if reason == "warmup":
            details = {
                "buffer_len": buffer_len_after,
                "min_needed": min_bars,
                "timing": timing_ctx,
            }
            self._waiting_close_streak[streak_key] = 0
            WAITING_CLOSE_STREAK.labels(**metric_labels).set(0)
            self._set_skip_reason(candle, "warmup", details)
            log.debug(
                "update_estado.skip",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": metric_timeframe,
                        "stage": "Trader._update_estado_con_candle",
                        "reason": "warmup",
                        "details": details,
                    }
                ),
            )
            log.debug(
                "update_estado.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": metric_timeframe,
                        "stage": "Trader._update_estado_con_candle",
                        "result": "skipped",
                    }
                ),
            )
            return False

        if reason == "waiting_close":
            details = {
                "buffer_len": buffer_len_after,
                "min_needed": min_bars,
                "timing": timing_ctx,
            }
            streak = self._waiting_close_streak[streak_key] + 1
            self._waiting_close_streak[streak_key] = streak
            WAITING_CLOSE_STREAK.labels(**metric_labels).set(streak)
            self._set_skip_reason(candle, "waiting_close", details)
            log.debug(
                "update_estado.skip",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": metric_timeframe,
                        "stage": "Trader._update_estado_con_candle",
                        "reason": "waiting_close",
                        "details": details,
                    }
                ),
            )
            log.debug(
                "update_estado.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": metric_timeframe,
                        "stage": "Trader._update_estado_con_candle",
                        "result": "skipped",
                    }
                ),
            )
            return False

        self._waiting_close_streak[streak_key] = 0
        WAITING_CLOSE_STREAK.labels(**metric_labels).set(0)
        EVAL_ATTEMPTS_TOTAL.labels(**metric_labels).inc()
        log.debug(
            "procesar_vela.go_evaluate",
            extra=safe_extra(
                {
                    "symbol": sym,
                    "tf": metric_timeframe,
                    "reason": reason,
                }
            ),
        )
        ts = self._extract_timestamp(candle)
        eval_reason = "go_evaluate"
        if reason and reason not in {"ready", "timeframe_none"}:
            eval_reason = str(reason)
        eval_context = {
            "symbol": sym,
            "timeframe": metric_timeframe,
            "reason": eval_reason,
            "raw_reason": reason,
            "bar_open_ts": timing_ctx.get("bar_open_ts"),
            "event_ts": timing_ctx.get("bar_event_ts"),
            "bar_close_ts": timing_ctx.get("bar_close_ts"),
            "elapsed_secs": timing_ctx.get("elapsed_secs"),
            "interval_secs": timing_ctx.get("interval_secs"),
            "skew_allow_secs": timing_ctx.get("skew_allow_secs"),
            "buffer_len": buffer_len_after,
            "min_needed": min_bars,
            "buffer_ready": buffer_len_after >= min_bars,
        }
        candle["_df_eval_context"] = eval_context
        log.debug(
            "update_estado.exit",
            extra=safe_extra(
                {
                    "symbol": sym,
                    "timestamp": ts,
                    "timeframe": metric_timeframe,
                    "stage": "Trader._update_estado_con_candle",
                    "result": "accepted",
                    "decision": reason,
                }
            ),
        )
        return True

    async def _procesar_vela(self, candle: dict) -> None:
        sym = self._extract_symbol(candle)
        ts = self._extract_timestamp(candle)
        timeframe = (
            candle.get("timeframe")
            or candle.get("interval")
            or candle.get("tf")
            or getattr(self.config, "intervalo_velas", None)
        )
        tf_str = str(timeframe) if timeframe else None
        eval_context = candle.get("_df_eval_context") if isinstance(candle, dict) else None
        metric_tf_label = (tf_str or "unknown").lower()
        TRADER_PROCESAR_VELA_CALLS_TOTAL.labels(symbol=sym, tf=metric_tf_label).inc()
        attempt_payload = {
            "symbol": sym,
            "timestamp": ts,
            "timeframe": tf_str,
            "stage": "Trader._procesar_vela",
            "event": "procesar_vela.attempt",
            "bar_open_ts": None,
            "event_ts": None,
            "elapsed_secs": None,
            "interval_secs": None,
            "buffer_len": None,
            "min_needed": None,
            "bar_close_ts": None,
            "skew_allow_secs": None,
            "buffer_ready": None,
        }
        attempt_reason = "go_evaluate"
        if isinstance(eval_context, dict):
            context_timeframe = eval_context.get("timeframe")
            if not tf_str and context_timeframe:
                attempt_payload["timeframe"] = context_timeframe
            for key in (
                "bar_open_ts",
                "event_ts",
                "elapsed_secs",
                "interval_secs",
                "buffer_len",
                "min_needed",
                "bar_close_ts",
                "skew_allow_secs",
                "buffer_ready",
            ):
                if key in eval_context:
                    attempt_payload[key] = eval_context[key]
            attempt_payload["raw_reason"] = eval_context.get("raw_reason")
            attempt_reason = str(eval_context.get("reason") or attempt_reason)
        attempt_payload["reason"] = attempt_reason
        log.info("procesar_vela.attempt", extra=safe_extra(attempt_payload))
        log.debug(
            "procesar_vela.enter",
            extra=safe_extra(
                {
                    "symbol": sym,
                    "timestamp": ts,
                    "timeframe": tf_str,
                    "stage": "Trader._procesar_vela",
                }
            ),
        )
        outcome = "ok"
        try:
            await self._handler_invoker(candle)
        except Exception:
            outcome = "error"
            raise
        else:
            skip_reason = candle.get("_df_skip_reason")
            if skip_reason == "warmup":
                log.debug(
                    "procesar_vela.skip",
                    extra=safe_extra(
                        {
                            "symbol": sym,
                            "timestamp": ts,
                            "timeframe": tf_str,
                            "stage": "Trader._procesar_vela",
                            "reason": skip_reason,
                        }
                    ),
                )
        finally:
            log.debug(
                "procesar_vela.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": tf_str,
                        "stage": "Trader._procesar_vela",
                        "result": outcome,
                    }
                ),
            )
            try:
                self._after_procesar_vela(sym)
            except Exception:
                log.debug(
                    "post_procesar_vela.error",
                    extra=safe_extra({"symbol": sym, "stage": "Trader._procesar_vela"}),
                    exc_info=True,
                )

    def _resolve_min_bars_requirement(self) -> int:
        candidatos: List[Any] = [getattr(self, "min_bars", None)]
        cfg = getattr(self, "config", None)
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

        return 0

    def _should_evaluate(
        self,
        symbol: str,
        timeframe: Optional[str],
        last_bar_ts: Optional[int],
    ) -> bool:
        if last_bar_ts is None:
            return True

        try:
            normalized_ts = int(last_bar_ts)
        except (TypeError, ValueError):
            return True

        key = (symbol.upper(), str(timeframe or "").lower())
        prev = self._last_evaluated_bar.get(key)
        if prev is not None and normalized_ts <= prev:
            return False

        self._last_evaluated_bar[key] = normalized_ts
        return True
