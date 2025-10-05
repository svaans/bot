"""Implementación modular mínima del núcleo del trader."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
from typing import Any, Awaitable, Callable, Dict, List, Optional, TYPE_CHECKING

from core.utils.utils import configurar_logger

from ._utils import EstadoSimbolo, _is_awaitable, _maybe_await

if TYPE_CHECKING:  # pragma: no cover
    from core.supervisor import Supervisor


log = configurar_logger("trader_modular", modo_silencioso=True)


def _compat_module():
    from core import trader_modular as compat

    return compat


def _data_feed_cls():
    return getattr(_compat_module(), "DataFeed")


def _supervisor_cls():
    return getattr(_compat_module(), "Supervisor")


def _crear_cliente_factory():
    return getattr(_compat_module(), "crear_cliente", None)


class TraderLite:
    """Orquesta DataFeedLite y delega el procesamiento de velas.

    Parámetros
    ----------
    config: objeto con al menos:
        - symbols: list[str]
        - intervalo_velas: str (p. ej. "1m")
        - modo_real: bool
    candle_handler: async callable(dict) -> None
        - Función que procesará cada vela cerrada (p. ej. tu `procesar_vela`).
        - Si no se provee y existe `core.procesar_vela.procesar_vela`, se intentará usarla.
    on_event: callable(evt: str, data: dict) -> None | None
        - Hook opcional para métricas / notificaciones.
    supervisor: SupervisorLite | None
        - Si no se provee, se crea uno interno.
    """

    def __init__(
        self,
        config: Any,
        *,
        candle_handler: Optional[Callable[[dict], Awaitable[None]]] = None,
        on_event: Optional[Callable[[str, dict], None]] = None,
        supervisor: Optional[Supervisor] = None,
    ) -> None:
        if not getattr(config, "symbols", None):
            raise ValueError("config.symbols vacío o no definido")
        if not getattr(config, "intervalo_velas", None):
            raise ValueError("config.intervalo_velas no definido")

        self.config = config
        self.on_event = on_event
        supervisor_cls = _supervisor_cls()
        self.supervisor = supervisor or supervisor_cls(on_event=on_event)

        # Estado por símbolo
        self.estado: Dict[str, EstadoSimbolo] = {s: EstadoSimbolo() for s in config.symbols}
        # Registro de tendencia por símbolo (utilizado por procesar_vela)
        self.estado_tendencia: Dict[str, Any] = {s: None for s in config.symbols}
        self._last_evaluated_bar: Dict[tuple[str, str], int] = {}

        # Protección dinámica ante spreads amplios (lazy import abajo).
        self.spread_guard: Any | None = None

        # Handler de velas
        self._handler = candle_handler or self._descubrir_handler_default()
        if not asyncio.iscoroutinefunction(self._handler):
            raise TypeError("candle_handler debe ser async (async def …)")
        self._handler_invoker = self._build_handler_invoker(self._handler)

        # DataFeedLite
        data_feed_cls = _data_feed_cls()
        self.feed = data_feed_cls(
            config.intervalo_velas,
            on_event=on_event,
            **self._resolve_data_feed_kwargs(),
        )
        # Señal para el StartupManager: este feed se arranca desde TraderLite
        try:
            setattr(self.feed, "_managed_by_trader", True)
        except Exception:
            log.debug("No se pudo marcar DataFeed como gestionado por Trader")

        # Cliente de exchange (solo modo real)
        self._cliente = None
        crear_cliente_fn = _crear_cliente_factory()
        if bool(getattr(config, "modo_real", False)) and crear_cliente_fn is not None:
            try:
                self._cliente = crear_cliente_fn(config)
            except Exception:
                # no bloquea el arranque en simulado
                self._cliente = None

        # Tareas
        self._stop_event = asyncio.Event()
        self._runner_task: Optional[asyncio.Task] = None

        # --- Lazy imports de módulos pesados / acoplados ---
        # Se difieren hasta ahora para evitar ciclos.
        try:
            from core.risk import SpreadGuard as _SpreadGuard
            self._SpreadGuard = _SpreadGuard
        except Exception:
            self._SpreadGuard = None
        try:
            from core.event_bus import EventBus as _EventBus
            from core.orders.order_manager import OrderManager as _OrderManager
            from core.orders.storage_simulado import sincronizar_ordenes_simuladas as _sync_sim
            from core.strategies import StrategyEngine as _StrategyEngine
            from core.persistencia_tecnica import PersistenciaTecnica as _PersistenciaTecnica
            from core.strategies.entry.verificar_entradas import verificar_entrada as _verificar_entrada
            self._EventBus = _EventBus
            self._OrderManager = _OrderManager
            self._sync_sim = _sync_sim
            self._StrategyEngine = _StrategyEngine
            self._PersistenciaTecnica = _PersistenciaTecnica
            self._verificar_entrada = _verificar_entrada
        except Exception:
            # Se inicializarán a None; el wrapper Trader aplicará guardas.
            self._EventBus = None
            self._OrderManager = None
            self._sync_sim = None
            self._StrategyEngine = None
            self._PersistenciaTecnica = None
            self._verificar_entrada = None

        # Configurar guardia de spread (puede devolver None si está deshabilitado).
        self.spread_guard = self._create_spread_guard()

    def _resolve_data_feed_kwargs(self) -> Dict[str, Any]:
        """Obtiene parámetros del ``DataFeed`` combinando config y entorno."""

        def _resolve(
            attr: str | None,
            env_key: str,
            default: Any,
            caster: Callable[[Any], Any],
        ) -> Any:
            if attr and hasattr(self.config, attr):
                value = getattr(self.config, attr)
                if value is not None:
                    try:
                        return caster(value)
                    except (TypeError, ValueError):
                        log.warning(
                            "Valor inválido para %s=%r en Config; usando fallback",
                            attr,
                            value,
                        )
            env_value = os.getenv(env_key)
            if env_value is not None:
                try:
                    return caster(env_value)
                except (TypeError, ValueError):
                    log.warning(
                        "Valor inválido para %s=%r en entorno; usando %r",
                        env_key,
                        env_value,
                        default,
                    )
            return caster(default)

        kwargs: Dict[str, Any] = {
            "handler_timeout": _resolve("handler_timeout", "DF_HANDLER_TIMEOUT_SEC", 2.0, float),
            "inactivity_intervals": _resolve("inactivity_intervals", "DF_INACTIVITY_INTERVALS", 10, int),
            "queue_max": _resolve("df_queue_default_limit", "DF_QUEUE_MAX", 2000, int),
            "queue_policy": _resolve("df_queue_policy", "DF_QUEUE_POLICY", "drop_oldest", lambda x: str(x).lower()),
            "monitor_interval": _resolve("monitor_interval", "DF_MONITOR_INTERVAL", 5.0, float),
            "cancel_timeout": _resolve(None, "DF_CANCEL_TIMEOUT", 5.0, float),
        }
        kwargs["backpressure"] = self._resolve_backpressure()
        return kwargs

    def _resolve_backpressure(self) -> bool:
        valor = getattr(self.config, "df_backpressure", None)
        if valor is not None:
            return bool(valor)
        raw = os.getenv("DF_BACKPRESSURE")
        if raw is None:
            return True
        return str(raw).lower() == "true"

    def _update_estado_con_candle(self, candle: dict) -> bool:
        """Actualiza buffers/estado y decide si se procesa la vela."""

        sym = candle.get("symbol")
        if not sym or sym not in self.estado:
            return True

        estado = self.estado[sym]
        if not estado.candle_filter.accept(candle):
            log.debug(
                "Vela rechazada por CandleFilter",
                extra={
                    "symbol": sym,
                    "timestamp": candle.get("timestamp"),
                    "estadisticas": dict(estado.candle_filter.estadisticas),
                },
            )
            return False

        estado.ultimo_timestamp = candle.get("timestamp", estado.ultimo_timestamp)
        estado.buffer.append(candle)
        try:
            self.supervisor.tick_data(sym)
        except Exception:
            log.exception("Error notificando tick_data al supervisor")
        return True

    # -------------------- API pública --------------------
    def start(self) -> None:
        """Arranca supervisor y la tarea principal del Trader."""
        # Soporta start_supervision sync/async
        try:
            res = self.supervisor.start_supervision()
            if _is_awaitable(res):
                asyncio.create_task(res, name="supervisor_start")
        except Exception:
            log.exception("Error iniciando supervisor (start_supervision)")
        if self._runner_task is None or self._runner_task.done():
            self._runner_task = asyncio.create_task(self._run(), name="trader_main")

    async def stop(self) -> None:
        """Solicita parada ordenada y espera cierre del feed."""
        self._stop_event.set()
        # Cierre del feed: soporta detener() sync/async/ausente
        try:
            detener = getattr(self.feed, "detener", None)
            if callable(detener):
                await _maybe_await(detener())
        except Exception:
            log.exception("Error deteniendo DataFeed")
        # Cierre del supervisor: sync/async
        try:
            await _maybe_await(self.supervisor.shutdown())
        except Exception:
            log.exception("Error cerrando supervisor")
        if self._runner_task:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._runner_task

    # ------------------- utilidades internas -------------------
    def _create_spread_guard(self) -> Any | None:
        """Instancia ``SpreadGuard`` cuando la configuración lo habilita."""
        base_limit = float(getattr(self.config, "max_spread_ratio", 0.0) or 0.0)
        dynamic_enabled = bool(getattr(self.config, "spread_dynamic", False))
        if not dynamic_enabled or base_limit <= 0:
            return None

        window = int(getattr(self.config, "spread_guard_window", 50) or 50)
        hysteresis = float(getattr(self.config, "spread_guard_hysteresis", 0.15) or 0.15)
        max_limit_default = base_limit * 5 if base_limit > 0 else 0.05
        max_limit = float(
            getattr(self.config, "spread_guard_max_limit", max_limit_default) or max_limit_default
        )

        # Normalizar parámetros para evitar valores inválidos.
        window = max(5, window)
        hysteresis = min(max(hysteresis, 0.0), 1.0)
        max_limit = max(base_limit, max_limit)

        try:
            if self._SpreadGuard is None:
                return None
            guard = self._SpreadGuard(
                base_limit=base_limit,
                max_limit=max_limit,
                window=window,
                hysteresis=hysteresis,
            )
        except Exception:
            log.exception("No se pudo inicializar SpreadGuard; se usará el límite estático")
            return None

        log.debug(
            "SpreadGuard habilitado",
            extra={
                "base_limit": base_limit,
                "max_limit": max_limit,
                "window": window,
                "hysteresis": hysteresis,
            },
        )
        return guard

    # ------------------- ciclo principal -------------------
    async def _run(self) -> None:
        symbols = list({s.upper() for s in self.config.symbols})
        self._emit("trader_start", {"symbols": symbols, "intervalo": self.config.intervalo_velas})

        async def _handler(c: dict) -> None:
            if not self._update_estado_con_candle(c):
                return
            await self._handler_invoker(c)

        # Registrar latidos periódicos
        self.supervisor.supervised_task(
            lambda: self._heartbeat_loop(), name="heartbeat_loop", expected_interval=60
        )

        # Lanzar DataFeedLite supervisado
        self.supervisor.supervised_task(
            lambda: self.feed.escuchar(symbols, _handler, cliente=self._cliente),
            name="data_feed",
            expected_interval=int(os.getenv("DF_EXPECTED_INTERVAL", "60")),
        )

        # Bucle de espera hasta stop
        while not self._stop_event.is_set():
            await asyncio.sleep(0.5)
        self._emit("trader_stop", {})

    async def _heartbeat_loop(self, interval: int = 60) -> None:
        while not self._stop_event.is_set():
            self.supervisor.beat("trader")
            await asyncio.sleep(interval)

    # --------------------- utilidades ---------------------
    def _emit(self, evt: str, data: dict) -> None:
        if self.on_event:
            try:
                self.on_event(evt, data)
            except Exception:
                pass

    def _descubrir_handler_default(self) -> Callable[[dict], Awaitable[None]]:
        """Intento suave de usar `core.procesar_vela.procesar_vela` si existe.
        Si no, devuelve un placeholder async (no-op).
        """
        try:  # pragma: no cover
            from core.procesar_vela import procesar_vela  # tu pipeline existente
            if asyncio.iscoroutinefunction(procesar_vela):
                return procesar_vela
        except Exception:
            pass

        async def _placeholder(_: dict) -> None:
            # No hace nada: pensado para pruebas de arranque
            return None

        return _placeholder

    def _build_handler_invoker(
        self, handler: Callable[..., Awaitable[None]]
    ) -> Callable[[dict], Awaitable[None]]:
        """Adapta firmas comunes de `candle_handler`.

        Permite reutilizar funciones existentes que esperan `(trader, vela)` o
        únicamente la `vela`. Si el handler requiere argumentos adicionales sin
        valores por defecto, se notifica explícitamente para evitar fallos
        silenciosos.
        """
        signature = inspect.signature(handler)
        trader_aliases = {"trader", "bot", "manager"}
        candle_aliases = ("vela", "candle", "kline", "bar", "candle_data")

        positional_params = [
            p
            for p in signature.parameters.values()
            if p.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]

        first_positional_name = positional_params[0].name if positional_params else None
        needs_trader = any(alias in signature.parameters for alias in trader_aliases)
        # Heurística: si hay ≥2 posicionales y el primero NO parece ser la vela, asumimos (trader, vela)
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
                # Si inferimos que necesita trader por la cantidad de posicionales,
                # lo inyectamos al inicio.
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
    
    def _resolve_min_bars_requirement(self) -> int:
        """Obtiene el mínimo de velas requerido para evaluar entradas."""

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
        """Controla que solo se evalúe una vez por vela cerrada."""

        if last_bar_ts is None:
            return True

        key = (symbol.upper(), str(timeframe or ""))
        prev = self._last_evaluated_bar.get(key)
        if prev == last_bar_ts:
            return False

        self._last_evaluated_bar[key] = last_bar_ts
        return True