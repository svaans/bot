"""
TraderLite — Núcleo limpio y mínimo para orquestar el bot.

Objetivo
--------
- Arrancar/parar el flujo de velas y delegar el procesamiento en un handler claro.
- Mantener un estado sencillo por símbolo (buffer y último timestamp).
- Integrarse con SupervisorLite para watchdog/heartbeat sin acoplarse a métricas externas.
- Evitar dependencias circulares con módulos gigantes (riesgo, capital, reporting, etc.).

Qué hace (MVP)
--------------
- Crea/usa un cliente de exchange si `modo_real=True`.
- Lanza DataFeedLite (combinado o por símbolo) y procesa solo velas cerradas.
- Mantiene un buffer fixed-size por símbolo para consumidores posteriores.
- Expone hooks `on_event` para métricas/notificaciones (opcional).
- Permite inyectar un `candle_handler` (async) para tu pipeline de estrategias.

Qué NO hace (porque habrá módulos dedicados)
-------------------------------------------
- Warmup/históricos, aprendizaje continuo, redistribución de capital (Kelly), fast-path,
  reporting/auditoría, gestión completa de órdenes.

Dependencias previstas
----------------------
- DataFeedLite (archivo ya entregado):
    from data_feed_lite import DataFeedLite
  Si renombraste el archivo a `core/data.py` o similar, ajusta el import abajo.
- SupervisorLite (archivo ya entregado):
    from supervisor_lite import SupervisorLite

Integración con tu proyecto
---------------------------
- Puedes pasar `candle_handler`=core.procesar_vela.procesar_vela si quieres reutilizar tu lógica.
- `on_event(evt, data)` te permite cablear Prometheus/Telegram sin acoplar TraderLite.
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, TYPE_CHECKING

import pandas as pd

# Imports tolerantes a ruta (ajusta según tu repo)
try:
    # Import DataFeed from data_feed.lite after relocation
    from data_feed.lite import DataFeed
except ModuleNotFoundError:  # pragma: no cover
    from data_feed import DataFeed  # compatibilidad retro

try:
    from core.supervisor import Supervisor
except ModuleNotFoundError:  # pragma: no cover
    from supervisor import Supervisor  # compatibilidad retro

# Cliente de exchange opcional (solo si operas en real)
try:  # pragma: no cover
    from binance_api.cliente import crear_cliente
except Exception:  # pragma: no cover
    crear_cliente = None  # seguirá funcionando en modo simulado

from core.streams.candle_filter import CandleFilter
# Módulos pesados en lazy import dentro de __init__ para evitar ciclos/latencia

from core.utils.utils import configurar_logger

if TYPE_CHECKING:  # pragma: no cover - solo para anotaciones
    from core.notification_manager import NotificationManager

UTC = timezone.utc
log = configurar_logger("trader_modular", modo_silencioso=True)


def _is_awaitable(x: Any) -> bool:
    return inspect.isawaitable(x) or asyncio.isfuture(x)


async def _maybe_await(x: Any):
    if _is_awaitable(x):
        return await x
    return x


def _silence_task_result(task: asyncio.Task) -> None:
    """Consume resultados/errores de tareas lanzadas en segundo plano."""
    with contextlib.suppress(Exception):
        task.result()


def _max_buffer_velas() -> int:
    """Obtiene el tamaño máximo del buffer de velas desde variables de entorno."""
    return int(os.getenv("MAX_BUFFER_VELAS", "300"))


def _max_estrategias_buffer() -> int:
    """Determina el tamaño máximo del buffer para resultados de estrategias."""
    return int(os.getenv("MAX_ESTRATEGIAS_BUFFER", str(_max_buffer_velas())))


@dataclass
class EstadoSimbolo:
    """Estado mínimo por símbolo.

    Mantén este dataclass liviano. Si más tarde necesitas métricas o caches,
    muévelas a un módulo aparte (p. ej. gestor_metricas.py).
    """
    buffer: Deque[dict] = field(default_factory=lambda: deque(maxlen=_max_buffer_velas()))
    estrategias_buffer: Deque[dict] = field(
        default_factory=lambda: deque(maxlen=_max_estrategias_buffer())
    )
    ultimo_timestamp: Optional[int] = None
    candle_filter: CandleFilter = field(default_factory=CandleFilter)
    indicadores_cache: Dict[str, dict[str, float | None]] = field(default_factory=dict)


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
        self.supervisor = supervisor or Supervisor(on_event=on_event)

        # Estado por símbolo
        self.estado: Dict[str, EstadoSimbolo] = {s: EstadoSimbolo() for s in config.symbols}
        # Registro de tendencia por símbolo (utilizado por procesar_vela)
        self.estado_tendencia: Dict[str, Any] = {s: None for s in config.symbols}

        # Protección dinámica ante spreads amplios (lazy import abajo).
        self.spread_guard: Any | None = None

        # Handler de velas
        self._handler = candle_handler or self._descubrir_handler_default()
        if not asyncio.iscoroutinefunction(self._handler):
            raise TypeError("candle_handler debe ser async (async def …)")
        self._handler_invoker = self._build_handler_invoker(self._handler)

        # DataFeedLite
        self.feed = DataFeed(
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
        if bool(getattr(config, "modo_real", False)) and crear_cliente is not None:
            try:
                self._cliente = crear_cliente(config)
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


class Trader(TraderLite):
    """Wrapper ligero que expone la interfaz histórica del bot."""

    def __init__(
        self,
        config: Any,
        *,
        candle_handler: Optional[Callable[[dict], Awaitable[None]]] = None,
        on_event: Optional[Callable[[str, dict], None]] = None,
        supervisor: Optional[Supervisor] = None,
    ) -> None:
        super().__init__(
            config,
            candle_handler=candle_handler,
            on_event=on_event,
            supervisor=supervisor,
        )
        self.modo_real = bool(getattr(config, "modo_real", False))
        self.cliente = self._cliente
        self.data_feed = self.feed
        self.historial_cierres: Dict[str, dict] = {s: {} for s in config.symbols}
        self.fecha_actual = datetime.now(UTC).date()
        self.estrategias_habilitadas = False
        self._bg_tasks: set[asyncio.Task] = set()
        self.notificador: NotificationManager | None = None
        self._verificar_entrada_provider: str | None = None

        # Lazy construcciones (si los módulos existen)
        if getattr(self, "_EventBus", None):
            self.bus = self._EventBus()
        else:
            self.bus = None

        if getattr(self, "_OrderManager", None) and getattr(self, "bus", None) is not None:
            self.orders = self._OrderManager(self.modo_real, self.bus)
            if not self.modo_real and getattr(self, "_sync_sim", None):
                try:
                    self._sync_sim(self.orders)
                except Exception:
                    log.exception("Fallo sincronizando órdenes simuladas")
        else:
            self.orders = None

        if getattr(self, "_StrategyEngine", None):
            self.engine = self._StrategyEngine()
        else:
            self.engine = None

        if getattr(self, "_PersistenciaTecnica", None):
            persistencia_min = int(getattr(config, "persistencia_minima", 1) or 1)
            persistencia_extra = float(getattr(config, "peso_extra_persistencia", 0.5) or 0.5)
            try:
                self.persistencia = self._PersistenciaTecnica(
                    minimo=persistencia_min, peso_extra=persistencia_extra
                )
            except Exception:
                self.persistencia = None
                log.exception("No se pudo inicializar PersistenciaTecnica")
        else:
            self.persistencia = None

        # Registro opcional de ventanas de cooldown por símbolo para entradas nuevas.
        self._entrada_cooldowns: Dict[str, datetime] = {}
        # Config por símbolo
        self.config_por_simbolo: Dict[str, dict] = {}
        config_pesos = getattr(config, "pesos_por_simbolo", None)
        if isinstance(config_pesos, dict):
            self.pesos_por_simbolo = {
                symbol: dict(config_pesos.get(symbol, {})) for symbol in config.symbols
            }
        else:
            self.pesos_por_simbolo = {symbol: {} for symbol in config.symbols}

    async def ejecutar(self) -> None:
        """Inicia el trader y espera hasta que finalice la tarea principal."""
        self.start()
        if self._runner_task is not None:
            await self._runner_task

    async def cerrar(self) -> None:
        """Detiene el trader y limpia tareas en segundo plano."""
        await self.stop()
        while self._bg_tasks:
            task = self._bg_tasks.pop()
            if task.done():
                continue
            task.cancel()
            with contextlib.suppress(Exception):
                await task
        if getattr(self, "bus", None) is not None and hasattr(self.bus, "close"):
            with contextlib.suppress(Exception):
                await self.bus.close()

    def solicitar_parada(self) -> None:
        """Señala al trader que debe detenerse en cuanto sea posible."""
        self._stop_event.set()

    async def _precargar_historico(self, velas: int | None = None) -> None:
        """Realiza un backfill inicial antes de abrir streams."""
        await self.feed.precargar(self.config.symbols, cliente=self._cliente, minimo=velas)

    def habilitar_estrategias(self) -> None:
        """Marca las estrategias como habilitadas (bandera de compatibilidad)."""
        self.estrategias_habilitadas = True

    def ajustar_capital_diario(self, *, fecha: Optional[Any] = None) -> None:
        """Actualiza la fecha de referencia utilizada por el capital manager."""
        target = fecha or datetime.now(UTC).date()
        self.fecha_actual = target

    def _puede_evaluar_entradas(self, symbol: str) -> bool:
        """Determina si ``symbol`` puede ser evaluado para una nueva entrada."""
        if getattr(self, "_stop_event", None) and self._stop_event.is_set():
            log.debug("[%s] Entrada bloqueada: stop solicitado", symbol)
            return False

        # Evitar duplicados cuando ya existe una orden o se está ejecutando la apertura.
        try:
            ordenes = getattr(self, "orders", None)
            if ordenes is not None:
                obtener_orden = getattr(ordenes, "obtener", None)
                if callable(obtener_orden):
                    orden_existente = obtener_orden(symbol)
                    if orden_existente is not None:
                        log.debug("[%s] Entrada bloqueada: orden existente", symbol)
                        return False
                abriendo = getattr(ordenes, "abriendo", None)
                if isinstance(abriendo, set) and symbol in abriendo:
                    log.debug("[%s] Entrada bloqueada: apertura en curso", symbol)
                    return False
        except Exception:
            log.warning("[%s] Error inspeccionando estado de órdenes", symbol, exc_info=True)

        # Respeta ventanas de cooldown externas (por ejemplo tras pérdidas consecutivas).
        cooldowns = getattr(self, "_entrada_cooldowns", None)
        if isinstance(cooldowns, dict):
            ventana = cooldowns.get(symbol)
            ahora = datetime.now(UTC)
            if isinstance(ventana, datetime) and ventana > ahora:
                log.debug(
                    "[%s] Entrada bloqueada: cooldown activo hasta %s",
                    symbol,
                    ventana.isoformat(),
                )
                return False
            if isinstance(ventana, (int, float)) and ventana > ahora.timestamp():
                log.debug(
                    "[%s] Entrada bloqueada: cooldown activo (timestamp %.0f)",
                    symbol,
                    ventana,
                )
                return False

        capital_manager = getattr(self, "capital_manager", None)
        if capital_manager is not None:
            try:
                if hasattr(capital_manager, "hay_capital_libre") and not capital_manager.hay_capital_libre():
                    log.debug("[%s] Entrada bloqueada: sin capital libre", symbol)
                    return False
                if hasattr(capital_manager, "tiene_capital") and not capital_manager.tiene_capital(symbol):
                    log.debug("[%s] Entrada bloqueada: sin capital asignado", symbol)
                    return False
            except Exception:
                log.warning("[%s] Error consultando capital disponible", symbol, exc_info=True)

        risk = getattr(self, "risk", None)
        if risk is not None and hasattr(risk, "permite_entrada"):
            try:
                correlaciones = {}
                if hasattr(risk, "correlaciones"):
                    correlaciones = risk.correlaciones.get(symbol, {})
                diversidad_minima = float(getattr(self.config, "diversidad_minima", 0.0) or 0.0)
                if not risk.permite_entrada(symbol, correlaciones, diversidad_minima):
                    log.debug("[%s] Entrada bloqueada por gestor de riesgo", symbol)
                    return False
            except Exception:
                log.warning("[%s] Error consultando gestor de riesgo", symbol, exc_info=True)

        return True

    async def evaluar_condiciones_de_entrada(
        self,
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
    ) -> dict[str, Any] | None:
        """Evalúa condiciones de entrada delegando en el pipeline modular.

        Returns
        -------
        dict[str, Any] | None
            Diccionario con la propuesta de entrada listo para `_abrir_operacion_real`
            o ``None`` si no se cumplieron las condiciones.
        """
        if not self.estrategias_habilitadas:
            log.debug("[%s] Estrategias deshabilitadas; entrada omitida", symbol)
            return None
        if not isinstance(df, pd.DataFrame) or df.empty:
            log.warning("[%s] DataFrame inválido al evaluar entrada", symbol)
            return None

        on_event_cb: Callable[[str, dict], None] | None = None
        if hasattr(self, "_emit"):
            on_event_cb = self._emit
        elif callable(self.on_event):
            on_event_cb = self.on_event

        try:
            resultado = await self.verificar_entrada(
                symbol,
                df,
                estado,
                on_event=on_event_cb,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("❌ Error evaluando condiciones de entrada para %s", symbol)
            return None

        if resultado:
            log.debug("[%s] Entrada candidata generada", symbol)
            return resultado

        provider = getattr(self, "_verificar_entrada_provider", None)
        if provider is None:
            log.warning(
                "verificar_entrada devolvió None; omitiendo evaluación",
                extra={"symbol": symbol, "timeframe": getattr(df, "tf", None)},
            )
        else:
            log.debug("[%s] Sin condiciones de entrada válidas", symbol)
        return None

    async def verificar_entrada(
        self,
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
        *,
        on_event: Callable[[str, dict], None] | None = None,
    ) -> dict[str, Any] | None:
        """Punto único para decidir entradas.

        Siempre existe para evitar advertencias por interfaces faltantes.
        Devuelve ``None`` cuando no hay pipeline disponible o la estrategia no
        produce una propuesta.
        """

        pipeline = getattr(self, "_verificar_entrada", None)
        if callable(pipeline):
            self._verificar_entrada_provider = "pipeline"
            return await pipeline(
                self,
                symbol,
                df,
                estado,
                on_event=on_event,
            )

        engine = getattr(self, "engine", None)
        if engine is not None:
            for attr in ("verificar_entrada", "evaluar_condiciones_de_entrada"):
                fn = getattr(engine, attr, None)
                if not callable(fn):
                    continue

                self._verificar_entrada_provider = f"engine.{attr}"

                attempts = []
                if on_event is not None:
                    attempts.append(lambda: fn(symbol, df, estado, on_event=on_event))
                attempts.append(lambda: fn(symbol, df, estado))
                if on_event is not None:
                    attempts.append(
                        lambda: fn(self, symbol, df, estado, on_event=on_event)
                    )
                attempts.append(lambda: fn(self, symbol, df, estado))
                if on_event is not None:
                    attempts.append(lambda: fn(symbol, df, on_event=on_event))
                    attempts.append(lambda: fn(self, symbol, df, on_event=on_event))
                attempts.append(lambda: fn(symbol, df))
                attempts.append(lambda: fn(self, symbol, df))

                for attempt in attempts:
                    try:
                        result = attempt()
                    except TypeError:
                        continue
                    return await _maybe_await(result)

        self._verificar_entrada_provider = None
        return None

    # Compat helpers -------------------------------------------------------
    def enqueue_notification(self, mensaje: str, nivel: str = "INFO") -> None:
        if self.on_event:
            try:
                self.on_event("notify", {"mensaje": mensaje, "nivel": nivel})
            except Exception:
                pass

        manager = getattr(self, "notificador", None)
        if manager is None:
            return

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            with contextlib.suppress(Exception):
                manager.enviar(mensaje, nivel)
            return

        task = asyncio.create_task(manager.enviar_async(mensaje, nivel))
        task.add_done_callback(_silence_task_result)

    def enqueue_persistence(self, tipo: str, datos: dict, *, immediate: bool = False) -> None:
        if self.on_event:
            payload = {"tipo": tipo, "datos": datos, "inmediato": immediate}
            try:
                self.on_event("persistencia", payload)
            except Exception:
                pass

