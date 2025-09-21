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
from typing import Any, Awaitable, Callable, Deque, Dict, Iterable, List, Optional, TYPE_CHECKING

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
from core.event_bus import EventBus
from core.orders.order_manager import OrderManager
from core.orders.storage_simulado import sincronizar_ordenes_simuladas
from core.persistencia_tecnica import PersistenciaTecnica
from core.risk import SpreadGuard
from core.strategies import StrategyEngine
from core.strategies.entry.verificar_entradas import verificar_entrada
from core.utils.utils import configurar_logger

if TYPE_CHECKING:  # pragma: no cover - solo para anotaciones
    from core.notification_manager import NotificationManager

UTC = timezone.utc


log = configurar_logger("trader_modular", modo_silencioso=True)

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

        # Protección dinámica ante spreads amplios.
        self.spread_guard: SpreadGuard | None = None

        # Handler de velas
        self._handler = candle_handler or self._descubrir_handler_default()
        if not asyncio.iscoroutinefunction(self._handler):
            raise TypeError("candle_handler debe ser async (async def …)")
        self._handler_invoker = self._build_handler_invoker(self._handler)

        # DataFeedLite
        self.feed = DataFeed(
            config.intervalo_velas,
            handler_timeout=float(os.getenv("DF_HANDLER_TIMEOUT_SEC", "2.0")),
            inactivity_intervals=int(os.getenv("DF_INACTIVITY_INTERVALS", "10")),
            queue_max=int(os.getenv("DF_QUEUE_MAX", "2000")),
            queue_policy=os.getenv("DF_QUEUE_POLICY", "drop_oldest"),
            on_event=on_event,
        )
        # Señal para el StartupManager: este feed se arranca desde TraderLite
        # (vía Supervisor) y no debe iniciarse automáticamente antes de que
        # el trader configure callbacks y símbolos.
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

        # Configurar guardia de spread (puede devolver None si está deshabilitado).
        self.spread_guard = self._create_spread_guard()

    # -------------------- API pública --------------------
    def start(self) -> None:
        """Arranca supervisor y la tarea principal del Trader."""
        self.supervisor.start_supervision()
        if self._runner_task is None or self._runner_task.done():
            self._runner_task = asyncio.create_task(self._run(), name="trader_main")

    async def stop(self) -> None:
        """Solicita parada ordenada y espera cierre del feed."""
        self._stop_event.set()
        try:
            await self.feed.detener()
        except Exception:
            pass
        await self.supervisor.shutdown()
        if self._runner_task:
            with contextlib.suppress(Exception):
                await self._runner_task

    # ------------------- utilidades internas -------------------
    def _create_spread_guard(self) -> SpreadGuard | None:
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
            guard = SpreadGuard(
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
            # Actualiza estado mínimo y delega
            sym = c.get("symbol")
            if sym in self.estado:
                est = self.estado[sym]
                est.ultimo_timestamp = c.get("timestamp", est.ultimo_timestamp)
                est.buffer.append(c)
            await self._handler_invoker(c)

        # Registrar latidos periódicos
        self.supervisor.supervised_task(lambda: self._heartbeat_loop(), name="heartbeat_loop", expected_interval=60)

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
        Si no, levanta un error claro.
        """
        try:  # pragma: no cover
            from core.procesar_vela import procesar_vela  # tu pipeline existente
            if asyncio.iscoroutinefunction(procesar_vela):
                return procesar_vela
        except Exception:
            pass
        # Fallback: obliga a inyectar un handler explícito
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
        if not needs_trader and len(positional_params) >= 2:
            if first_positional_name not in {"vela", "candle", "kline", "bar", "candle_data"}:
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
        self.bus = EventBus()
        self.orders = OrderManager(self.modo_real, self.bus)
        if not self.modo_real:
            sincronizar_ordenes_simuladas(self.orders)
        # Registro opcional de ventanas de cooldown por símbolo para entradas nuevas.
        self._entrada_cooldowns: Dict[str, datetime] = {}
        self.engine = StrategyEngine()
        persistencia_min = int(getattr(config, "persistencia_minima", 1) or 1)
        persistencia_extra = float(
            getattr(config, "peso_extra_persistencia", 0.5) or 0.5
        )
        self.persistencia = PersistenciaTecnica(
            minimo=persistencia_min, peso_extra=persistencia_extra
        )
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
        if hasattr(self, "bus"):
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
            # Ante cualquier inconsistencia, preferimos continuar para no romper el flujo.
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

        Parameters
        ----------
        symbol:
            Par de trading a evaluar (por ejemplo ``"BTC/EUR"``).
        df:
            DataFrame OHLCV con la ventana más reciente de velas.
        estado:
            Estado interno asociado al símbolo (buffers, caches, etc.).

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
            resultado = await verificar_entrada(
                self,
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
        else:
            log.debug("[%s] Sin condiciones de entrada válidas", symbol)
        return resultado

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


# Nota: si lo deseas, más adelante extraeremos EstadoSimbolo a `estado_simbolo.py`
# y añadiremos conectores a `gestor_capital`, `gestor_ordenes`, `gestor_warmup`, etc.
