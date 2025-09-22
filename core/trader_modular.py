*** a/core/trader_modular.py
--- b/core/trader_modular.py
@@
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
-    from core.supervisor import Supervisor
+    from core.supervisor import Supervisor
 except ModuleNotFoundError:  # pragma: no cover
     from supervisor import Supervisor  # compatibilidad retro
 
 # Cliente de exchange opcional (solo si operas en real)
 try:  # pragma: no cover
     from binance_api.cliente import crear_cliente
 except Exception:  # pragma: no cover
     crear_cliente = None  # seguirá funcionando en modo simulado
 
-from core.streams.candle_filter import CandleFilter
-from core.event_bus import EventBus
-from core.orders.order_manager import OrderManager
-from core.orders.storage_simulado import sincronizar_ordenes_simuladas
-from core.persistencia_tecnica import PersistenciaTecnica
-from core.risk import SpreadGuard
-from core.strategies import StrategyEngine
-from core.strategies.entry.verificar_entradas import verificar_entrada
+from core.streams.candle_filter import CandleFilter
+# ⚠️ Lazy imports de módulos pesados para evitar ciclos/latencia en arranque.
+# Se importan dentro de __init__ cuando realmente se usan.
 
 from core.utils.utils import configurar_logger
 
 if TYPE_CHECKING:  # pragma: no cover - solo para anotaciones
     from core.notification_manager import NotificationManager
 
 UTC = timezone.utc
 
 
 log = configurar_logger("trader_modular", modo_silencioso=True)
 
+def _is_awaitable(x: Any) -> bool:
+    return inspect.isawaitable(x) or asyncio.isfuture(x)
+
+async def _maybe_await(x: Any):
+    if _is_awaitable(x):
+        return await x
+    return x
+
 def _silence_task_result(task: asyncio.Task) -> None:
     """Consume resultados/errores de tareas lanzadas en segundo plano."""
 
     with contextlib.suppress(Exception):
         task.result()
@@
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
 
-        # Protección dinámica ante spreads amplios.
-        self.spread_guard: SpreadGuard | None = None
+        # Protección dinámica ante spreads amplios (lazy import abajo).
+        self.spread_guard: Any | None = None
 
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
@@
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
 
-        # Configurar guardia de spread (puede devolver None si está deshabilitado).
-        self.spread_guard = self._create_spread_guard()
+        # --- Lazy imports de módulos pesados / acoplados ---
+        # Se difieren hasta ahora para evitar ciclos.
+        try:
+            from core.risk import SpreadGuard as _SpreadGuard
+            self._SpreadGuard = _SpreadGuard
+        except Exception:
+            self._SpreadGuard = None
+        try:
+            from core.event_bus import EventBus as _EventBus
+            from core.orders.order_manager import OrderManager as _OrderManager
+            from core.orders.storage_simulado import sincronizar_ordenes_simuladas as _sync_sim
+            from core.strategies import StrategyEngine as _StrategyEngine
+            from core.persistencia_tecnica import PersistenciaTecnica as _PersistenciaTecnica
+            from core.strategies.entry.verificar_entradas import verificar_entrada as _verificar_entrada
+            self._EventBus = _EventBus
+            self._OrderManager = _OrderManager
+            self._sync_sim = _sync_sim
+            self._StrategyEngine = _StrategyEngine
+            self._PersistenciaTecnica = _PersistenciaTecnica
+            self._verificar_entrada = _verificar_entrada
+        except Exception:
+            # Se inicializarán a None; el wrapper Trader aplicará guardas.
+            self._EventBus = None
+            self._OrderManager = None
+            self._sync_sim = None
+            self._StrategyEngine = None
+            self._PersistenciaTecnica = None
+            self._verificar_entrada = None
+
+        # Configurar guardia de spread (puede devolver None si está deshabilitado).
+        self.spread_guard = self._create_spread_guard()
@@
     def start(self) -> None:
         """Arranca supervisor y la tarea principal del Trader."""
-        self.supervisor.start_supervision()
+        # Soporta start_supervision sync/async
+        try:
+            res = self.supervisor.start_supervision()
+            if _is_awaitable(res):
+                # Lanzar sin bloquear el hilo de arranque
+                asyncio.create_task(res, name="supervisor_start")
+        except Exception:
+            log.exception("Error iniciando supervisor (start_supervision)")
         if self._runner_task is None or self._runner_task.done():
-            self._runner_task = asyncio.create_task(self._run(), name="trader_main")
+            self._runner_task = asyncio.create_task(self._run(), name="trader_main")
@@
     async def stop(self) -> None:
         """Solicita parada ordenada y espera cierre del feed."""
         self._stop_event.set()
-        try:
-            await self.feed.detener()
-        except Exception:
-            pass
-        await self.supervisor.shutdown()
+        # Cierre del feed: soporta detener() sync/async/ausente
+        try:
+            detener = getattr(self.feed, "detener", None)
+            if callable(detener):
+                await _maybe_await(detener())
+        except Exception:
+            log.exception("Error deteniendo DataFeed")
+        # Cierre del supervisor: sync/async
+        try:
+            await _maybe_await(self.supervisor.shutdown())
+        except Exception:
+            log.exception("Error cerrando supervisor")
         if self._runner_task:
             with contextlib.suppress(Exception):
                 await self._runner_task
@@
-    def _create_spread_guard(self) -> SpreadGuard | None:
+    def _create_spread_guard(self) -> Any | None:
         """Instancia ``SpreadGuard`` cuando la configuración lo habilita."""
 
         base_limit = float(getattr(self.config, "max_spread_ratio", 0.0) or 0.0)
         dynamic_enabled = bool(getattr(self.config, "spread_dynamic", False))
         if not dynamic_enabled or base_limit <= 0:
             return None
@@
-        try:
-            guard = SpreadGuard(
+        try:
+            if self._SpreadGuard is None:
+                return None
+            guard = self._SpreadGuard(
                 base_limit=base_limit,
                 max_limit=max_limit,
                 window=window,
                 hysteresis=hysteresis,
             )
@@
-        signature = inspect.signature(handler)
+        signature = inspect.signature(handler)
         trader_aliases = {"trader", "bot", "manager"}
-        candle_aliases = ("vela", "candle", "kline", "bar", "candle_data")
+        candle_aliases = ("vela", "candle", "kline", "bar", "candle_data")
@@
-        first_positional_name = positional_params[0].name if positional_params else None
-        needs_trader = any(alias in signature.parameters for alias in trader_aliases)
-        if not needs_trader and len(positional_params) >= 2:
-            if first_positional_name not in {"vela", "candle", "kline", "bar", "candle_data"}:
-                needs_trader = True
+        first_positional_name = positional_params[0].name if positional_params else None
+        needs_trader = any(alias in signature.parameters for alias in trader_aliases)
+        # Heurística: si hay ≥2 posicionales y el primero NO parece ser la vela, asumimos (trader, vela)
+        if not needs_trader and len(positional_params) >= 2:
+            if first_positional_name not in candle_aliases:
+                needs_trader = True
@@
             if needs_trader and all(alias not in signature.parameters for alias in trader_aliases):
                 # Si inferimos que necesita trader por la cantidad de posicionales,
                 # lo inyectamos al inicio.
                 args.insert(0, self)
@@
 class Trader(TraderLite):
     """Wrapper ligero que expone la interfaz histórica del bot."""
@@
         self.modo_real = bool(getattr(config, "modo_real", False))
         self.cliente = self._cliente
         self.data_feed = self.feed
-        self.historial_cierres: Dict[str, dict] = {s: {} for s in config.symbols}
+        self.historial_cierres: Dict[str, dict] = {s: {} for s in config.symbols}
         self.fecha_actual = datetime.now(UTC).date()
         self.estrategias_habilitadas = False
         self._bg_tasks: set[asyncio.Task] = set()
         self.notificador: NotificationManager | None = None
-        self.bus = EventBus()
-        self.orders = OrderManager(self.modo_real, self.bus)
-        if not self.modo_real:
-            sincronizar_ordenes_simuladas(self.orders)
+        # Lazy construcciones (si los módulos existen)
+        if getattr(self, "_EventBus", None):
+            self.bus = self._EventBus()
+        else:
+            self.bus = None
+        if getattr(self, "_OrderManager", None) and self.bus is not None:
+            self.orders = self._OrderManager(self.modo_real, self.bus)
+            if not self.modo_real and getattr(self, "_sync_sim", None):
+                try:
+                    self._sync_sim(self.orders)
+                except Exception:
+                    log.exception("Fallo sincronizando órdenes simuladas")
+        else:
+            self.orders = None
@@
-        self.engine = StrategyEngine()
-        persistencia_min = int(getattr(config, "persistencia_minima", 1) or 1)
-        persistencia_extra = float(
-            getattr(config, "peso_extra_persistencia", 0.5) or 0.5
-        )
-        self.persistencia = PersistenciaTecnica(
-            minimo=persistencia_min, peso_extra=persistencia_extra
-        )
+        if getattr(self, "_StrategyEngine", None):
+            self.engine = self._StrategyEngine()
+        else:
+            self.engine = None
+        if getattr(self, "_PersistenciaTecnica", None):
+            persistencia_min = int(getattr(config, "persistencia_minima", 1) or 1)
+            persistencia_extra = float(
+                getattr(config, "peso_extra_persistencia", 0.5) or 0.5
+            )
+            try:
+                self.persistencia = self._PersistenciaTecnica(
+                    minimo=persistencia_min, peso_extra=persistencia_extra
+                )
+            except Exception:
+                self.persistencia = None
+                log.exception("No se pudo inicializar PersistenciaTecnica")
+        else:
+            self.persistencia = None
@@
     async def ejecutar(self) -> None:
         """Inicia el trader y espera hasta que finalice la tarea principal."""
 
         self.start()
         if self._runner_task is not None:
             await self._runner_task
@@
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
-        if hasattr(self, "bus"):
+        if getattr(self, "bus", None) is not None and hasattr(self.bus, "close"):
             with contextlib.suppress(Exception):
                 await self.bus.close()
@@
     async def evaluar_condiciones_de_entrada(
         self,
         symbol: str,
         df: pd.DataFrame,
         estado: Any,
     ) -> dict[str, Any] | None:
@@
-        try:
-            resultado = await verificar_entrada(
-                self,
-                symbol,
-                df,
-                estado,
-                on_event=on_event_cb,
-            )
+        try:
+            if getattr(self, "_verificar_entrada", None) is None:
+                log.warning("verificar_entrada no disponible; omitiendo evaluación")
+                return None
+            resultado = await self._verificar_entrada(
+                    self,
+                    symbol,
+                    df,
+                    estado,
+                    on_event=on_event_cb,
+            )

