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
import os
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Deque, Dict, Iterable, List, Optional
from collections import deque
from datetime import timezone
import contextlib

# Imports tolerantes a ruta (ajusta según tu repo)
try:
    from data_feed import DataFeed  # canvas entregado
except Exception:  # pragma: no cover
    from data_feed import DataFeed # si lo integraste en tu módulo existente

try:
    from supervisor import Supervisor  # canvas entregado
except Exception:  # pragma: no cover
    from supervisor import Supervisor # si lo integraste con este nombre

# Cliente de exchange opcional (solo si operas en real)
try:  # pragma: no cover
    from binance_api.cliente import crear_cliente
except Exception:  # pragma: no cover
    crear_cliente = None  # seguirá funcionando en modo simulado

UTC = timezone.utc


@dataclass
class EstadoSimbolo:
    """Estado mínimo por símbolo.

    Mantén este dataclass liviano. Si más tarde necesitas métricas o caches,
    muévelas a un módulo aparte (p. ej. gestor_metricas.py).
    """
    buffer: Deque[dict] = field(default_factory=lambda: deque(maxlen=int(os.getenv("MAX_BUFFER_VELAS", "300"))))
    ultimo_timestamp: Optional[int] = None


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

        # Handler de velas
        self._handler = candle_handler or self._descubrir_handler_default()
        if not asyncio.iscoroutinefunction(self._handler):
            raise TypeError("candle_handler debe ser async (async def …)")

        # DataFeedLite
        self.feed = DataFeed(
            config.intervalo_velas,
            handler_timeout=float(os.getenv("DF_HANDLER_TIMEOUT_SEC", "2.0")),
            inactivity_intervals=int(os.getenv("DF_INACTIVITY_INTERVALS", "10")),
            queue_max=int(os.getenv("DF_QUEUE_MAX", "2000")),
            queue_policy=os.getenv("DF_QUEUE_POLICY", "drop_oldest"),
            on_event=on_event,
        )

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
            await self._handler(c)

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


# Nota: si lo deseas, más adelante extraeremos EstadoSimbolo a `estado_simbolo.py`
# y añadiremos conectores a `gestor_capital`, `gestor_ordenes`, `gestor_warmup`, etc.
