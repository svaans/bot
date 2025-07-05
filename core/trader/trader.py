"""Trader: controlador principal del bot modular (núcleo de coordinación)"""

from __future__ import annotations
import asyncio
from typing import Callable, Awaitable
from config.config_manager import Config
from core.data import DataFeed
from core.strategies import StrategyEngine
from core.risk import RiskManager
from core.notification_manager import NotificationManager
from core.position_manager import PositionManager
from binance_api.cliente import crear_cliente
from core.utils.utils import configurar_logger
from core.contexto_externo import StreamContexto
from core.orders import real_orders
from core.capital_manager import CapitalManager
from core.data import PersistenciaTecnica
from .gestion_estado import cargar_estado_persistente
from .gestion_estado import guardar_estado_persistente
from .tareas import precargar_historico
from core.adaptador_configuracion_dinamica import adaptar_configuracion
from core.adaptador_dinamico import adaptar_configuracion as adaptar_configuracion_base
from learning.aprendizaje_continuo import ejecutar_ciclo as ciclo_aprendizaje
from core.strategies.exit.verificar_salidas import verificar_salidas
from core.procesar_vela import procesar_vela
from core.monitor_estado_bot import monitorear_estado_periodicamente
from core.reporting import reporter_diario
from ccxt.base.errors import BaseError
import os

log = configurar_logger('trader')

class Trader:
    """Orquesta el flujo de datos y las operaciones de trading."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.data_feed = DataFeed(config.intervalo_velas)
        self.engine = StrategyEngine()
        self.risk = RiskManager(config.umbral_riesgo_diario)
        self.notificador = NotificationManager(config.telegram_token, config.telegram_chat_id)
        self.modo_real = getattr(config, 'modo_real', False)
        self.orders = PositionManager(self.modo_real, self.risk, self.notificador)
        self.cliente = crear_cliente(config) if self.modo_real else None
        if not self.modo_real:
            log.info('🧪 Modo simulado activado. No se inicializará cliente Binance')
        self._markets = None
        self.modo_capital_bajo = config.modo_capital_bajo
        self.persistencia = PersistenciaTecnica(config.persistencia_minima, config.peso_extra_persistencia)
        os.makedirs('logs/rechazos', exist_ok=True)
        os.makedirs(os.path.dirname(config.registro_tecnico_csv), exist_ok=True)
        self.umbral_score_tecnico = config.umbral_score_tecnico
        self.usar_score_tecnico = getattr(config, 'usar_score_tecnico', True)
        self.contradicciones_bloquean_entrada = config.contradicciones_bloquean_entrada
        self.registro_tecnico_csv = config.registro_tecnico_csv
        self.historicos = {}
        self.capital_manager = CapitalManager(config, self.cliente, self.risk, 1.0)
        self.capital_por_simbolo = self.capital_manager.capital_por_simbolo
        self.capital_inicial_diario = self.capital_manager.capital_inicial_diario
        self.reservas_piramide = self.capital_manager.reservas_piramide
        self.fecha_actual = self.capital_manager.fecha_actual
        self.estado = {s: None for s in config.symbols}
        self.estado_tendencia = {}
        self.config_por_simbolo = {s: {} for s in config.symbols}
        self.historial_cierres = {}
        self._tareas: dict[str, asyncio.Task] = {}
        self._factories: dict[str, Callable[[], Awaitable]] = {}
        self._stop_event = asyncio.Event()
        self._cerrado = False
        self.context_stream = StreamContexto()
        try:
            self.orders.ordenes = real_orders.obtener_todas_las_ordenes()
            if self.modo_real and not self.orders.ordenes:
                self.orders.ordenes = real_orders.sincronizar_ordenes_binance(config.symbols)
        except Exception as e:
            log.warning(f'⚠️ Error cargando órdenes previas desde la base de datos: {e}')
            raise
        if self.orders.ordenes:
            log.warning('⚠️ Órdenes abiertas encontradas al iniciar. Serán monitoreadas.')
        if 'PYTEST_CURRENT_TEST' not in os.environ:
            cargar_estado_persistente(self)
        else:
            log.debug('🔍 Modo prueba: se omite carga de estado persistente')

    async def ejecutar(self) -> None:
        """Inicia el procesamiento de todos los símbolos."""

        async def handle(candle: dict) -> None:
            await self._procesar_vela(candle)

        async def handle_context(symbol: str, score: float) -> None:
            log.debug(f'🔁 Contexto actualizado {symbol}: {score:.2f}')

        symbols = list(self.estado.keys())
        await precargar_historico(velas=60)
        tareas = {
            'data_feed': lambda: self.data_feed.escuchar(symbols, handle),
            'estado': lambda: monitorear_estado_periodicamente(self),
            'context_stream': lambda: self.context_stream.escuchar(symbols, handle_context),
            'flush': lambda: real_orders.flush_periodico()
        }
        if 'PYTEST_CURRENT_TEST' not in os.environ:
            tareas['aprendizaje'] = lambda: ciclo_aprendizaje()

        for nombre, factory in tareas.items():
            self._iniciar_tarea(nombre, factory)
        self._iniciar_tarea('heartbeat', lambda: self._vigilancia_tareas())
        await self._stop_event.wait()

    async def cerrar(self) -> None:
        self._cerrado = True
        self._stop_event.set()
        for nombre, tarea in list(self._tareas.items()):
            if nombre == 'data_feed':
                await self.data_feed.detener()
            if nombre == 'context_stream':
                await self.context_stream.detener()
            tarea.cancel()
        await asyncio.gather(*self._tareas.values(), return_exceptions=True)
        guardar_estado_persistente(self)

    async def _procesar_vela(self, vela: dict) -> None:
        symbol = vela.get('symbol')
        if not self._validar_config(symbol):
            return
        await procesar_vela(self, vela)

    def _iniciar_tarea(self, nombre: str, factory: Callable[[], Awaitable]) -> None:
        self._factories[nombre] = factory
        self._tareas[nombre] = asyncio.create_task(factory())

    async def _vigilancia_tareas(self, intervalo: int = 60) -> None:
        while not self._cerrado:
            activos = 0
            for nombre, task in list(self._tareas.items()):
                if task.done():
                    exc = task.exception()
                    if exc:
                        log.warning(f'⚠️ Heartbeat: tarea {nombre} terminó con error: {exc}')
                        self._iniciar_tarea(nombre, self._factories[nombre])
                    elif task.cancelled():
                        log.info(f'🟡 Heartbeat: tarea {nombre} fue cancelada manualmente')
                    else:
                        activos += 1
                else:
                    activos += 1
            log.info(f'🟢 Heartbeat: tareas activas {activos}/{len(self._tareas)}')
            await asyncio.sleep(intervalo)

    def _validar_config(self, symbol: str) -> bool:
        cfg = self.config_por_simbolo.get(symbol)
        if not isinstance(cfg, dict):
            log.error(f'⚠️ Configuración no encontrada para {symbol}')
            return False
        return True