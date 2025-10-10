"""Persistencia segura y asíncrona del estado de riesgo."""

from __future__ import annotations

import atexit
import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from queue import Empty, SimpleQueue
from typing import Dict, Optional

from filelock import FileLock

from core.utils.utils import configurar_logger

log = configurar_logger('riesgo')
RUTA_ESTADO = 'config/riesgo.json'
RUTA_ESTADO_BAK = RUTA_ESTADO + '.bak'
_LOCK_PATH = RUTA_ESTADO + '.lock'


def cargar_estado_riesgo_seguro() ->dict:
    """Lee ``riesgo.json`` de forma segura usando bloqueo de archivo."""
    lock = FileLock(_LOCK_PATH)
    with lock:
        if not os.path.exists(RUTA_ESTADO):
            if os.path.exists(RUTA_ESTADO_BAK):
                try:
                    with open(RUTA_ESTADO_BAK, 'r') as f:
                        return json.load(f)
                except Exception as e:  # pragma: no cover - error de lectura
                    log.warning(f'⚠️ Backup corrupto: {e}')
            return {'fecha': '', 'perdida_acumulada': 0.0}
        try:
            with open(RUTA_ESTADO, 'r') as f:
                estado = json.load(f)
            if not isinstance(estado, dict):
                raise ValueError('❌ Formato inválido en estado de riesgo.')
            return estado
        except (OSError, json.JSONDecodeError) as e:
            log.warning(f'⚠️ Error al cargar estado de riesgo: {e}')
            if os.path.exists(RUTA_ESTADO_BAK):
                try:
                    with open(RUTA_ESTADO_BAK, 'r') as f:
                        return json.load(f)
                except Exception as e2:
                    log.warning(f'⚠️ Backup también falló: {e2}')
            return {'fecha': '', 'perdida_acumulada': 0.0}


def cargar_estado_riesgo() ->dict:
    """Compatibilidad retro: delega en :func:`cargar_estado_riesgo_seguro`."""
    return cargar_estado_riesgo_seguro()


def guardar_estado_riesgo_seguro(estado: dict) ->None:
    """Guarda ``estado`` en ``riesgo.json`` de forma atómica y segura."""
    lock = FileLock(_LOCK_PATH)
    try:
        with lock, open(RUTA_ESTADO, 'w') as f:
            json.dump(estado, f, indent=4)
        try:
            with open(RUTA_ESTADO_BAK, 'w') as fb:
                json.dump(estado, fb, indent=4)
        except OSError as e:
            log.warning(f'⚠️ No se pudo escribir backup: {e}')
        log.info(f'💾 Estado de riesgo actualizado: {estado}')
    except OSError as e:
        log.error(f'❌ No se pudo guardar estado de riesgo: {e}')
        raise


def guardar_estado_riesgo(estado: dict) ->None:
    """Compatibilidad retro: delega en :func:`guardar_estado_riesgo_seguro`."""
    guardar_estado_riesgo_seguro(estado)


@dataclass(slots=True)
class _LossUpdate:
    simbolo: str
    perdida: float


class AsyncRiskPersistence:
    """Administra la escritura en disco del estado de riesgo.

    Usa una cola en memoria y un hilo dedicado para reducir la contención
    de ``FileLock`` cuando múltiples procesos/threads informan pérdidas.
    """

    def __init__(self, flush_interval: float = 0.5) -> None:
        self._flush_interval = flush_interval
        self._queue: SimpleQueue[_LossUpdate] = SimpleQueue()
        self._stop_event = threading.Event()
        self._pending_event = threading.Event()
        self._lock = threading.Lock()
        self._estado_cache: dict = cargar_estado_riesgo_seguro()
        self._thread = threading.Thread(target=self._run, daemon=True, name='risk-persistence')
        self._thread.start()

    def registrar_perdida(self, simbolo: str, perdida: float) -> None:
        """Coloca una pérdida en cola para persistirla de forma asíncrona."""
        if perdida >= 0:
            return
        self._queue.put(_LossUpdate(simbolo=simbolo, perdida=abs(perdida)))

    def estado_actual(self) -> dict:
        """Devuelve una copia del estado cacheado."""
        with self._lock:
            return dict(self._estado_cache)

    def esperar_flush(self, timeout: float = 2.0) -> None:
        """Bloquea hasta que la cola esté vacía y el estado haya sido persistido."""
        limite = time.monotonic() + timeout
        while time.monotonic() < limite:
            if self._queue.empty() and not self._pending_event.is_set():
                return
            time.sleep(0.01)
        raise TimeoutError('No se pudo vaciar la cola de riesgo a tiempo.')

    def shutdown(self, timeout: float = 2.0) -> None:
        """Detiene el hilo de persistencia asegurando el flush final."""
        self._stop_event.set()
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            log.warning('⚠️ Persistencia de riesgo no se detuvo a tiempo.')

    def _run(self) -> None:
        pendientes: Dict[str, float] = {}
        ultimo_flush = time.monotonic()
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                update = self._queue.get(timeout=0.2)
                self._pending_event.set()
                pendientes[update.simbolo] = pendientes.get(update.simbolo, 0.0) + update.perdida
                self._aplicar_perdida(update.perdida)
            except Empty:
                pass
            ahora = time.monotonic()
            if self._pending_event.is_set() and (
                self._stop_event.is_set() or (ahora - ultimo_flush) >= self._flush_interval
            ):
                self._flush_estado(pendientes)
                pendientes.clear()
                ultimo_flush = ahora
        if self._pending_event.is_set():
            self._flush_estado(pendientes)

    def _aplicar_perdida(self, perdida: float) -> None:
        hoy = datetime.now(UTC).date().isoformat()
        with self._lock:
            if self._estado_cache.get('fecha') != hoy:
                self._estado_cache = {'fecha': hoy, 'perdida_acumulada': 0.0}
            self._estado_cache['perdida_acumulada'] += perdida

    def _flush_estado(self, pendientes: Dict[str, float]) -> None:
        with self._lock:
            estado = dict(self._estado_cache)
        guardar_estado_riesgo_seguro(estado)
        if pendientes:
            log.info(
                '📉 Perdidas persistidas | %s | Total día: %.2f',
                ', '.join(f'{simbolo}={valor:.2f}' for simbolo, valor in pendientes.items()),
                estado.get('perdida_acumulada', 0.0),
            )
        self._pending_event.clear()


_PERSISTENCIA: Optional[AsyncRiskPersistence] = None


def _obtener_persistencia() -> AsyncRiskPersistence:
    global _PERSISTENCIA
    if _PERSISTENCIA is None:
        _PERSISTENCIA = AsyncRiskPersistence()
        atexit.register(detener_persistencia)
    return _PERSISTENCIA


def detener_persistencia(timeout: float = 2.0) -> None:
    """Detiene la persistencia global (útil en tests)."""
    global _PERSISTENCIA
    if _PERSISTENCIA is not None:
        _PERSISTENCIA.shutdown(timeout=timeout)
        _PERSISTENCIA = None


def esperar_persistencia(timeout: float = 2.0) -> None:
    """Bloquea hasta que la cola de persistencia se vacíe."""
    if _PERSISTENCIA is not None:
        _PERSISTENCIA.esperar_flush(timeout=timeout)


def actualizar_perdida(simbolo: str, perdida: float) -> None:
    """Registra una pérdida para el día actual de forma asíncrona."""
    _obtener_persistencia().registrar_perdida(simbolo, perdida)


def riesgo_superado(umbral: float, capital_total: float) ->bool:
    """Evalúa si el umbral de pérdida diaria ha sido superado."""
    estado = _obtener_persistencia().estado_actual()
    hoy = datetime.now(UTC).date().isoformat()
    if estado.get('fecha') != hoy:
        return False
    if capital_total <= 0:
        log.warning('⚠️ Capital total es 0. No se puede evaluar riesgo.')
        return False
    porcentaje_perdido = estado['perdida_acumulada'] / capital_total
    log.debug(
        f'Evaluación de riesgo: {porcentaje_perdido:.2%} perdido (umbral: {umbral:.2%})'
        )
    return porcentaje_perdido >= umbral
