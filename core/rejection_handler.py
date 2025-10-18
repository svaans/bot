from __future__ import annotations

"""Módulo para gestionar rechazos de operaciones y su registro."""

import asyncio
import os
import time
from datetime import datetime, timezone

from typing import Callable, Dict, List, Optional

import pandas as pd

from core.utils.io_metrics import observe_disk_write
from core.registro_metrico import registro_metrico
from core.auditoria import AuditEvent, AuditResult, registrar_auditoria
from core.utils.utils import configurar_logger
from core.supervisor import tick
from observability.metrics import REPORT_IO_ERRORS_TOTAL

log = configurar_logger('rechazos')

UTC = timezone.utc


class RejectionHandler:
    """Encapsula la lógica de registro y almacenamiento de rechazos."""

    def __init__(
        self,
        log_dir: str,
        registro_tecnico_csv: str | None = None,
        batch_size: int = 10,
        flush_retry_attempts: int = 3,
        flush_retry_base_delay: float = 0.5,
        flush_retry_max_delay: float = 5.0,
        fallback_handler: Callable[[List[dict]], None] | None = None,
        *,
        audit_retry_attempts: int = 3,
        audit_retry_base_delay: float = 0.5,
        audit_retry_max_delay: float = 5.0,
        audit_circuit_breaker_threshold: int = 5,
        audit_circuit_breaker_reset: float = 60.0,
    ) -> None:
        self.log_dir = log_dir
        self.registro_tecnico_csv = registro_tecnico_csv
        os.makedirs(os.path.join(log_dir, 'rechazos'), exist_ok=True)
        if registro_tecnico_csv:
            os.makedirs(os.path.dirname(registro_tecnico_csv), exist_ok=True)
        self._buffer: List[dict] = []
        self._batch_size = batch_size
        self._flush_retry_attempts = max(1, flush_retry_attempts)
        self._flush_retry_base_delay = max(0.0, flush_retry_base_delay)
        self._flush_retry_max_delay = max(self._flush_retry_base_delay, flush_retry_max_delay)
        self._fallback_handler = fallback_handler
        self._fallback_queue: List[dict] = []
        self._flush_failures: int = 0
        self._audit_retry_attempts = max(1, audit_retry_attempts)
        self._audit_retry_base_delay = max(0.0, audit_retry_base_delay)
        self._audit_retry_max_delay = max(
            self._audit_retry_base_delay, audit_retry_max_delay
        )
        self._audit_circuit_breaker_threshold = max(1, audit_circuit_breaker_threshold)
        self._audit_circuit_breaker_reset = max(0.0, audit_circuit_breaker_reset)
        self._audit_failures_streak: int = 0
        self._audit_circuit_breaker_open_until: float | None = None
        self._audit_dead_letter: List[dict] = []

    def registrar(
        self,
        symbol: str,
        motivo: str,
        puntaje: Optional[float] = None,
        peso_total: Optional[float] = None,
        estrategias: Optional[List[str] | Dict[str, float]] = None,
        capital: float = 0.0,
        config: Optional[dict] = None,
    ) -> None:
        """Centraliza los mensajes de rechazo de entradas."""
        mensaje = f'🔴 RECHAZO: {symbol} | Causa: {motivo}'
        if puntaje is not None:
            mensaje += f' | Puntaje: {puntaje:.2f}'
        if peso_total is not None:
            mensaje += f' | Peso: {peso_total:.2f}'
        if estrategias:
            estr = estrategias
            if isinstance(estr, dict):
                estr = list(estr.keys())
            mensaje += f' | Estrategias: {estr}'
        log.info(mensaje)
        registro = {
            'symbol': symbol,
            'motivo': motivo,
            'puntaje': puntaje,
            'peso_total': peso_total,
            'estrategias': ','.join(
                estrategias.keys() if isinstance(estrategias, dict) else estrategias
            )
            if estrategias
            else '',
        }
        self._buffer.append(registro)
        if len(self._buffer) >= self._batch_size:
            self.flush()
        try:
            registro_metrico.registrar('rechazo', registro)
        except Exception as exc:  # pragma: no cover - logging path
            log.warning(
                'No se pudo emitir métrica de rechazo: %s',
                exc,
                exc_info=exc,
            )
        audit_payload = {
            'symbol': symbol,
            'evento': AuditEvent.REJECTION,
            'resultado': AuditResult.REJECTED,
            'estrategias_activas': estrategias,
            'score': puntaje,
            'razon': motivo,
            'capital_actual': capital,
            'config_usada': config or {},
            'source': 'risk.rejection_handler',
        }
        audit_success = self._enviar_auditoria_con_resiliencia(audit_payload)
        if audit_success:
            self.reenviar_auditorias_pendientes()

    def flush(self) -> None:
        buffer = [r for r in self._buffer if r]
        if not buffer and not self._fallback_queue:
            return
        fecha = datetime.now(UTC).strftime('%Y%m%d')
        archivo = os.path.join(self.log_dir, 'rechazos', f'{fecha}.csv')
        payload = self._fallback_queue + buffer
        df = pd.DataFrame(payload)
        archivo_existe = os.path.exists(archivo)
        modo = 'a' if archivo_existe else 'w'
        header = not archivo_existe

        if self._write_with_retries(archivo, df, modo, header):
            self._buffer.clear()
            self._fallback_queue.clear()
            return

        if buffer:
            self._fallback_queue.extend(buffer)
        self._buffer.clear()
        self._emit_fallback_metrics(len(buffer))

    async def flush_periodically(self, intervalo: int, stop_event: asyncio.Event) -> None:
        """Vacía el buffer de rechazos a intervalos regulares manejando fallos transitorios."""

        cancelled = False
        try:
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=intervalo)
                except asyncio.TimeoutError:
                    pass

                if stop_event.is_set():
                    break

                try:
                    self.flush()
                except Exception as exc:  # pragma: no cover - logging path
                    self._flush_failures += 1
                    log.error('❌ Error al hacer flush periódico de rechazos: %s', exc, exc_info=exc)
                    continue

                tick('rechazos_flush')
        except asyncio.CancelledError:
            cancelled = True
        finally:
            try:
                self.flush()
            except Exception as exc:  # pragma: no cover - logging path
                self._flush_failures += 1
                log.error('❌ Error al ejecutar flush final de rechazos: %s', exc, exc_info=exc)
            else:
                tick('rechazos_flush')

            if cancelled:
                raise

    def reenviar_auditorias_pendientes(self) -> None:
        """Reintenta la entrega de auditorías previamente rechazadas."""

        if not self._audit_dead_letter:
            return

        pendientes = list(self._audit_dead_letter)
        self._audit_dead_letter.clear()
        for payload in pendientes:
            exito = self._enviar_auditoria_con_resiliencia(payload)
            if exito:
                continue
            if self._circuit_breaker_activo():
                break

    def registrar_tecnico(
        self,
        symbol: str,
        score: float,
        puntos: Dict[str, float],
        tendencia: str,
        precio: float,
        motivo: str,
        estrategias: Dict[str, float] | None = None,
    ) -> None:
        """Guarda detalles de rechazos técnicos en un CSV separado."""
        if not self.registro_tecnico_csv:
            return
        fila = {
            'timestamp': datetime.now(UTC).isoformat(),
            'symbol': symbol,
            'puntaje_total': score,
            'indicadores_fallidos': ','.join([k for k, v in puntos.items() if not v]),
            'estado_mercado': tendencia,
            'precio': precio,
            'motivo': motivo,
            'estrategias': ','.join(estrategias.keys()) if estrategias else '',
        }
        df = pd.DataFrame([fila])
        modo = 'a' if os.path.exists(self.registro_tecnico_csv) else 'w'
        observe_disk_write(
            'rechazos_tecnicos_csv',
            self.registro_tecnico_csv,
            lambda: df.to_csv(
                self.registro_tecnico_csv,
                mode=modo,
                header=not os.path.exists(self.registro_tecnico_csv),
                index=False,
            ),
        )

    def _write_with_retries(
        self,
        archivo: str,
        dataframe: pd.DataFrame,
        modo: str,
        header: bool,
    ) -> bool:
        delay = self._flush_retry_base_delay
        for intento in range(1, self._flush_retry_attempts + 1):
            try:
                observe_disk_write(
                    'rechazos_csv',
                    archivo,
                    lambda: dataframe.to_csv(
                        archivo,
                        mode=modo,
                        header=header,
                        index=False,
                    ),
                )
                return True
            except Exception as exc:  # pragma: no cover - logging path
                REPORT_IO_ERRORS_TOTAL.labels(operation='rechazos_csv').inc()
                log.error(
                    '❌ Error escribiendo rechazos en %s (intento %s/%s): %s',
                    archivo,
                    intento,
                    self._flush_retry_attempts,
                    exc,
                    exc_info=exc,
                )
                if intento >= self._flush_retry_attempts:
                    break
                time.sleep(delay)
                delay = min(delay * 2, self._flush_retry_max_delay)
        return False

    def _emit_fallback_metrics(self, nuevos_rechazos: int) -> None:
        if nuevos_rechazos:
            log.warning(
                '⚠️ Persistencia de rechazos degradada a almacenamiento alternativo (%s nuevos).',
                nuevos_rechazos,
            )
        if not self._fallback_handler or not self._fallback_queue:
            return
        try:
            self._fallback_handler(list(self._fallback_queue))
        except Exception as exc:  # pragma: no cover - logging path
            log.error('❌ Error al notificar fallback de rechazos: %s', exc, exc_info=exc)

    def _enviar_auditoria_con_resiliencia(self, payload: dict) -> bool:
        if self._circuit_breaker_activo():
            log.error(
                'Circuit breaker de auditoría activo; evento encolado para reintento',
                extra={'symbol': payload.get('symbol'), 'evento': payload.get('evento')},
            )
            self._audit_dead_letter.append(payload)
            return False

        attempts = 0
        last_exc: Exception | None = None
        while attempts < self._audit_retry_attempts:
            attempts += 1
            try:
                registrar_auditoria(**payload)
            except Exception as exc:  # pragma: no cover - logging path exercised in tests
                last_exc = exc
                log.warning(
                    'Intento %d/%d falló al registrar auditoría: %s',
                    attempts,
                    self._audit_retry_attempts,
                    exc,
                    exc_info=exc if attempts == self._audit_retry_attempts else False,
                    extra={'symbol': payload.get('symbol')},
                )
                if attempts >= self._audit_retry_attempts:
                    break
                espera = min(
                    self._audit_retry_base_delay * (2 ** (attempts - 1)),
                    self._audit_retry_max_delay,
                )
                self._sleep(espera)
            else:
                if self._audit_failures_streak:
                    log.info(
                        'Auditoría recuperada tras %d fallos consecutivos',
                        self._audit_failures_streak,
                        extra={'symbol': payload.get('symbol')},
                    )
                self._audit_failures_streak = 0
                return True

        self._audit_failures_streak += 1
        if last_exc is not None:
            log.error(
                'No se pudo registrar auditoría tras %d intentos: %s',
                self._audit_retry_attempts,
                last_exc,
                exc_info=last_exc,
                extra={'symbol': payload.get('symbol')},
            )
        self._abrir_circuit_breaker_si_corresponde()
        self._audit_dead_letter.append(payload)
        return False

    def _abrir_circuit_breaker_si_corresponde(self) -> None:
        if self._audit_failures_streak < self._audit_circuit_breaker_threshold:
            return
        if self._audit_circuit_breaker_reset <= 0:
            self._audit_circuit_breaker_open_until = float('inf')
        else:
            self._audit_circuit_breaker_open_until = (
                time.monotonic() + self._audit_circuit_breaker_reset
            )
        log.error(
            'Circuit breaker de auditoría activado tras %d fallos consecutivos',
            self._audit_failures_streak,
        )

    def _circuit_breaker_activo(self) -> bool:
        if self._audit_circuit_breaker_open_until is None:
            return False
        if self._audit_circuit_breaker_open_until == float('inf'):
            return True
        if time.monotonic() >= self._audit_circuit_breaker_open_until:
            self._audit_circuit_breaker_open_until = None
            self._audit_failures_streak = 0
            log.info('Circuit breaker de auditoría restablecido tras periodo de enfriamiento')
            return False
        return True

    def _sleep(self, segundos: float) -> None:
        if segundos <= 0:
            return
        time.sleep(segundos)
