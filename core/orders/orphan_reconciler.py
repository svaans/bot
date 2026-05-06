# core/orders/orphan_reconciler.py
"""
Reconciliador de intents de pre-ejecución huérfanos.

Arquitectura de dos fases:
  Fase 1 — sync, en __init__: escanea filesystem, emite CRITICAL + Telegram,
            bloquea símbolos afectados para impedir nuevas órdenes.
  Fase 2 — async, background: espera señal explícita de readiness de ccxt,
            consulta Binance con retry/backoff, elimina archivos confirmados,
            desbloquea símbolos.

Invariantes:
  - Un símbolo con orphan en estado DETECTED o QUERYING no puede operar.
  - El archivo de intent solo se elimina después de verificación confirmada.
  - La señal de readiness de ccxt es explícita (asyncio.Event), nunca un delay.
  - Si se superan los MAX_RETRIES, el símbolo queda bloqueado hasta reinicio
    con intervención manual.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("orphan_reconciler")

# ---------------------------------------------------------------------------
# Señal global de readiness de ccxt
# Creada lazy para evitar problemas con el event loop en tiempo de importación.
# ---------------------------------------------------------------------------
_CCXT_READY: Optional[asyncio.Event] = None
_CCXT_READY_LOCK = asyncio.Lock()  # protege la creación lazy


def get_ccxt_ready_event() -> asyncio.Event:
    """Devuelve el Event singleton de readiness de ccxt (creación lazy)."""
    global _CCXT_READY
    if _CCXT_READY is None:
        _CCXT_READY = asyncio.Event()
    return _CCXT_READY


def signal_ccxt_ready() -> None:
    """Llamar desde ccxt_client cuando load_markets() completa con éxito."""
    evt = get_ccxt_ready_event()
    if not evt.is_set():
        evt.set()
        log.info("ccxt.ready_signal.fired")


# ---------------------------------------------------------------------------
# Máquina de estados del orphan
# ---------------------------------------------------------------------------
class OrphanState(str, Enum):
    DETECTED = "detected"              # Encontrado en filesystem, esperando reconciliar
    QUERYING = "querying"              # Consultando Binance ahora mismo
    RECONCILED = "reconciled"          # Encontrado y registrado en Binance
    CONFIRMED_ABSENT = "confirmed_absent"  # Binance confirma que no existe
    FAILED_PERMANENT = "failed_permanent"  # Max retries superados, operador debe intervenir


@dataclass
class OrphanRecord:
    file: Path
    symbol: str
    operation_id: str
    data: dict
    state: OrphanState = OrphanState.DETECTED
    attempts: int = 0
    last_error: str = ""


# ---------------------------------------------------------------------------
# Reconciliador
# ---------------------------------------------------------------------------
class OrphanReconciler:
    """
    Gestiona el ciclo de vida completo de los orphans.

    Uso:
        reconciler = OrphanReconciler()
        reconciler.scan_and_detect(RUTA_DB)   # en __init__ del OrderManager
        reconciler.start(loop)                 # después de crear el event loop
        # ... ccxt_client llama signal_ccxt_ready() ...
        # ... reconciler resuelve en background ...
        reconciler.shutdown()                  # en el cierre del OrderManager
    """

    MAX_RETRIES = 5
    BACKOFF_BASE = 2.0
    BACKOFF_MAX = 120.0

    def __init__(self) -> None:
        # Keyed por operation_id
        self._records: dict[str, OrphanRecord] = {}
        # Símbolos bloqueados: conjunto de symbols con orphan activo
        self._blocked: set[str] = set()
        # Lock por símbolo: evita reconciliaciones concurrentes del mismo par
        self._sym_locks: dict[str, asyncio.Lock] = {}
        self._task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    # ------------------------------------------------------------------
    # FASE 1 — sync, sin red
    # ------------------------------------------------------------------

    def scan_and_detect(self, ruta_db: str) -> int:
        """
        Escanea el directorio de intents, emite CRITICAL por cada uno,
        bloquea los símbolos afectados y devuelve el número de orphans.
        No hace ninguna llamada de red.
        """
        carpeta = Path(ruta_db).parent
        if not carpeta.exists():
            return 0

        found = 0
        for f in carpeta.glob("pre_exec_intent_*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
            except Exception as exc:
                log.error(
                    "orphan.scan.corrupt_file",
                    extra={"file": str(f), "error": str(exc)},
                )
                f.unlink(missing_ok=True)
                continue

            symbol = data.get("symbol") if isinstance(data, dict) else None
            op_id = data.get("operation_id") if isinstance(data, dict) else None

            if not symbol or not op_id:
                log.warning(
                    "orphan.scan.incomplete_data",
                    extra={"file": str(f), "data": data},
                )
                f.unlink(missing_ok=True)
                continue

            record = OrphanRecord(
                file=f,
                symbol=symbol,
                operation_id=op_id,
                data=data,
                state=OrphanState.DETECTED,
            )
            self._records[op_id] = record
            self._blocked.add(symbol)
            found += 1

            log.critical(
                "orphan.detected",
                extra={
                    "symbol": symbol,
                    "operation_id": op_id,
                    "precio": data.get("precio"),
                    "direccion": data.get("direccion"),
                    "timestamp_intent": data.get("timestamp"),
                    "state": OrphanState.DETECTED.value,
                    "trading_blocked": True,
                },
            )
            self._notify_telegram(
                f"🚨 Orphan detectado: {symbol}\n"
                f"ID: {op_id}\n"
                f"El símbolo está BLOQUEADO hasta reconciliar con Binance.",
                "CRITICAL",
            )

        return found

    # ------------------------------------------------------------------
    # Invariante de bloqueo — llamar desde abrir_async
    # ------------------------------------------------------------------

    def is_blocked(self, symbol: str) -> bool:
        """True si el símbolo tiene un orphan pendiente. Bloquea nuevas órdenes."""
        return symbol in self._blocked

    def get_blocking_record(self, symbol: str) -> Optional[OrphanRecord]:
        """Devuelve el OrphanRecord activo que bloquea este símbolo, si existe."""
        for rec in self._records.values():
            if rec.symbol == symbol and rec.state in (
                OrphanState.DETECTED,
                OrphanState.QUERYING,
                OrphanState.FAILED_PERMANENT,
            ):
                return rec
        return None

    # ------------------------------------------------------------------
    # FASE 2 — async background
    # ------------------------------------------------------------------

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Lanza la tarea de reconciliación si hay orphans pendientes."""
        if not self._records:
            return
        self._task = loop.create_task(
            self._reconcile_all(),
            name="orphan_reconciler.reconcile_all",
        )

    async def _reconcile_all(self) -> None:
        """
        Espera la señal explícita de ccxt, luego reconcilia todos los orphans
        en paralelo (pero serializados por símbolo).
        """
        ccxt_ready = get_ccxt_ready_event()
        log.info(
            "orphan.waiting_ccxt_ready",
            extra={"pending_orphans": len(self._records)},
        )

        # Espera indefinida pero interrumpible por shutdown.
        # Si ccxt nunca llega a estar listo, no hay trading de todos modos.
        done, _ = await asyncio.wait(
            [
                asyncio.create_task(ccxt_ready.wait(), name="wait_ccxt"),
                asyncio.create_task(self._shutdown_event.wait(), name="wait_shutdown"),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if self._shutdown_event.is_set():
            log.info("orphan.reconcile.aborted_by_shutdown")
            return

        log.info("orphan.ccxt_ready_received")

        # Reconciliar todos los orphans en paralelo, con serialización por símbolo
        await asyncio.gather(
            *[
                self._reconcile_one(rec)
                for rec in list(self._records.values())
            ],
            return_exceptions=True,
        )

    async def _reconcile_one(self, record: OrphanRecord) -> None:
        """
        Reconcilia un único orphan con retry y backoff exponencial.

        Garantías:
        - El archivo solo se elimina cuando el estado es confirmado (RECONCILED o CONFIRMED_ABSENT).
        - El símbolo se desbloquea solo cuando el archivo es eliminado.
        - Si se superan MAX_RETRIES, el símbolo queda bloqueado y se notifica al operador.
        - Serializa por símbolo: dos orphans del mismo par no corren en paralelo.
        """
        symbol = record.symbol
        op_id = record.operation_id

        if symbol not in self._sym_locks:
            self._sym_locks[symbol] = asyncio.Lock()

        async with self._sym_locks[symbol]:
            for attempt in range(1, self.MAX_RETRIES + 1):
                if self._shutdown_event.is_set():
                    log.info(
                        "orphan.reconcile.interrupted",
                        extra={"symbol": symbol, "op_id": op_id, "attempt": attempt},
                    )
                    return

                record.state = OrphanState.QUERYING
                record.attempts = attempt

                log.info(
                    "orphan.reconcile.attempt",
                    extra={
                        "symbol": symbol,
                        "operation_id": op_id,
                        "attempt": attempt,
                        "max_retries": self.MAX_RETRIES,
                    },
                )

                try:
                    from core.orders.real_orders_reconcile import sincronizar_ordenes_binance

                    reconciliadas = sincronizar_ordenes_binance(
                        simbolos=[symbol], modo_real=True
                    )

                    if symbol in reconciliadas:
                        ord_ext = reconciliadas[symbol]
                        active_op_id = getattr(ord_ext, "operation_id", None)

                        if active_op_id == op_id:
                            # Orden encontrada con el mismo ID: estaba en Binance
                            record.state = OrphanState.RECONCILED
                            log.info(
                                "orphan.reconcile.found_on_exchange",
                                extra={
                                    "symbol": symbol,
                                    "operation_id": op_id,
                                    "state": record.state.value,
                                },
                            )
                            self._notify_telegram(
                                f"✅ Orphan reconciliado: {symbol}\n"
                                f"La orden existía en Binance y fue registrada localmente.\n"
                                f"ID: {op_id}",
                                "INFO",
                            )
                        else:
                            # Hay una orden activa pero con distinto ID: el intent es obsoleto
                            record.state = OrphanState.CONFIRMED_ABSENT
                            log.warning(
                                "orphan.reconcile.stale_intent",
                                extra={
                                    "symbol": symbol,
                                    "operation_id_intent": op_id,
                                    "operation_id_active": active_op_id,
                                    "state": record.state.value,
                                },
                            )
                    else:
                        # Binance confirma que no hay orden activa: el intent nunca ejecutó
                        record.state = OrphanState.CONFIRMED_ABSENT
                        log.warning(
                            "orphan.reconcile.not_found_on_exchange",
                            extra={
                                "symbol": symbol,
                                "operation_id": op_id,
                                "state": record.state.value,
                            },
                        )
                        self._notify_telegram(
                            f"⚠️ Orphan resuelto: {symbol}\n"
                            f"No hay orden activa en Binance.\n"
                            f"ID: {op_id} — Probable fallo previo de ejecución.",
                            "WARNING",
                        )

                    # Eliminar archivo SOLO si tenemos un estado confirmado
                    record.file.unlink(missing_ok=True)
                    self._blocked.discard(symbol)
                    log.info(
                        "orphan.reconcile.complete",
                        extra={
                            "symbol": symbol,
                            "operation_id": op_id,
                            "state": record.state.value,
                            "attempts": record.attempts,
                            "trading_unblocked": True,
                        },
                    )
                    return  # éxito

                except asyncio.CancelledError:
                    # Cancelación limpia: preservar el archivo
                    log.info(
                        "orphan.reconcile.cancelled",
                        extra={"symbol": symbol, "op_id": op_id},
                    )
                    raise

                except Exception as exc:
                    record.last_error = str(exc)

                    if attempt >= self.MAX_RETRIES:
                        record.state = OrphanState.FAILED_PERMANENT
                        log.error(
                            "orphan.reconcile.max_retries_exceeded",
                            extra={
                                "symbol": symbol,
                                "operation_id": op_id,
                                "attempts": record.attempts,
                                "last_error": record.last_error,
                                "state": record.state.value,
                                "trading_blocked": True,
                                "action_required": "manual_intervention",
                            },
                        )
                        self._notify_telegram(
                            f"🚨 FALLO PERMANENTE orphan {symbol}\n"
                            f"Tras {self.MAX_RETRIES} intentos no se pudo reconciliar.\n"
                            f"ID: {op_id}\n"
                            f"ACCIÓN REQUERIDA: verificar Binance manualmente.\n"
                            f"El símbolo sigue BLOQUEADO.",
                            "CRITICAL",
                        )
                        return  # símbolo permanece bloqueado

                    backoff = min(self.BACKOFF_BASE ** attempt, self.BACKOFF_MAX)
                    log.warning(
                        "orphan.reconcile.retry",
                        extra={
                            "symbol": symbol,
                            "operation_id": op_id,
                            "attempt": attempt,
                            "next_attempt_in_seconds": backoff,
                            "error": str(exc),
                        },
                    )
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(), timeout=backoff
                        )
                        # Si llegamos aquí: shutdown durante el backoff
                        log.info("orphan.reconcile.aborted_during_backoff")
                        return
                    except asyncio.TimeoutError:
                        pass  # backoff completado, reintentar

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        """Señala shutdown limpio. El archivo NO se elimina si la reconciliación
        estaba en curso — se retomará en el próximo arranque."""
        self._shutdown_event.set()
        if self._task and not self._task.done():
            self._task.cancel()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _notify_telegram(self, mensaje: str, nivel: str) -> None:
        try:
            from core.orders.real_orders import notificador
            notificador.enviar(mensaje, nivel)
        except Exception:
            pass

    def status(self) -> list[dict]:
        """Devuelve el estado actual de todos los orphans (para logs/heartbeat)."""
        return [
            {
                "symbol": r.symbol,
                "operation_id": r.operation_id,
                "state": r.state.value,
                "attempts": r.attempts,
                "blocked": r.symbol in self._blocked,
                "last_error": r.last_error or None,
            }
            for r in self._records.values()
        ]
