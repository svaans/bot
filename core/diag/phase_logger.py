"""Context manager para registrar fases de ejecución con métricas de duración."""
from __future__ import annotations

import logging
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Dict, Mapping, MutableMapping, Optional, Type

from core.utils.logger import configurar_logger

__all__ = ["PhaseLogger", "phase"]


@dataclass
class PhaseLogger(AbstractContextManager["PhaseLogger"]):
    """Context manager/decorator que registra eventos de inicio y fin de una fase.

    Está pensado para instrumentar secciones críticas del bot, dejando constancia
    estructurada del comienzo, la duración y los errores ocurridos. El registro se
    emite en formato JSON para integrarse con los pipelines de observabilidad.
    """

    name: str
    logger: logging.Logger = field(default_factory=lambda: configurar_logger("phase"))
    level: int = logging.INFO
    error_level: int = logging.ERROR
    extra: MutableMapping[str, Any] | Mapping[str, Any] | None = None

    _start: float = field(init=False, default=0.0)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("PhaseLogger.name debe ser un string no vacío")
        if not isinstance(self.level, int):
            raise TypeError("PhaseLogger.level debe ser un entero compatible con logging")
        if not isinstance(self.error_level, int):
            raise TypeError("PhaseLogger.error_level debe ser un entero compatible con logging")

    # --- Context manager sync ---
    def __enter__(self) -> "PhaseLogger":
        self._start = time.perf_counter()
        self._log_event("start", level=self.level)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        duration = max(time.perf_counter() - self._start, 0.0)
        if exc_type is None:
            self._log_event("end", level=self.level, duration=duration)
            return False

        payload = {
            "error_type": exc_type.__name__,
            "error_message": str(exc) if exc is not None else "",
        }
        self._log_event(
            "error",
            level=self.error_level,
            duration=duration,
            extra_payload=payload,
            exc_info=(exc_type, exc, tb),
        )
        return False  # Propaga la excepción

    # --- Context manager async ---
    async def __aenter__(self) -> "PhaseLogger":
        return self.__enter__()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        return self.__exit__(exc_type, exc, tb)

    # --- Utilidades internas ---
    def _log_event(
        self,
        state: str,
        *,
        level: int,
        duration: float | None = None,
        extra_payload: Optional[Dict[str, Any]] = None,
        exc_info: Any = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "event": "phase",
            "phase": self.name,
            "state": state,
        }
        if duration is not None:
            payload["duration"] = round(duration, 6)
        if self.extra:
            payload["context"] = dict(self.extra)
        if extra_payload:
            payload.update(extra_payload)
        self.logger.log(level, f"phase:{self.name}:{state}", extra={"phase": payload}, exc_info=exc_info)


def phase(
    name: str,
    *,
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
    error_level: int = logging.ERROR,
    extra: MutableMapping[str, Any] | Mapping[str, Any] | None = None,
) -> PhaseLogger:
    """Facilidad para crear un :class:`PhaseLogger`.

    Parameters
    ----------
    name:
        Identificador de la fase.
    logger:
        Logger pre-configurado; si se omite se usa ``configurar_logger('phase')``.
    level:
        Nivel de log para eventos de inicio/fin.
    error_level:
        Nivel de log utilizado al capturar excepciones dentro de la fase.
    extra:
        Metadata adicional que se incluirá bajo la clave ``context`` en los logs.
    """

    logger = logger or configurar_logger("phase")
    return PhaseLogger(name=name, logger=logger, level=level, error_level=error_level, extra=extra)