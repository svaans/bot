"""
Gestor de Aprendizaje — ciclo periódico desacoplado del Trader.

Objetivo
--------
- Ejecutar un ciclo de aprendizaje (offline/online) sin bloquear el loop.
- Reportar progreso al watchdog (latidos) y emitir eventos.

API
---
async ciclo_aprendizaje_periodico(fn_entrenar, *, intervalo=86400, watchdog=None, nombre_latido="aprendizaje", on_event=None)
    - `fn_entrenar`: callable sin argumentos que ejecuta un ciclo de entrenamiento.
      Se ejecuta en thread pool (to_thread) para no bloquear el loop.
    - `intervalo`: segundos entre ciclos (por defecto 1 día).
    - `watchdog`: objeto con método `.latido(nombre, causa)` (opcional).
    - `on_event`: callable(evt: str, data: dict) (opcional).
"""
from __future__ import annotations
import asyncio
from typing import Any, Callable, Optional


def _emit(on_event: Optional[Callable[[str, dict], None]], evt: str, data: dict) -> None:
    if on_event:
        try:
            on_event(evt, data)
        except Exception:
            pass


async def ciclo_aprendizaje_periodico(
    fn_entrenar: Callable[[], Any],
    *,
    intervalo: int = 86400,
    watchdog: Any | None = None,
    nombre_latido: str = "aprendizaje",
    on_event: Optional[Callable[[str, dict], None]] = None,
) -> None:
    """Ejecuta `fn_entrenar` periódicamente, con latidos entre iteraciones.

    - Corre `fn_entrenar` en `asyncio.to_thread` para evitar bloquear.
    - Emite latidos durante ejecución y al esperar el siguiente ciclo.
    - No lanza excepciones al exterior; reintenta con backoff básico.
    """
    backoff = 5
    while True:
        try:
            _emit(on_event, "aprendizaje_inicio", {})
            # Ejecuta entrenamiento en thread pool
            fut = asyncio.to_thread(fn_entrenar)
            # Mientras corre, enviamos latidos periódicos
            while not fut.done():
                if watchdog:
                    watchdog.latido(nombre_latido, "run")
                await asyncio.sleep(max(1, intervalo // 60))
            await fut
            _emit(on_event, "aprendizaje_ok", {})
            backoff = 5
        except Exception as e:  # pragma: no cover
            _emit(on_event, "aprendizaje_error", {"error": str(e)})
            await asyncio.sleep(backoff)
            backoff = min(300, backoff * 2)
        # Espera hasta el próximo ciclo, con latidos periódicos
        restante = intervalo
        while restante > 0:
            if watchdog:
                watchdog.latido(nombre_latido, "idle")
            pausa = min(restante, max(1, intervalo // 60))
            await asyncio.sleep(pausa)
            restante -= pausa
