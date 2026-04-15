# core/strategies/pesos_governance.py
"""Gobernanza y trazabilidad de persistencia de pesos.

Inventario de escrituras (mantener al añadir call sites)
=======================================================

**Pesos de entrada (estrategias por símbolo)** — archivo efectivo: ``gestor_pesos.ruta``
(``ESTRATEGIAS_PESOS_PATH``, por defecto ``config/estrategias_pesos.json``).
Sobrescribe con la variable de entorno homónima; si antes usabas
``estado/pesos_estrategias.json``, apunta ``ESTRATEGIAS_PESOS_PATH`` ahí o migra el fichero.

**Política opcional:** si ``PESOS_ENTRY_ONLY_SOURCE`` está definido (valor = constante
``EntryWeightSource.*``), solo ese origen puede persistir; el resto registra advertencia y no escribe.

- ``learning/entrenador_estrategias.actualizar_pesos_estrategias_symbol`` — suavizado train/test.
- ``learning/aprendizaje_en_linea.actualizar_pesos_dinamicos`` — ventana corta (solo simulado).
- ``learning/gestor_aprendizaje.registrar_resultado_trade`` — softmax vía ``ajustador_pesos`` + merge.
- ``learning/recalibrar_semana.recalibrar_pesos_semana`` — recalibración semanal global.
- ``learning/aprendizaje_continuo._aplicar_feedback_pesos`` — deltas desde ``config/feedback_manual.json``.
- ``learning/reset_pesos.resetear_pesos_diarios_si_corresponde`` — copia desde base diaria.
- ``core/strategies/entry/analisis_pesos.calcular_pesos_desde_backtest`` — CSV backtest + gestor.

**Persistencia interna** (sin ``source`` explícito hasta refactor):

- ``GestorPesos.set_pesos_symbol`` / ``merge_add`` / ``calcular_desde_backtest`` en ``core/strategies/pesos.py``.

**Pesos técnicos (umbral / score técnico)** — ``config/pesos_tecnicos.json``:

- ``core/strategies/evaluador_tecnico.actualizar_pesos_tecnicos``.

**Salidas de softmax offline** (archivo arbitrario, no siempre el gestor):

- ``core/strategies/ajustador_pesos.ajustar_pesos_por_desempeno`` escribe ``ruta_salida`` (p. ej. tmp).

**Nota:** Los temporales de softmax usan :func:`core.strategies.pesos.entry_weights_temp_path`
al lado del JSON canónico (mismo directorio que ``ESTRATEGIAS_PESOS_PATH``).

**Atribución (Fase 1):** informe consolidado por estrategia desde Parquet —
``python -m core.diag.atribucion_estrategias``.

**Validación OOS / walk-forward (Fase 3):** train vs último tramo test y pliegues
cronológicos — ``python -m core.diag.informe_oos``.

**Vista cartera (Fase 4):** límites globales vía ``RiskManager`` —
``MAX_POSICIONES_CARTERA`` (total de órdenes abiertas antes de nueva entrada) y
``MAX_POSICIONES_MISMO_SENTIDO`` (tope por long o por short, tras conocer el
``side`` del motor). ``0`` = sin límite.

**Régimen / volatilidad (Fase 5):** en :class:`core.strategies.strategy_engine.StrategyEngine`,
etiqueta ``alta``/``media``/``baja`` vía ATR/cierre y multiplicadores opcionales
de ``umbral`` y ``umbral_score_tecnico`` (``REGIMEN_ENTRADA_ENABLED=true`` y
``REGIMEN_MULT_*``). El pipeline pasa ``config`` del trader al motor desde
``verificar_entradas``.

**Ejecución real / slippage (Fase 6):** cada fill de mercado real puede dejar una
línea JSON en ``logs/ejecuciones.jsonl`` (``EXECUTION_QUALITY_LOG_PATH``,
``EXECUTION_QUALITY_LOG_ENABLED``). Incluye precio ticker previo al envío,
precio de fill, slippage vs ticker y vs precio de señal del bot. Resumen por
símbolo: ``python -m core.diag.informe_ejecucion``. Tras compra/venta real, el
``OrderManager`` usa el VWAP de fill en ``precio_entrada`` y en
``precio_cierre`` cuando existe
(:class:`core.orders.market_retry_executor.ExecutionResult`). En backtest/sim,
``BACKTEST_SLIPPAGE_BPS`` desplaza entrada y salida en contra del trader (long:
compra más cara y vende más barato).
"""
from __future__ import annotations

import os
from typing import Any, Dict

from core.strategies.pesos import GestorPesos
from core.utils.metrics_compat import Counter
from core.utils.utils import configurar_logger

log = configurar_logger("pesos_governance")

_WEIGHT_PERSIST = Counter(
    "pesos_entrada_persist_total",
    "Persistencias de pesos de entrada por origen",
    ("source",),
)
_WEIGHT_BLOCKED = Counter(
    "pesos_entrada_persist_blocked_total",
    "Intentos de persistir pesos bloqueados por PESOS_ENTRY_ONLY_SOURCE",
    ("source",),
)


def _entry_only_source_allowed() -> str | None:
    v = os.getenv("PESOS_ENTRY_ONLY_SOURCE", "").strip()
    return v or None


class EntryWeightSource:
    """Orígenes conocidos para atribución y logs."""

    ENTRENADOR_SIMBOLO = "entrenador_estrategias"
    APRENDIZAJE_EN_LINEA = "aprendizaje_en_linea"
    GESTOR_APRENDIZAJE_TRADE = "gestor_aprendizaje_trade"
    RECALIBRAR_SEMANA = "recalibrar_semana"
    FEEDBACK_MANUAL = "feedback_manual"
    RESET_DIARIO = "reset_pesos_diario"
    ANALISIS_PESOS_BACKTEST = "analisis_pesos_backtest"


def persist_entry_weights(
    gestor: GestorPesos,
    snapshot: Dict[str, Any] | None = None,
    *,
    source: str,
    detail: str | None = None,
) -> None:
    """Persiste pesos de entrada y deja rastro estructurado (log + métrica)."""

    allowed_only = _entry_only_source_allowed()
    if allowed_only is not None and source != allowed_only:
        log.warning(
            "pesos_entrada_persist_bloqueado",
            extra={
                "source": source,
                "detail": detail,
                "allowed_only": allowed_only,
                "ruta": str(gestor.ruta),
            },
        )
        _WEIGHT_BLOCKED.labels(source).inc()
        return

    symbols = sorted(
        (snapshot or gestor.pesos).keys(),
        key=lambda s: str(s).upper(),
    )
    log.info(
        "pesos_entrada_persistidos",
        extra={
            "source": source,
            "detail": detail,
            "ruta": str(gestor.ruta),
            "n_symbols": len(symbols),
            "symbols": symbols[:50],
            "symbols_truncated": len(symbols) > 50,
        },
    )
    _WEIGHT_PERSIST.labels(source).inc()
    gestor.guardar(snapshot)


def log_pesos_tecnicos_persistidos(symbol: str, ruta: str, n_symbols_en_archivo: int) -> None:
    """Registro único cuando se escribe ``pesos_tecnicos.json``."""

    log.info(
        "pesos_tecnicos_persistidos",
        extra={
            "source": "evaluador_tecnico",
            "symbol": symbol,
            "ruta": ruta,
            "n_symbols_en_archivo": n_symbols_en_archivo,
        },
    )
