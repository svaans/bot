"""Minimal capital facade used by :mod:`core.risk.risk_manager`.

The production project historically shipped a large ``CapitalManager`` that
handled balance synchronisation with the exchange, advanced position sizing and
event bus integrations.  For the current risk wave we only need a lightweight
object that exposes the pieces consumed by :class:`RiskManager`: capital
availability per symbol, a global exposure view and a simple Kelly multiplier.

This module keeps that interface focused and backed by configuration data so it
can be instantiated in unit tests without external dependencies.
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Set

from config.config_manager import Config
from core.utils.logger import configurar_logger
from core.capital_repository import CapitalRepository, CapitalSnapshot
from observability.metrics import (
    CAPITAL_CONFIGURED_GAUGE,
    CAPITAL_CONFIGURED_TOTAL,
    CAPITAL_DIVERGENCE_ABSOLUTE,
    CAPITAL_DIVERGENCE_RATIO,
    CAPITAL_DIVERGENCE_THRESHOLD,
    CAPITAL_REGISTERED_GAUGE,
    CAPITAL_REGISTERED_TOTAL,
)

log = configurar_logger("capital_manager", modo_silencioso=True)


def _normalizar_symbol(symbol: str) -> str:
    """Return ``symbol`` uppercased while tolerating ``None`` inputs."""

    return symbol.upper() if isinstance(symbol, str) else ""


@dataclass(slots=True)
class CapitalState:
    """Snapshot of capital configuration used by :class:`CapitalManager`."""

    total: float = 0.0
    default_por_symbol: float = 0.0
    por_symbol: Dict[str, float] = field(default_factory=dict)


class CapitalManager:
    """Facade returning capital and exposure information for risk checks."""

    def __init__(
        self,
        config: Config,
        *,
        exposure_limits: Mapping[str, float] | None = None,
        exposure_total: float | None = None,
        capital_repository: CapitalRepository | None = None,
    ) -> None:
        self.config = config
        self._repository = capital_repository or CapitalRepository()

        repo_path = getattr(self._repository, "path", None)
        repo_path_obj = Path(repo_path) if repo_path else None
        file_exists = repo_path_obj.exists() if repo_path_obj else False

        snapshot = self._repository.load()
        snapshot_capital = dict(snapshot.capital_por_simbolo)
        snapshot_disponible = max(0.0, float(snapshot.disponible_global))
        overrides: Dict[str, float] = {}
        if exposure_limits:
            overrides.update(dict(exposure_limits))
        if snapshot.capital_por_simbolo:
            overrides.update(snapshot.capital_por_simbolo)

        self._state = self._build_state(
            config,
            overrides if overrides else None,
            exposure_total,
        )
        self.capital_por_simbolo = dict(self._state.por_symbol)
        self._divergence_threshold = max(
            0.0,
            float(getattr(config, "risk_capital_divergence_threshold", 0.0) or 0.0),
        )
        self._divergence_alert_active = False
        self._last_divergence_ratio = 0.0
        self._metric_symbols: Set[str] = set()
        self._kelly_base = float(getattr(config, "risk_kelly_base", 0.1) or 0.1)
        self.fraccion_kelly = self._kelly_base
        self._recalcular_disponible_global()
        self._event_bus: Any | None = None
        self._apply_persisted_state(snapshot)
        if (
            not file_exists
            or self.capital_por_simbolo != snapshot_capital
            or self._disponible_global != snapshot_disponible
        ):
            self._persist_state()

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    def _build_state(
        self,
        config: Config,
        exposure_limits: Mapping[str, float] | None,
        exposure_total: float | None,
    ) -> CapitalState:
        total = float(
            exposure_total
            if exposure_total is not None
            else getattr(config, "risk_capital_total", 0.0) or 0.0
        )
        default = float(
            getattr(config, "risk_capital_default_per_symbol", 0.0) or 0.0
        )
        por_symbol = {
            _normalizar_symbol(sym): float(value)
            for sym, value in getattr(config, "risk_capital_per_symbol", {}).items()
        }
        if exposure_limits:
            por_symbol.update(
                {
                    _normalizar_symbol(sym): float(value)
                    for sym, value in exposure_limits.items()
                }
            )

        symbols = [_normalizar_symbol(sym) for sym in getattr(config, "symbols", [])]
        symbols = [sym for sym in symbols if sym]

        if default <= 0 and total > 0 and symbols:
            default = total / max(len(symbols), 1)

        if default <= 0:
            default = float(getattr(config, "min_order_eur", 0.0) or 0.0)

        capital: Dict[str, float] = {}
        for symbol in symbols:
            capital[symbol] = max(0.0, por_symbol.get(symbol, default))

        # Include any manual overrides not listed in ``config.symbols``.
        for symbol, value in por_symbol.items():
            capital.setdefault(symbol, max(0.0, value))

        return CapitalState(total=total, default_por_symbol=default, por_symbol=capital)

    def _recalcular_disponible_global(self) -> None:
        disponible = sum(valor for valor in self.capital_por_simbolo.values() if valor > 0)
        if self._state.total > 0:
            disponible = min(disponible, self._state.total)
        self._disponible_global = disponible
        self._refresh_metrics()

    # ------------------------------------------------------------------
    # Public API consumed by ``RiskManager``
    # ------------------------------------------------------------------
    def hay_capital_libre(self) -> bool:
        """Return ``True`` when at least one symbol has positive exposure."""

        if self._state.total > 0:
            return self._disponible_global > 0
        return any(valor > 0 for valor in self.capital_por_simbolo.values())

    def tiene_capital(self, symbol: str) -> bool:
        """Indicate whether ``symbol`` has positive exposure assigned."""

        return self.exposure_disponible(symbol) > 0

    def exposure_disponible(self, symbol: str | None = None) -> float:
        """Return available exposure globally or for ``symbol`` if provided."""

        if symbol:
            clave = _normalizar_symbol(symbol)
            return float(self.capital_por_simbolo.get(clave, 0.0))
        return float(self._disponible_global)

    def exposure_asignada(self, symbol: str | None = None) -> float:
        """Return the configured exposure for ``symbol`` or globally if ``None``."""

        if symbol is None:
            if self._state.total > 0:
                return float(self._state.total)
            return float(sum(self._state.por_symbol.values()))
        clave = _normalizar_symbol(symbol)
        if clave in self._state.por_symbol:
            return float(self._state.por_symbol[clave])
        if self._state.default_por_symbol > 0:
            return float(self._state.default_por_symbol)
        return 0.0

    def actualizar_exposure(self, symbol: str, disponible: float) -> None:
        """Update the available exposure for ``symbol`` and refresh caches."""

        clave = _normalizar_symbol(symbol)
        self.capital_por_simbolo[clave] = max(0.0, float(disponible))
        self._recalcular_disponible_global()
        self._persist_state()

    def aplicar_multiplicador_kelly(self, factor: float) -> float:
        """Adjust the Kelly fraction with ``factor`` keeping defensive bounds."""
        if not isinstance(factor, (int, float)) or factor <= 0:
            log.debug("capital_manager.kelly_invalid", extra={"factor": factor})
            return self.fraccion_kelly
        multiplicador = max(0.1, min(5.0, float(factor)))
        self.fraccion_kelly = round(self._kelly_base * multiplicador, 6)
        log.debug(
            "capital_manager.kelly_applied",
            extra={"factor": multiplicador, "fraccion": self.fraccion_kelly},
        )
        return self.fraccion_kelly

    @property
    def event_bus(self) -> Any | None:
        return self._event_bus

    @event_bus.setter
    def event_bus(self, value: Any | None) -> None:
        self._event_bus = value
        if value is None:
            # Aseguramos que cualquier alerta activa se limpie si perdemos el bus.
            self._divergence_alert_active = False
            return
        start = getattr(value, "start", None)
        if callable(start):
            try:
                start()
            except Exception:
                log.warning(
                    "No se pudo iniciar event_bus tras inyección en CapitalManager",
                    exc_info=True,
                )
        self._refresh_metrics()

    # ------------------------------------------------------------------
    # Persistencia
    # ------------------------------------------------------------------
    def _apply_persisted_state(self, snapshot: CapitalSnapshot) -> None:
        snapshot: CapitalSnapshot = self._repository.load()
        overrides = snapshot.capital_por_simbolo
        if overrides:
            for symbol, value in overrides.items():
                clave = _normalizar_symbol(symbol)
                if not clave:
                    continue
                self.capital_por_simbolo[clave] = max(0.0, float(value))
        stored_disponible = max(0.0, float(snapshot.disponible_global))
        self._recalcular_disponible_global()
        if stored_disponible > 0:
            capped = min(stored_disponible, self._disponible_global)
            if capped < self._disponible_global:
                log.debug(
                    "capital_manager.disponible_capped",
                    extra={
                        "stored": stored_disponible,
                        "recalculated": self._disponible_global,
                        "applied": capped,
                    },
                )
            self._disponible_global = capped
        self._refresh_metrics()

    def _persist_state(self) -> None:
        try:
            self._repository.save(self.capital_por_simbolo, self._disponible_global)
        except Exception:
            log.warning(
                "capital_manager.persist_failed",
                extra={"path": str(getattr(self._repository, "path", ""))},
                exc_info=True,
            )
    # ------------------------------------------------------------------
    # Métricas y alertas de divergencia de capital
    # ------------------------------------------------------------------
    def _refresh_metrics(self) -> None:
        """Actualiza gauges Prometheus y evalúa la divergencia de capital."""

        try:
            symbols = set(self.capital_por_simbolo.keys()) | set(self._state.por_symbol.keys())
            removed = self._metric_symbols - symbols
            self._metric_symbols = symbols

            for symbol in symbols:
                actual = float(self.capital_por_simbolo.get(symbol, 0.0))
                theoretical = float(self._state.por_symbol.get(symbol, 0.0))
                CAPITAL_REGISTERED_GAUGE.labels(symbol=symbol).set(actual)
                CAPITAL_CONFIGURED_GAUGE.labels(symbol=symbol).set(theoretical)

            for symbol in removed:
                CAPITAL_REGISTERED_GAUGE.labels(symbol=symbol).set(0.0)
                CAPITAL_CONFIGURED_GAUGE.labels(symbol=symbol).set(0.0)

            registered_total = sum(self.capital_por_simbolo.get(symbol, 0.0) for symbol in symbols)
            configured_total = self.exposure_asignada(None)
            diff_abs = abs(registered_total - configured_total)

            CAPITAL_REGISTERED_TOTAL.set(registered_total)
            CAPITAL_CONFIGURED_TOTAL.set(configured_total)
            CAPITAL_DIVERGENCE_ABSOLUTE.set(diff_abs)
            CAPITAL_DIVERGENCE_THRESHOLD.set(self._divergence_threshold)

            if configured_total > 0:
                ratio = diff_abs / configured_total
            else:
                ratio = 1.0 if diff_abs > 0 else 0.0

            ratio = float(max(ratio, 0.0))
            self._last_divergence_ratio = ratio
            CAPITAL_DIVERGENCE_RATIO.set(ratio)

            self._evaluate_divergence_alert(
                ratio=ratio,
                diff_abs=diff_abs,
                registered_total=registered_total,
                configured_total=configured_total,
            )
        except Exception:
            log.debug("capital_manager.metrics_failed", exc_info=True)

    def _evaluate_divergence_alert(
        self,
        *,
        ratio: float,
        diff_abs: float,
        registered_total: float,
        configured_total: float,
    ) -> None:
        threshold = self._divergence_threshold
        if threshold <= 0:
            if self._divergence_alert_active:
                self._divergence_alert_active = False
                self._emit_divergence_cleared(
                    ratio=ratio,
                    diff_abs=diff_abs,
                    registered_total=registered_total,
                    configured_total=configured_total,
                )
            return

        if ratio > threshold:
            if not self._divergence_alert_active:
                self._divergence_alert_active = True
                self._emit_divergence_alert(
                    ratio=ratio,
                    diff_abs=diff_abs,
                    registered_total=registered_total,
                    configured_total=configured_total,
                )
        elif self._divergence_alert_active:
            self._divergence_alert_active = False
            self._emit_divergence_cleared(
                ratio=ratio,
                diff_abs=diff_abs,
                registered_total=registered_total,
                configured_total=configured_total,
            )

    def _emit_divergence_alert(
        self,
        *,
        ratio: float,
        diff_abs: float,
        registered_total: float,
        configured_total: float,
    ) -> None:
        payload = self._build_divergence_payload(
            ratio=ratio,
            diff_abs=diff_abs,
            registered_total=registered_total,
            configured_total=configured_total,
            status="detected",
        )
        log.warning("capital_manager.divergence_detected", extra=payload)
        self._emit_event("capital.divergence_detected", payload)

    def _emit_divergence_cleared(
        self,
        *,
        ratio: float,
        diff_abs: float,
        registered_total: float,
        configured_total: float,
    ) -> None:
        payload = self._build_divergence_payload(
            ratio=ratio,
            diff_abs=diff_abs,
            registered_total=registered_total,
            configured_total=configured_total,
            status="cleared",
        )
        log.info("capital_manager.divergence_cleared", extra=payload)
        self._emit_event("capital.divergence_cleared", payload)

    def _build_divergence_payload(
        self,
        *,
        ratio: float,
        diff_abs: float,
        registered_total: float,
        configured_total: float,
        status: str,
    ) -> MutableMapping[str, float | str]:
        return {
            "status": status,
            "threshold": float(self._divergence_threshold),
            "ratio": float(ratio),
            "difference": float(diff_abs),
            "capital_registrado": float(registered_total),
            "capital_teorico": float(configured_total),
        }

    def _emit_event(self, event: str, payload: Mapping[str, float | str]) -> None:
        bus = self._event_bus
        if not bus:
            return
        emit = getattr(bus, "emit", None)
        if not callable(emit):
            return
        try:
            emit(event, dict(payload))
        except Exception:
            log.debug(
                "capital_manager.emit_failed",
                extra={"event": event},
                exc_info=True,
            )

