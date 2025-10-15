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
from dataclasses import dataclass, field
from typing import Dict, Mapping

from config.config_manager import Config
import asyncio
from core.utils.logger import configurar_logger

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
    ) -> None:
        self.config = config
        self._state = self._build_state(config, exposure_limits, exposure_total)
        self.capital_por_simbolo = dict(self._state.por_symbol)
        self._kelly_base = float(getattr(config, "risk_kelly_base", 0.1) or 0.1)
        self.fraccion_kelly = self._kelly_base
        self._recalcular_disponible_global()

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

    def actualizar_exposure(self, symbol: str, disponible: float) -> None:
        """Update the available exposure for ``symbol`` and refresh caches."""

        clave = _normalizar_symbol(symbol)
        self.capital_por_simbolo[clave] = max(0.0, float(disponible))
        self._recalcular_disponible_global()

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

