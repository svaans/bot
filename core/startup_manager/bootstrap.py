"""Rutinas de arranque de datos y activación de estrategias."""

from __future__ import annotations

import asyncio
import inspect
import os
from typing import Any

from core.data.bootstrap import warmup_inicial
from core.diag.phase_logger import phase


class BootstrapMixin:
    """Responsabilidades de precarga, validaciones y habilitación de estrategias."""

    trader: Any
    config: Any
    log: Any

    async def _bootstrap(self) -> None:
        assert self.trader is not None and self.config is not None
        await warmup_inicial(
            self.config.symbols,
            self.config.intervalo_velas,
            min_bars=int(os.getenv("MIN_BARS", "400")),
        )
        precargar = getattr(self.trader, "_precargar_historico", None)
        if not precargar:
            self.log.debug("_precargar_historico() no definido en Trader; se omite.")
            return
        try:
            if inspect.iscoroutinefunction(precargar):
                await precargar()
            else:
                precargar()
        except Exception as exc:  # nosec
            self.log.warning(
                "Fallo al ejecutar _precargar_historico(): %s (continuando)",
                exc,
            )

    async def _validate_feeds(self) -> None:
        assert self.trader is not None and self.config is not None
        if getattr(self.config, "modo_real", False) and not getattr(self.trader, "cliente", None):
            msg = (
                "Cliente Binance no inicializado. "
                "Verifica las claves API y las variables de entorno "
                "BINANCE_API_KEY/BINANCE_SECRET."
            )
            self.log.error(msg)
            raise RuntimeError(msg)

    async def _enable_strategies(self) -> None:
        assert self.trader is not None and self.config is not None

        with phase("_check_clock_drift"):
            clock_ok = await asyncio.wait_for(self._check_clock_drift(), timeout=5)
        if not clock_ok:
            await self._apply_clock_drift_safety()

        if not await self._check_storage():
            raise RuntimeError(
                'Storage no disponible. Verifica los permisos de escritura en el directorio de datos.'
            )

        if hasattr(self.trader, "habilitar_estrategias"):
            self.trader.habilitar_estrategias()
        else:
            self.log.debug("Trader sin habilitar_estrategias(); se omite la activación.")

        self._snapshot()
