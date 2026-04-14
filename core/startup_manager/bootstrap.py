"""Rutinas de arranque de datos y activación de estrategias."""

from __future__ import annotations

import asyncio
import inspect
import os
from typing import Any

from core.data.bootstrap import warmup_inicial
from core.operational_mode import OperationalMode
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
        require_client = bool(getattr(self.config, "modo_real", False))
        mode = getattr(self.config, "modo_operativo", None)
        if isinstance(mode, OperationalMode):
            require_client = mode.is_real or mode.uses_testnet
        if require_client and not getattr(self.trader, "cliente", None):
            msg = (
                "Cliente Binance no inicializado. "
                "Verifica las claves API y las variables de entorno "
                "BINANCE_API_KEY y BINANCE_API_SECRET."
            )
            self.log.error(msg)
            raise RuntimeError(msg)

    async def _enable_strategies(self) -> None:
        assert self.trader is not None and self.config is not None

        if os.getenv("BINANCE_SYNC_SYSTEM_TIME", "").strip().lower() in {"1", "true", "yes", "on"}:
            from core.utils.binance_time_sync import try_sync_windows_system_time_from_binance

            synced = await asyncio.to_thread(try_sync_windows_system_time_from_binance)
            if synced:
                self.log.info(
                    "Reloj del sistema alineado con la hora UTC de Binance (SetSystemTime)."
                )
            else:
                self.log.warning(
                    "BINANCE_SYNC_SYSTEM_TIME activo pero no se pudo fijar la hora del SO. "
                    "El bot debe ejecutarse como administrador en Windows, o lanza manualmente: "
                    "python -m core.utils.binance_time_sync"
                )

        with phase("_check_clock_drift"):
            clock_ok = await asyncio.wait_for(self._check_clock_drift(), timeout=5)
        if not clock_ok:
            # Por defecto NO se pasa a papel: CCXT ya compensa tiempo y MODO_REAL=true
            # debe respetarse. Activa CLOCK_DRIFT_FORCE_PAPER=true si quieres el fallback estricto.
            force_paper = os.getenv("CLOCK_DRIFT_FORCE_PAPER", "false").strip().lower() in {
                "true",
                "1",
                "yes",
                "on",
            }
            if force_paper:
                await self._apply_clock_drift_safety()
            else:
                self.log.error(
                    "Desfase de reloj frente a Binance por encima del umbral; "
                    "se mantiene el modo del .env (CLOCK_DRIFT_FORCE_PAPER no está activo). "
                    "Sincroniza NTP en Windows, sube CLOCK_DRIFT_MAX_SECONDS o pon "
                    "CLOCK_DRIFT_FORCE_PAPER=true para forzar modo papel."
                )

        if not await self._check_storage():
            raise RuntimeError(
                'Storage no disponible. Verifica los permisos de escritura en el directorio de datos.'
            )

        if hasattr(self.trader, "habilitar_estrategias"):
            self.trader.habilitar_estrategias()
        else:
            self.log.debug("Trader sin habilitar_estrategias(); se omite la activación.")

        self._snapshot()
