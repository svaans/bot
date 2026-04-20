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

        # La sincronización del reloj ya se hizo al principio de
        # ``StartupManager.run`` (antes de crear CCXT / abrir WS) para evitar
        # que un salto de reloj deje obsoleto el ``timeDifference`` cacheado
        # por CCXT y dispare Binance -1021. Aquí solo verificamos el drift.
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
                    "Sincroniza NTP en Windows, sube CLOCK_DRIFT_MAX_SECONDS, pon "
                    "BINANCE_SYNC_SYSTEM_TIME=elevate para auto-elevar, o "
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

    async def _auto_sync_system_clock(self) -> None:
        """Intenta sincronizar el reloj del SO con la hora UTC de Binance.

        Lee ``BINANCE_SYNC_SYSTEM_TIME`` y delega en
        :func:`core.utils.binance_time_sync.auto_sync_system_clock_from_binance`.
        No propaga excepciones: cualquier fallo solo se registra para no abortar
        el arranque del bot.
        """
        try:
            from core.utils.binance_time_sync import (
                auto_sync_system_clock_from_binance,
            )

            outcome = await asyncio.to_thread(auto_sync_system_clock_from_binance)
        except Exception as exc:  # pragma: no cover - defensivo
            self.log.debug(
                "auto_sync_system_clock_from_binance falló: %s", exc, exc_info=True
            )
            return

        mode = outcome.get("mode")
        method = outcome.get("method")
        synced = bool(outcome.get("synced"))

        if mode == "off":
            return

        if synced:
            self.log.info(
                "Reloj del sistema alineado con la hora UTC de Binance.",
                extra={"event": "clock.autosync.ok", "clock_sync": outcome},
            )
            # Si CCXT ya había cacheado ``timeDifference`` con el reloj viejo,
            # refrescarlo ahora evita un Binance -1021 en la primera llamada
            # firmada tras el salto de reloj.
            await asyncio.to_thread(self._refresh_ccxt_time_difference)
            return

        if method == "SetSystemTime:requires_admin":
            self.log.warning(
                "Sincronización automática omitida: el proceso no es administrador. "
                "Relanza el bot como admin o define BINANCE_SYNC_SYSTEM_TIME=elevate "
                "para aceptar un popup UAC en cada arranque.",
                extra={"event": "clock.autosync.skip", "clock_sync": outcome},
            )
            return

        self.log.warning(
            "No se pudo sincronizar el reloj automáticamente (%s).",
            method,
            extra={"event": "clock.autosync.fail", "clock_sync": outcome},
        )

    def _refresh_ccxt_time_difference(self) -> None:
        """Refresca el ``timeDifference`` del singleton CCXT si ya existe.

        Tras un salto de reloj del SO, el offset cacheado por CCXT queda
        obsoleto; si se deja así, Binance responde ``-1021`` en la primera
        petición firmada. Este helper es no-op si el cliente aún no se ha
        creado (caso normal cuando la sync se ejecuta al inicio del arranque).
        """
        try:
            from binance_api import ccxt_client as _cc
        except Exception:
            return
        exchange = getattr(_cc, "_EXCHANGE", None)
        if exchange is None:
            return
        try:
            exchange.load_time_difference()
            self.log.info(
                "CCXT.timeDifference refrescado tras sincronización de reloj.",
                extra={"event": "clock.autosync.ccxt_refresh"},
            )
        except Exception as exc:  # pragma: no cover - red externa
            self.log.debug(
                "No se pudo refrescar CCXT.timeDifference tras sync: %s",
                exc,
                exc_info=True,
            )
