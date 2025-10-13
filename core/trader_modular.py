"""Compatibilidad histórica para el trader modular.

Este módulo mantiene las rutas de importación previas delegando en el paquete
:mod:`core.trader`, que ahora aloja la implementación dividida en componentes.
"""
from __future__ import annotations

from typing import Any

try:
    from data_feed.lite import DataFeed
except ModuleNotFoundError:  # pragma: no cover
    from data_feed import DataFeed  # type: ignore

try:
    from core.supervisor import Supervisor
except ModuleNotFoundError:  # pragma: no cover
    from supervisor import Supervisor  # type: ignore

try:  # pragma: no cover
    from binance_api.cliente import crear_cliente as _crear_cliente_impl
except Exception:  # pragma: no cover
    _crear_cliente_impl = None  # type: ignore

from core import trader as _trader

TraderLite = _trader.TraderLite
Trader = _trader.Trader
EstadoSimbolo = _trader.EstadoSimbolo
TraderComponentFactories = _trader.TraderComponentFactories
ComponentResolutionError = _trader.ComponentResolutionError

_is_awaitable = _trader._is_awaitable
_maybe_await = _trader._maybe_await
_silence_task_result = _trader._silence_task_result
_normalize_timestamp = _trader._normalize_timestamp
_reason_none = _trader._reason_none
tf_seconds = _trader.tf_seconds

__all__ = list(_trader.__all__)


def crear_cliente(*args: Any, **kwargs: Any) -> Any:
    """Crea un cliente de Binance respetando la configuración del bot.

    Este *shim* mantiene compatibilidad con implementaciones antiguas que
    importaban ``crear_cliente`` desde :mod:`core.trader_modular` pero añade un
    comportamiento extra: cuando se recibe un objeto de configuración como
    primer argumento, se extraen automáticamente las credenciales y se fuerza
    el modo real si ``modo_real`` es ``True``.

    Parameters
    ----------
    args, kwargs:
        Se reenvían al creador original del cliente. Si el primer argumento es
        un objeto con atributos ``api_key``/``api_secret``/``modo_real`` se
        utiliza como fuente de configuración.
    """

    if _crear_cliente_impl is None:  # pragma: no cover - entorno degradado
        raise RuntimeError("binance_api.cliente.crear_cliente no disponible")

    if args:
        candidato = args[0]
        es_config = False
        if not isinstance(candidato, (str, bytes, type(None))):
            for attr in ("api_key", "modo_real", "api_secret"):
                if hasattr(candidato, attr):
                    es_config = True
                    break
        if es_config:
            config = candidato
            resto = args[1:]
            if resto:
                raise TypeError(
                    "crear_cliente(config, ...) no acepta argumentos posicionales adicionales"
                )

            api_key = getattr(config, "api_key", None)
            api_secret = getattr(config, "api_secret", None)

            simulated = kwargs.pop("simulated", None)
            if simulated is None:
                prefer_sim = getattr(config, "binance_simulado", None)
                if prefer_sim is None:
                    prefer_sim = getattr(config, "binance_simulated", None)
                if prefer_sim is None:
                    prefer_sim = getattr(config, "simulated", None)
                if prefer_sim is not None:
                    simulated = bool(prefer_sim)
                else:
                    simulated = not bool(getattr(config, "modo_real", False))

            testnet = kwargs.pop("testnet", None)
            if testnet is None:
                prefer_testnet = getattr(config, "binance_testnet", None)
                if prefer_testnet is None:
                    prefer_testnet = getattr(config, "testnet", None)
                if prefer_testnet is not None:
                    testnet = bool(prefer_testnet)
                else:
                    testnet = False

            return _crear_cliente_impl(
                api_key=api_key,
                api_secret=api_secret,
                testnet=testnet,
                simulated=simulated,
                **kwargs,
            )

    return _crear_cliente_impl(*args, **kwargs)


__all__.append("crear_cliente")
