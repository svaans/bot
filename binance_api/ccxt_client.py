"""Cliente CCXT (Binance spot) compartido para órdenes y mercados.

``real_orders`` y validadores esperan la API unificada de CCXT
(``create_market_buy_order``, ``load_markets``, etc.). Este módulo centraliza
una instancia reutilizable y la resolución de ``Config`` frente a
``obtener_cliente(config)`` erróneo con el cliente REST aiohttp.
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

BINANCE_CLIENT_ORDER_ID_MAX_LEN = 36

_LOCK = threading.RLock()
_EXCHANGE: Any = None
_EXCHANGE_KEY: tuple[Any, ...] | None = None


def reset_ccxt_exchange() -> None:
    """Invalida el singleton (tests o cambio de credenciales)."""

    global _EXCHANGE, _EXCHANGE_KEY
    with _LOCK:
        _EXCHANGE = None
        _EXCHANGE_KEY = None


def binance_spot_client_order_id(
    operation_id: str | None, *, attempt: int = 1
) -> str | None:
    """``newClientOrderId`` Binance spot: máx 36 caracteres, [a-zA-Z0-9-_].

    En reintentos, añade un sufijo para no reutilizar el id de una orden ya
    enviada (idempotencia del exchange).
    """

    if not operation_id:
        return None
    base = operation_id.replace("/", "").strip()
    if not base:
        return None
    if attempt <= 1:
        return base[:BINANCE_CLIENT_ORDER_ID_MAX_LEN]
    suffix = f"a{attempt}"
    sep = "-"
    max_len = BINANCE_CLIENT_ORDER_ID_MAX_LEN
    room = max_len - len(sep) - len(suffix)
    if room < 1:
        combined = f"{suffix}{sep}{base}"
        return combined[:max_len]
    trimmed = base[:room]
    return f"{trimmed}{sep}{suffix}"[:max_len]


def _effective_config(config: Any | None) -> Any:
    if config is not None:
        return config
    try:
        from config.config import cfg as app_cfg

        return app_cfg
    except Exception:
        return None


def _fingerprint(cfg: Any | None) -> tuple[Any, ...]:
    api_key = ""
    api_secret = ""
    testnet = False
    modo = ""
    staging = os.getenv("BINANCE_STAGING_REST_URL", "")
    if cfg is not None:
        api_key = (getattr(cfg, "api_key", None) or "") or ""
        api_secret = (getattr(cfg, "api_secret", None) or "") or ""
        mo = getattr(cfg, "modo_operativo", None)
        if mo is not None:
            modo = str(getattr(mo, "value", mo))
            try:
                testnet = bool(mo.uses_testnet)
            except Exception:
                testnet = False
    else:
        api_key = os.environ.get("BINANCE_API_KEY", "") or ""
        api_secret = os.environ.get("BINANCE_API_SECRET", "") or ""
    return (
        api_key[:16],
        len(api_key),
        len(api_secret),
        testnet,
        staging,
        modo,
    )


def _apply_staging_urls(exchange: Any, staging_raw: str) -> None:
    staging = staging_raw.strip().rstrip("/")
    if not staging:
        exchange.set_sandbox_mode(True)
        return
    if "testnet.binance" in staging:
        exchange.set_sandbox_mode(True)
        return
    if staging.endswith("/api/v3"):
        root = staging
    elif staging.endswith("/api"):
        root = f"{staging}/v3"
    else:
        root = f"{staging}/api/v3"
    exchange.urls["api"] = {
        "public": root,
        "private": root,
        "v3": root,
    }


def obtener_ccxt(config: Any | None = None) -> Any:
    """Devuelve instancia CCXT ``binance`` (spot), reutilizada por proceso."""

    global _EXCHANGE, _EXCHANGE_KEY
    cfg = _effective_config(config)
    fp = _fingerprint(cfg)
    with _LOCK:
        if _EXCHANGE is not None and _EXCHANGE_KEY == fp:
            return _EXCHANGE

        import ccxt

        api_key = ""
        api_secret = ""
        if cfg is not None:
            api_key = (getattr(cfg, "api_key", None) or "") or ""
            api_secret = (getattr(cfg, "api_secret", None) or "") or ""
        if not api_key:
            api_key = os.environ.get("BINANCE_API_KEY", "") or ""
        if not api_secret:
            api_secret = os.environ.get("BINANCE_API_SECRET", "") or ""

        exchange = ccxt.binance(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {
                    "defaultType": "spot",
                    # Evita Binance -1021 cuando el reloj local va unos cientos de ms
                    # por delante: fetch_markets() llama a load_time_difference() y el
                    # nonce usa milliseconds() - timeDifference (ver ccxt binance).
                    "adjustForTimeDifference": True,
                },
            }
        )

        mo = getattr(cfg, "modo_operativo", None) if cfg is not None else None
        testnet_active = False
        if mo is not None:
            try:
                testnet_active = bool(mo.uses_testnet)
            except Exception:
                testnet_active = False
            if testnet_active:
                staging = os.getenv("BINANCE_STAGING_REST_URL", "")
                _apply_staging_urls(exchange, staging)

        # ``load_markets()`` de CCXT invoca ``fetch_currencies()`` *antes* de
        # ``fetch_markets()``; la primera es firmada (sapi/v1/capital/config/getall).
        # ``load_time_difference()`` solo se disparaba dentro de ``fetch_markets()``,
        # así que esa primera firma iba con timeDifference=0 → Binance -1021.
        try:
            exchange.load_time_difference()
        except Exception as exc:
            logger.warning(
                "ccxt: load_time_difference falló (%s); riesgo de -1021 en la carga inicial",
                exc,
            )

        exchange.load_markets()
        _EXCHANGE = exchange
        _EXCHANGE_KEY = fp
        logger.info(
            "ccxt.binance listo",
            extra={"event": "ccxt_exchange_ready", "testnet": testnet_active},
        )
        return _EXCHANGE
