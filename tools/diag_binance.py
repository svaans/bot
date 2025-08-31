#!/usr/bin/env python3
"""Herramienta de diagnóstico para credenciales de Binance."""

from __future__ import annotations

import os
import sys
from typing import List

import ccxt  # type: ignore
import requests


def _public_ip() -> str:
    try:
        return requests.get("https://api.ipify.org", timeout=10).text.strip()
    except Exception:
        return "X.X.X.X"


def main() -> int:
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    modo_real = os.getenv("MODO_REAL", "true").lower() == "true"
    testnet = os.getenv("BINANCE_TESTNET", "false").lower() == "true"
    default_type = os.getenv("BINANCE_DEFAULT_TYPE", "spot").lower()

    errores: List[str] = []
    acciones: List[str] = []

    if not api_key or not api_secret:
        errores.append("Faltan credenciales BINANCE_API_KEY o BINANCE_API_SECRET")
        if not api_key:
            acciones.append("define la variable de entorno BINANCE_API_KEY")
        if not api_secret:
            acciones.append("define la variable de entorno BINANCE_API_SECRET")
        _mostrar_resultado(errores, acciones)
        return 1

    exchange = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": default_type},
        }
    )

    esperado = testnet or not modo_real
    exchange.set_sandbox_mode(esperado)
    if getattr(exchange, "sandboxMode", esperado) != esperado:
        errores.append("sandbox_mode no coincide con modo_real/BINANCE_TESTNET")

    if default_type != "spot":
        errores.append(f"defaultType debe ser 'spot' y es '{default_type}'")

    try:
        exchange.fetch_ticker("BTC/USDT")
    except Exception as exc:  # pragma: no cover - solo diagnóstico
        errores.append(f"fetch_ticker falló: {exc}")

    if modo_real:
        try:
            exchange.fetch_balance()
        except Exception as exc:  # pragma: no cover - solo diagnóstico
            errores.append(f"fetch_balance falló: {exc}")
            ip = _public_ip()
            acciones.extend(
                [
                    f"añade IP {ip} a lista permitida",
                    "habilita Spot & Margin",
                ]
            )
            if testnet:
                acciones.append("desactiva testnet para mainnet")

    if errores:
        _mostrar_resultado(errores, acciones)
        return 1

    print("✅ Diagnóstico Binance OK")
    return 0


def _mostrar_resultado(errores: List[str], acciones: List[str]) -> None:
    print("❌ Diagnóstico Binance fallido")
    for err in errores:
        print(f"- {err}")
    if acciones:
        print("Acciones sugeridas:")
        for acc in acciones:
            print(f"- {acc}")


if __name__ == "__main__":
    sys.exit(main())
