"""Herramientas CLI para operaciones administrativas de órdenes."""
from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from core.utils.logger import configurar_logger

from .real_orders import reconciliar_trades_binance

log = configurar_logger("orders.cli", modo_silencioso=True)


def _parse_symbols(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    symbols = [chunk.strip().upper() for chunk in raw.split(",") if chunk.strip()]
    return symbols or None


def _handle_reconcile(args: argparse.Namespace) -> int:
    symbols = _parse_symbols(getattr(args, "symbols", None))
    divergencias = reconciliar_trades_binance(
        symbols,
        limit=int(getattr(args, "limit", 50)),
        apply_changes=bool(getattr(args, "apply", False)),
    )

    if getattr(args, "json", False):
        payload = {"divergences": divergencias, "count": len(divergencias)}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        if divergencias:
            print(f"⚠️ {len(divergencias)} divergencias detectadas:")
            for entry in divergencias:
                symbol = entry.get("symbol", "?")
                reason = entry.get("reason", "unknown")
                amount = entry.get("amount")
                local = entry.get("local_amount")
                side = entry.get("side")
                details = (
                    f" - {symbol} [{side}] motivo={reason} "
                    f"amount={amount} local={local}"
                )
                print(details)
        else:
            print("✅ Sin divergencias detectadas")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Utilidades administrativas de órdenes")
    subparsers = parser.add_subparsers(dest="command", required=True)

    reconcile = subparsers.add_parser(
        "reconcile-trades",
        help="Analiza trades recientes y detecta divergencias con el estado local",
    )
    reconcile.add_argument(
        "--symbols",
        help="Lista separada por comas de símbolos a reconciliar (por defecto todos)",
    )
    reconcile.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Número máximo de trades a consultar por símbolo",
    )
    reconcile.add_argument(
        "--apply",
        action="store_true",
        help="Aplica cambios automáticamente además de reportar",
    )
    reconcile.add_argument(
        "--json",
        action="store_true",
        help="Imprime el resultado en formato JSON",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    command = getattr(args, "command", None)
    if command == "reconcile-trades":
        return _handle_reconcile(args)

    log.error("Comando desconocido: %s", command)
    return 1


if __name__ == "__main__":  # pragma: no cover - punto de entrada manual
    sys.exit(main())
