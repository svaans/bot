"""Utility to normalize JSON logs emitted by the trading bot.

The module exposes a small CLI that reads JSON lines from a file or stdin,
removes ANSI escape sequences, tolerates non finite floats (NaN/Infinity) and
optionally merges the JSON payload embedded inside the ``message`` field.

Usage examples::

    # Pretty-print the live output of the bot, skipping malformed lines.
    python main.py 2>&1 | python -m tools.log_stream --pretty

    # Inspect only ERROR/WARNING entries stored in a jsonl log file.
    python -m tools.log_stream logs/bot.jsonl --level ERROR WARNING

The helper mirrors the ``jq`` pipeline recommended in the runbooks but provides
an all-Python alternative that works out of the box in any environment where
this repository is executed.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")
NON_FINITE_RE = re.compile(r":\s*(-?Infinity|NaN)(?=[,}\s])")


def _sanitize_line(raw_line: str) -> str:
    """Remove ANSI escape codes and replace non-finite JSON tokens."""

    line = ANSI_ESCAPE_RE.sub("", raw_line).strip()
    if not line:
        return ""
    return NON_FINITE_RE.sub(": null", line)


def _merge_message_payload(record: dict[str, object]) -> dict[str, object]:
    """Merge JSON payload embedded inside the ``message`` field."""

    message = record.get("message")
    if not isinstance(message, str):
        return record

    try:
        nested = json.loads(message)
    except json.JSONDecodeError:
        return record

    if isinstance(nested, dict):
        # Fields in the nested payload should override the top-level ones, as the
        # ``jq`` helper used in the runbook performs ``$r + $m``.
        merged: dict[str, object] = {**record, **nested}
        return merged
    return record


def iter_records(
    lines: Iterable[str],
    *,
    merge_message: bool = True,
) -> Iterator[dict[str, object]]:
    """Yield parsed JSON objects from ``lines`` ignoring malformed entries."""

    for raw_line in lines:
        sanitized = _sanitize_line(raw_line)
        if not sanitized:
            continue
        try:
            record: dict[str, object] = json.loads(sanitized)
        except json.JSONDecodeError:
            continue
        if merge_message:
            record = _merge_message_payload(record)
        yield record


def _filter_record(
    record: dict[str, object],
    *,
    loggers: Sequence[str] | None,
    level_whitelist: set[str] | None,
) -> bool:
    """Return ``True`` when ``record`` passes all provided filters."""

    if loggers:
        logger_name = record.get("logger")
        if not isinstance(logger_name, str) or logger_name not in loggers:
            return False
    if level_whitelist:
        level = record.get("level")
        if not isinstance(level, str) or level.upper() not in level_whitelist:
            return False
    return True


def process_stream(
    lines: Iterable[str],
    *,
    merge_message: bool = True,
    loggers: Sequence[str] | None = None,
    levels: Sequence[str] | None = None,
) -> List[dict[str, object]]:
    """Return the filtered JSON records extracted from ``lines``."""

    records: List[dict[str, object]] = []
    level_whitelist = {lvl.upper() for lvl in levels} if levels else None

    for record in iter_records(lines, merge_message=merge_message):
        if _filter_record(
            record,
            loggers=loggers,
            level_whitelist=level_whitelist,
        ):
            records.append(record)
    return records


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Stream trading bot logs, cleaning invalid JSON entries and merging "
            "nested message payloads."
        )
    )
    parser.add_argument(
        "path",
        nargs="?",
        type=Path,
        help="Optional log file in JSONL format. If omitted, stdin is used.",
    )
    parser.add_argument(
        "--no-merge-message",
        action="store_true",
        help="Disable merging JSON content embedded in the message field.",
    )
    parser.add_argument(
        "--logger",
        nargs="*",
        help="Only include records whose logger name matches one of the values.",
    )
    parser.add_argument(
        "--level",
        nargs="*",
        help="Only include records whose severity matches one of the values.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print each record using indentation.",
    )
    return parser


def _read_lines(path: Path | None) -> Iterable[str]:
    if path is None:
        return sys.stdin
    return path.open("r", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    lines = _read_lines(args.path)
    records = process_stream(
        lines,
        merge_message=not args.no_merge_message,
        loggers=args.logger,
        levels=args.level,
    )

    dump_kwargs = {"ensure_ascii": False}
    if args.pretty:
        dump_kwargs["indent"] = 2

    for record in records:
        json.dump(record, sys.stdout, **dump_kwargs)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())