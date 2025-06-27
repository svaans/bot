#!/usr/bin/env python3
"""Launch required services and then start the trading bot."""
from __future__ import annotations
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

services = [
    subprocess.Popen(["go", "run", "main.go"], cwd=ROOT / "candle_service"),
    subprocess.Popen(["go", "run", "main.go"], cwd=ROOT / "orders_worker"),
]
try:
    subprocess.run([sys.executable, str(ROOT / "main.py")], check=True)
finally:
    for proc in services:
        proc.terminate()
    for proc in services:
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()