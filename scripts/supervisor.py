#!/usr/bin/env python3
"""Launch required services and then start the trading bot."""
from __future__ import annotations
import subprocess
import sys
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parent.parent

use_docker = shutil.which("go") is None

if use_docker:
    subprocess.run([
        "docker",
        "build",
        "-t",
        "candle_service",
        str(ROOT / "candle_service"),
    ], cwd=ROOT, check=True)
    subprocess.run([
        "docker",
        "build",
        "-t",
        "orders_worker",
        str(ROOT / "orders_worker"),
    ], cwd=ROOT, check=True)
    services = [
        subprocess.Popen([
            "docker",
            "run",
            "--rm",
            "-p",
            "9000:9000",
            "candle_service",
        ], cwd=ROOT),
        subprocess.Popen([
            "docker",
            "run",
            "--rm",
            "-p",
            "9100:9100",
            "orders_worker",
        ], cwd=ROOT),
    ]
else:
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
