"""Python wrapper for the Rust backtesting library with optional gRPC."""

from __future__ import annotations

import os
import json
from typing import Sequence

import grpc

from .rust_backtesting import run_backtest as _run_local
from core import backtesting_pb2, backtesting_pb2_grpc

__all__ = ["run_backtest"]


def run_backtest(symbols: Sequence[str], dataset_path: str):
    """Run the backtest either locally or via gRPC depending on env vars."""
    if os.getenv("USE_GRPC_BACKTEST"):
        host = os.getenv("BACKTEST_GRPC_HOST", "localhost")
        port = os.getenv("BACKTEST_GRPC_PORT", "9200")
        channel = grpc.insecure_channel(f"{host}:{port}")
        stub = backtesting_pb2_grpc.BacktestServiceStub(channel)
        req = backtesting_pb2.BacktestRequest(symbols=list(symbols), dataset_path=dataset_path)
        resp = stub.RunBacktest(req, timeout=3600)
        channel.close()
        return json.loads(resp.json_result)
    return _run_local(symbols, dataset_path)