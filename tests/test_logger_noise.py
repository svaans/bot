"""Tests para validar el filtrado de loggers ruidosos."""

from __future__ import annotations

import logging


def test_noisy_loggers_escalated():
    """Los loggers de websockets y binance deben elevarse al menos a INFO."""

    from core.utils import logger as logger_module

    websockets_logger = logging.getLogger("websockets.client")
    protocol_logger = logging.getLogger("websockets.protocol")
    binance_logger = logging.getLogger("binance_api.websocket")

    original_levels = {
        "websockets.client": websockets_logger.level,
        "websockets.protocol": protocol_logger.level,
        "binance_api.websocket": binance_logger.level,
    }

    try:
        websockets_logger.setLevel(logging.DEBUG)
        protocol_logger.setLevel(logging.DEBUG)
        binance_logger.setLevel(logging.DEBUG)

        logger_module._configure_noisy_loggers()

        assert websockets_logger.level >= logging.INFO
        assert protocol_logger.level >= logging.INFO
        assert binance_logger.level >= logging.INFO
    finally:
        for name, level in original_levels.items():
            target_level = level if level not in (0, logging.NOTSET) else logging.NOTSET
            logging.getLogger(name).setLevel(target_level)
        logger_module._configure_noisy_loggers()
