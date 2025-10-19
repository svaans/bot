import os
from dotenv import load_dotenv

from core.operational_mode import OperationalMode

load_dotenv('config/claves.env')


def _resolve_mode() -> OperationalMode:
    raw = os.getenv('MODO_OPERATIVO') or os.getenv('BOT_MODE')
    if raw:
        return OperationalMode.parse(raw, default=OperationalMode.PAPER_TRADING)
    raw_bool = os.getenv('MODO_REAL')
    if raw_bool is None:
        return OperationalMode.PAPER_TRADING
    return OperationalMode.REAL if raw_bool.strip().lower() == 'true' else OperationalMode.PAPER_TRADING


MODO_OPERATIVO = _resolve_mode()
MODO_REAL = MODO_OPERATIVO.is_real
