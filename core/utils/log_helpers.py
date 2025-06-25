from __future__ import annotations
import json


def build_log_message(event: str, **fields: object) -> str:
    """Return a JSON string with ``event`` and extra ``fields``."""
    payload = {"event": event, **fields}
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        detalles = ", ".join(f"{k}={v}" for k, v in payload.items())
        return detalles