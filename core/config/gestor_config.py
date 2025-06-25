from __future__ import annotations

import json
from pathlib import Path
from typing import Dict


_RUTA_CONFIG = Path("config/configuraciones_optimas.json")
_CACHE: Dict[str, dict] | None = None


def cargar_config_optima() -> Dict[str, dict]:
    """Carga y cachea las configuraciones optimizadas para los símbolos."""
    global _CACHE
    if _CACHE is None:
        if _RUTA_CONFIG.exists():
            with open(_RUTA_CONFIG, "r", encoding="utf-8") as fh:
                _CACHE = json.load(fh)
        else:
            _CACHE = {}
    return _CACHE