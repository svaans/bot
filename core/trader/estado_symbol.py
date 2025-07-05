from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class SymbolState:
    """Estado de un símbolo para el trader."""
    buffer: List[Dict[str, Any]] = field(default_factory=list)
    ultimo_timestamp: int | None = None
    tendencia_detectada: str = ''