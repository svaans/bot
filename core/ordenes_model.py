from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional
import json

@dataclass
class Orden:
    symbol: str
    precio_entrada: float
    cantidad: float
    stop_loss: float
    take_profit: float
    timestamp: str
    estrategias_activas: Dict[str, Any]
    tendencia: str
    max_price: float
    direccion: str = "long"
    precio_cierre: Optional[float] = None
    fecha_cierre: Optional[str] = None
    motivo_cierre: Optional[str] = None
    retorno_total: Optional[float] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Orden":
        estrategias = data.get("estrategias_activas")
        if isinstance(estrategias, str):
            try:
                estrategias = json.loads(estrategias.replace("'", '"'))
            except json.JSONDecodeError:
                estrategias = {}
        data["estrategias_activas"] = estrategias or {}

        tendencia = data.get("tendencia")
        if isinstance(tendencia, (list, tuple)):
            data["tendencia"] = tendencia[0] if tendencia else ""

        return Orden(**data)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_parquet_record(self) -> Dict[str, Any]:
        data = asdict(self)
        if isinstance(data.get("estrategias_activas"), dict):
            data["estrategias_activas"] = json.dumps(data["estrategias_activas"])
        return data