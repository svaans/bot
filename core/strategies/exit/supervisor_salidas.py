from dataclasses import dataclass
from typing import Dict, Any
import pandas as pd

from core.utils import configurar_logger
from .salida_break_even import salida_break_even
from .salida_trailing_stop import verificar_trailing_stop
from .salida_stoploss import verificar_salida_stoploss

log = configurar_logger("supervisor_salidas")

@dataclass
class EstadoSalidas:
    """Estado compartido entre verificaciones de salida."""

    max_price: float
    duracion_en_velas: int


class SupervisorSalidas:
    """Gestiona el orden de evaluación de salidas básicas."""

    def __init__(self, orden: Any, config: Dict[str, Any] | None = None) -> None:
        self.orden = orden
        self.config = config or {}
        self.estado = EstadoSalidas(
            max_price=getattr(orden, "max_price", orden.precio_entrada),
            duracion_en_velas=getattr(orden, "duracion_en_velas", 0),
        )

    def _actualizar_estado(self, precio_actual: float) -> None:
        self.estado.max_price = max(self.estado.max_price, precio_actual)
        self.estado.duracion_en_velas += 1
        # Propaga el estado a la orden
        self.orden.max_price = self.estado.max_price
        self.orden.duracion_en_velas = self.estado.duracion_en_velas

    def evaluar(
        self,
        df: pd.DataFrame,
        precio_cierre: float | None = None,
        precio_min: float | None = None,
        precio_max: float | None = None,
    ) -> Dict[str, Any]:
        """Ejecuta las verificaciones de break-even, trailing, SL y TP."""
        if df is None or df.empty or not {"close", "high", "low"}.issubset(df.columns):
            return {"cerrar": False}

        if precio_cierre is None:
            precio_cierre = float(df["close"].iloc[-1])
        if precio_min is None:
            precio_min = float(df["low"].iloc[-1])
        if precio_max is None:
            precio_max = float(df["high"].iloc[-1])

        self._actualizar_estado(precio_cierre)

        resultado = {"cerrar": False, "razon": "", "break_even": False}

        # 1. Break Even
        res_be = salida_break_even(self.orden.to_dict(), df, config=self.config)
        if res_be.get("break_even"):
            nuevo_sl = res_be.get("nuevo_sl")
            if nuevo_sl is not None:
                self.orden.stop_loss = nuevo_sl
            resultado["break_even"] = True

        # 2. Trailing Stop
        if not resultado["cerrar"]:
            cerrar, motivo = verificar_trailing_stop(
                self.orden.to_dict(), precio_cierre, df, config=self.config
            )
            if cerrar:
                resultado.update({"cerrar": True, "razon": motivo})

        # 3. Stop Loss
        if not resultado["cerrar"] and precio_min <= self.orden.stop_loss:
            res_sl = verificar_salida_stoploss(
                self.orden.to_dict(), df, config=self.config
            )
            if res_sl.get("cerrar", False):
                resultado.update({"cerrar": True, "razon": res_sl.get("motivo", "Stop Loss")})

        # 4. Take Profit
        if not resultado["cerrar"] and precio_max >= self.orden.take_profit:
            direccion = 1 if self.orden.direccion in ("long", "compra") else -1
            beneficio = (
                (precio_cierre - self.orden.precio_entrada) / self.orden.precio_entrada
            ) * direccion
            if beneficio > 0:
                resultado.update({"cerrar": True, "razon": "Take Profit"})
            else:
                resultado.update({"cerrar": True, "razon": "Debilidad"})

        return resultado