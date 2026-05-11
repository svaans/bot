from dataclasses import dataclass, asdict
from decimal import ROUND_CEILING, ROUND_DOWN, Decimal, InvalidOperation
from typing import Dict, Any, Optional
import json
import math


def _to_decimal(value: float) -> Decimal:
    """Convierte a Decimal vía str para evitar errores de representación binaria."""
    return Decimal(str(value))


def normalizar_precio_cantidad(filtros: Dict[str, float], precio: float, cantidad: float,
                               direccion: str = 'long') -> tuple[float, float]:
    """Ajusta ``precio`` y ``cantidad`` según los filtros de mercado.

    Parameters
    ----------
    market_info:
        Información del mercado tal y como la expone CCXT.
    precio:
        Precio objetivo de la orden.
    cantidad:
        Cantidad de la orden.
    direccion:
        ``'long'``/``'compra'`` o ``'short'``/``'venta'`` para determinar si el
        redondeo del precio debe realizarse hacia arriba o hacia abajo.

    Returns
    -------
    tuple[float, float]
        Precio y cantidad ajustados.
    """
    tick_size = filtros.get('tick_size', 0.0)
    step_size = filtros.get('step_size', 0.0)
    min_notional = filtros.get('min_notional', 0.0)
    min_amount = filtros.get('min_qty', 0.0)

    precio = ajustar_tick_size(precio, tick_size, direccion)

    def _floor_step(q: float) -> float:
        if step_size <= 0:
            return q
        try:
            return float(
                (_to_decimal(q) / _to_decimal(step_size)).to_integral_value(
                    rounding=ROUND_DOWN
                ) * _to_decimal(step_size)
            )
        except InvalidOperation:
            return math.floor(q / step_size) * step_size

    def _ceil_step(q: float) -> float:
        if step_size <= 0:
            return q
        try:
            return float(
                (_to_decimal(q) / _to_decimal(step_size)).to_integral_value(
                    rounding=ROUND_CEILING
                ) * _to_decimal(step_size)
            )
        except InvalidOperation:
            return math.ceil(q / step_size) * step_size

    cantidad = _floor_step(cantidad)

    if min_amount and step_size > 0 and cantidad < min_amount:
        cantidad = _ceil_step(min_amount)

    if (
        min_notional
        and precio
        and step_size > 0
        and precio * cantidad < min_notional
    ):
        cantidad = _ceil_step(min_notional / precio)

    # Final snap: min_notional ceil may itself need flooring to step boundary.
    cantidad = _floor_step(cantidad)
    return precio, cantidad


def ajustar_tick_size(precio: float, tick_size: float, direccion: str = 'long') -> float:
    """Ajusta un precio al múltiplo de ``tick_size`` según la dirección.

    Usa ``Decimal`` para evitar errores de representación flotante en ticks
    pequeños (< 0.001), coherente con :mod:`core.risk.sizing` y
    :mod:`core.risk.level_validators`.
    """
    if tick_size <= 0:
        return precio
    try:
        dec_precio = _to_decimal(precio)
        dec_tick = _to_decimal(tick_size)
        if direccion in ('short', 'venta'):
            rounded = (dec_precio / dec_tick).to_integral_value(rounding=ROUND_CEILING)
        else:
            rounded = (dec_precio / dec_tick).to_integral_value(rounding=ROUND_DOWN)
        return float(rounded * dec_tick)
    except InvalidOperation:
        factor = precio / tick_size
        if direccion in ('short', 'venta'):
            return math.ceil(factor) * tick_size
        return math.floor(factor) * tick_size


@dataclass
class Order:
    symbol: str
    precio_entrada: float
    cantidad: float
    stop_loss: float
    take_profit: float
    timestamp: str
    estrategias_activas: Dict[str, Any]
    tendencia: str
    max_price: float
    direccion: str = 'long'
    cantidad_abierta: float = 0.0
    parcial_cerrado: bool = False
    entradas: list | None = None
    fracciones_totales: int = 1
    fracciones_restantes: int = 0
    precio_ultima_piramide: float = 0.0
    precio_cierre: Optional[float] = None
    fecha_cierre: Optional[str] = None
    motivo_cierre: Optional[str] = None
    retorno_total: Optional[float] = None
    puntaje_entrada: float = 0.0
    umbral_entrada: float = 0.0
    score_tecnico: float = 0.0
    detalles_tecnicos: dict | None = None
    sl_evitar_info: list | None = None
    break_even_activado: bool = False
    duracion_en_velas: int = 0
    intentos_cierre: int = 0
    sl_emergencia: float | None = None
    cerrando: bool = False
    fee_total: float = 0.0
    pnl_realizado: float = 0.0
    pnl_latente: float = 0.0
    registro_pendiente: bool = False
    operation_id: str | None = None
    # Targets de TP multi-nivel ya ejecutados (list[dict] con porcentaje+qty_frac).
    # Permite a salida_takeprofit_atr omitir niveles ya procesados en velas previas.
    targets_alcanzados: list | None = None
    # [VERIF-TRAILING-STATE-LOST-01] Nivel activo del trailing stop (precio absoluto).
    # verificar_trailing_stop() escribe sl_trailing al dict-copia de Order, que se
    # perdía cada vela impidiendo el guard de movimiento mínimo (2 × tick_size).
    # Ahora se persiste en el Order real y to_dict() lo incluye automáticamente.
    sl_trailing: float | None = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) ->'Order':
        estrategias = data.get('estrategias_activas')
        if isinstance(estrategias, str):
            try:
                estrategias = json.loads(estrategias.replace("'", '"'))
            except json.JSONDecodeError:
                estrategias = {}
        data['estrategias_activas'] = estrategias or {}
        tendencia = data.get('tendencia')
        if isinstance(tendencia, (list, tuple)):
            data['tendencia'] = tendencia[0] if tendencia else ''
        if 'cantidad_abierta' not in data:
            data['cantidad_abierta'] = data.get('cantidad', 0.0)
        if 'parcial_cerrado' not in data:
            data['parcial_cerrado'] = False
        data.setdefault('entradas', [])
        data.setdefault('fracciones_totales', 1)
        data.setdefault('fracciones_restantes', 0)
        data.setdefault('precio_ultima_piramide', data.get('precio_entrada',
            0.0))
        data.setdefault('puntaje_entrada', 0.0)
        data.setdefault('umbral_entrada', 0.0)
        data.setdefault('score_tecnico', 0.0)
        data.setdefault('detalles_tecnicos', None)
        data.setdefault('sl_evitar_info', [])
        data.setdefault('break_even_activado', False)
        data.setdefault('duracion_en_velas', 0)
        data.setdefault('intentos_cierre', 0)
        data.setdefault('sl_emergencia', None)
        data.setdefault('cerrando', False)
        legacy_pnl = data.pop('pnl_operaciones', None)
        data.setdefault('fee_total', 0.0)
        data.setdefault('pnl_realizado', legacy_pnl if legacy_pnl is not None else 0.0)
        data.setdefault('pnl_latente', 0.0)
        data.setdefault('registro_pendiente', False)
        data.setdefault('operation_id', None)
        # [VERIF-TRAILING-STATE-LOST-01] Nivel activo del trailing stop; None para
        # órdenes antiguas sin este campo persistido en BD.
        data.setdefault('sl_trailing', None)
        # P6-F5-LATENT fix: deserializar desde TEXT si viene de SQLite.
        ta = data.get('targets_alcanzados')
        if isinstance(ta, str):
            try:
                data['targets_alcanzados'] = json.loads(ta)
            except (json.JSONDecodeError, ValueError):
                data['targets_alcanzados'] = None
        else:
            data.setdefault('targets_alcanzados', None)
        return Order(**data)

    def to_dict(self) ->Dict[str, Any]:
        return asdict(self)

    def to_parquet_record(self) ->Dict[str, Any]:
        data = asdict(self)
        if isinstance(data.get('estrategias_activas'), dict):
            data['estrategias_activas'] = json.dumps(data[
                'estrategias_activas'])
        return data

    @property
    def pnl_operaciones(self) -> float:
        """Compatibilidad retro: suma de PnL realizado y latente."""

        return float(self.pnl_realizado) + float(self.pnl_latente)

    @pnl_operaciones.setter
    def pnl_operaciones(self, value: float) -> None:
        """Mantiene compatibilidad asignando al PnL realizado."""

        self.pnl_realizado = float(value)
        self.pnl_latente = 0.0
