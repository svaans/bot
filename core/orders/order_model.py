from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional
import json
import math


def normalizar_precio_cantidad(market_info: dict, precio: float, cantidad: float,
                               direccion: str = 'long') -> tuple[float, float]:
    """Ajusta ``precio`` y ``cantidad`` a las restricciones del mercado.

    Se aplican ``tickSize``, ``stepSize`` y ``minNotional`` tomando los datos
    de ``market_info``. El redondeo de ``precio`` respeta la ``direccion`` de la
    orden para evitar que una operación de venta quede por debajo del valor
    deseado.

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
    precision_price = market_info.get('precision', {}).get('price', 8)
    precision_amount = market_info.get('precision', {}).get('amount', 8)
    tick_size = 10 ** -precision_price
    step_size = 10 ** -precision_amount
    min_notional = float(
        market_info.get('limits', {}).get('cost', {}).get('min') or 0
    )
    min_amount = float(
        market_info.get('limits', {}).get('amount', {}).get('min') or 0
    )
    precio = ajustar_tick_size(precio, tick_size, direccion)
    if step_size > 0:
        cantidad = math.floor(cantidad / step_size) * step_size

    if min_amount and step_size > 0 and cantidad < min_amount:
        cantidad = math.ceil(min_amount / step_size) * step_size

    if (
        min_notional
        and precio
        and step_size > 0
        and precio * cantidad < min_notional
    ):
        cantidad = math.ceil(min_notional / precio / step_size) * step_size

    if step_size > 0:
        cantidad = math.floor(cantidad / step_size) * step_size
    return precio, cantidad


def ajustar_tick_size(precio: float, tick_size: float, direccion: str = 'long') -> float:
    """Ajusta un precio al múltiplo de ``tick_size`` según la dirección."""
    if tick_size <= 0:
        return precio
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
    detalles_tecnicos: dict | None = None
    sl_evitar_info: list | None = None
    break_even_activado: bool = False
    duracion_en_velas: int = 0
    intentos_cierre: int = 0
    sl_emergencia: float | None = None
    cerrando: bool = False
    fee_total: float = 0.0
    pnl_operaciones: float = 0.0
    registro_pendiente: bool = False

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
        data.setdefault('detalles_tecnicos', None)
        data.setdefault('sl_evitar_info', [])
        data.setdefault('break_even_activado', False)
        data.setdefault('duracion_en_velas', 0)
        data.setdefault('intentos_cierre', 0)
        data.setdefault('sl_emergencia', None)
        data.setdefault('cerrando', False)
        data.setdefault('fee_total', 0.0)
        data.setdefault('pnl_operaciones', 0.0)
        data.setdefault('registro_pendiente', False)
        return Order(**data)

    def to_dict(self) ->Dict[str, Any]:
        return asdict(self)

    def to_parquet_record(self) ->Dict[str, Any]:
        data = asdict(self)
        if isinstance(data.get('estrategias_activas'), dict):
            data['estrategias_activas'] = json.dumps(data[
                'estrategias_activas'])
        return data
