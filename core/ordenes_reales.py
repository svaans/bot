"""Stub del modulo de ordenes reales para pruebas."""

def ejecutar_orden_market(*args, **kwargs):
    return {'ejecutado': 0.0, 'restante': 0.0, 'status': 'FILLED', 'min_qty': 0.0, 'fee': 0.0, 'pnl': 0.0}

def registrar_orden(*args, **kwargs):
    return None

def eliminar_orden(*args, **kwargs):
    return None

def obtener_todas_las_ordenes(*args, **kwargs):
    return {}

def sincronizar_ordenes_binance(*args, **kwargs):
    return {}

def ejecutar_orden_market_sell(*args, **kwargs):
    return {'ejecutado': 0.0, 'restante': 0.0, 'status': 'FILLED', 'min_qty': 0.0, 'fee': 0.0, 'pnl': 0.0}