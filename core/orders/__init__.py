from importlib import import_module
__all__ = ['OrderManager', 'Order', 'ejecutar_orden_market',
    'ejecutar_orden_market_sell', 'registrar_orden', 'eliminar_orden',
    'obtener_orden', 'obtener_todas_las_ordenes', 'sincronizar_ordenes_binance'
    ]


def __getattr__(name):
    if name == 'OrderManager':
        return import_module('.order_manager', __name__).OrderManager
    if name == 'Orden':
        return import_module('.order_model', __name__).Order
    if name in __all__:
        return getattr(import_module('.real_orders', __name__), name)
    raise AttributeError(name)
