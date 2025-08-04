import asyncio
from config.config_manager import Config
from core.trader_modular import Trader


class DummyClient:

    def fetch_balance(self):
        return {'total': {'EUR': 100}}


def test_calcular_cantidad(monkeypatch):
    monkeypatch.setattr('binance_api.cliente.crear_cliente', lambda config=
        None: DummyClient())
    monkeypatch.setattr('core.trader_modular.crear_cliente', lambda config=
        None: DummyClient())
    monkeypatch.setattr('core.trader_modular.calcular_fraccion_kelly', lambda :
        1.0)
    monkeypatch.setattr('core.trader_modular.cargar_pesos_estrategias', lambda
        : {})
    monkeypatch.setattr('core.ordenes_reales.obtener_todas_las_ordenes', lambda
        : {})
    cfg = Config(api_key='k', api_secret='s', modo_real=False,
        intervalo_velas='1m', symbols=['BTC/EUR'], umbral_riesgo_diario=0.1,
        min_order_eur=10)
    trader = Trader(cfg)
    qty = asyncio.run(trader._calcular_cantidad_async('BTC/EUR', 10))
    assert qty == 10.0
