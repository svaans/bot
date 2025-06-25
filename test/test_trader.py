import asyncio
from config.config_manager import Config
from core.trader_modular import Trader


class DummyClient:
    def fetch_balance(self):
        return {"total": {"EUR": 100}}


def test_calcular_cantidad(monkeypatch):
    monkeypatch.setattr("binance_api.cliente.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.calcular_fraccion_kelly", lambda: 1.0)
    monkeypatch.setattr("core.trader_modular.cargar_pesos_estrategias", lambda: {})
    monkeypatch.setattr("core.ordenes_reales.obtener_todas_las_ordenes", lambda: {})

    cfg = Config(
        api_key="k",
        api_secret="s",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=10,
    )

    trader = Trader(cfg)
    qty = trader._calcular_cantidad("BTC/EUR", 10)
    assert qty == 10.0


def _patch_trader_deps(monkeypatch):
    monkeypatch.setattr("binance_api.cliente.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.calcular_fraccion_kelly", lambda: 1.0)
    monkeypatch.setattr("core.trader_modular.cargar_pesos_estrategias", lambda: {})
    monkeypatch.setattr("core.orders.real_orders.obtener_todas_las_ordenes", lambda: {})


def test_trader_carga_estado(monkeypatch):
    _patch_trader_deps(monkeypatch)

    called = {}

    def fake_cargar():
        called["ok"] = True
        return {"x": {}}, {"x": 1.0}

    monkeypatch.setattr("core.storage.persistencia.cargar_estado", fake_cargar)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    cfg = Config(
        api_key="k",
        api_secret="s",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=10,
    )

    trader = Trader(cfg)
    assert called.get("ok")
    assert trader.historial_cierres == {"x": {}}
    assert trader.capital_por_simbolo.get("x") == 1.0


def test_trader_guarda_estado(monkeypatch):
    _patch_trader_deps(monkeypatch)

    saved = {}

    def fake_guardar(hist, cap, carpeta="estado"):
        saved["hist"] = hist
        saved["cap"] = cap

    monkeypatch.setattr("core.storage.persistencia.guardar_estado", fake_guardar)

    cfg = Config(
        api_key="k",
        api_secret="s",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=10,
    )

    trader = Trader(cfg)
    asyncio.run(trader.cerrar())

    assert saved["hist"] is trader.historial_cierres
    assert saved["cap"] is trader.capital_por_simbolo