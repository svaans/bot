import pytest
import asyncio
from config.config_manager import Config
from core.capital_manager import CapitalManager
from core.risk.risk_manager import RiskManager
from core.trader_modular import Trader
from core.orders.order_service import OrderServiceSimulado

class DummyClient:
    def fetch_balance(self):
        return {"total": {"EUR": 100}}


def _patch_trader_base(monkeypatch, kelly_value=0.1):
    monkeypatch.setattr("binance_api.cliente.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.cargar_pesos_estrategias", lambda: {})
    monkeypatch.setattr("core.trader_modular.calcular_fraccion_kelly", lambda *a, **k: kelly_value)
    monkeypatch.setattr("core.trader_modular.RiskManager.multiplicador_kelly", lambda self: 1.0)
    monkeypatch.setattr("core.trader_modular.RiskManager.factor_volatilidad", lambda self, *a, **k: 1.0)
    monkeypatch.setattr("config.configuracion.cargar_configuracion_simbolo", lambda s: {"riesgo_maximo_diario": 1.0})
    monkeypatch.setattr("core.trader_modular.Trader._obtener_historico", lambda self, sym: None)
    monkeypatch.setattr("core.data_feed.DataFeed.detener", lambda self: asyncio.sleep(0))
    monkeypatch.setattr("core.contexto_externo.StreamContexto.detener", lambda self: asyncio.sleep(0))


def test_min_order_configurable():
    cfg = Config(
        api_key="k",
        api_secret="s",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=5.0,
    )
    risk = RiskManager(0.1)
    cm = CapitalManager(cfg, None, risk, fraccion_kelly=1.0, min_order_eur=5.0, min_order_symbol=None)
    cm.capital_por_simbolo["BTC/EUR"] = 4.0
    qty = asyncio.run(cm.calcular_cantidad_async("BTC/EUR", 100))
    assert qty == 0.0


def test_kelly_dinamica(monkeypatch):
    _patch_trader_base(monkeypatch, kelly_value=0.1)
    cfg = Config(
        api_key="k",
        api_secret="s",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=10.0,
    )
    trader = Trader(cfg, order_service=OrderServiceSimulado())
    assert trader.capital_manager.fraccion_kelly == 0.1
    monkeypatch.setattr("core.trader_modular.calcular_fraccion_kelly", lambda *a, **k: 0.3)
    trader.actualizar_fraccion_kelly()
    assert trader.capital_manager.fraccion_kelly == 0.3


def test_riesgo_maximo_diario():
    cfg = Config(
        api_key="k",
        api_secret="s",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=10.0,
    )
    risk = RiskManager(1.0)
    cm = CapitalManager(
        cfg,
        None,
        risk,
        fraccion_kelly=1.0,
        min_order_eur=10.0,
        min_order_symbol=None,
        riesgo_maximo_diario={"BTC/EUR": 0.05},
    )
    qty = asyncio.run(cm.calcular_cantidad_async("BTC/EUR", 10))
    assert qty * 10 <= 50.0
    assert qty * 10 == pytest.approx(50.0)


def test_umbral_sincronizado(monkeypatch):
    cfg = Config(
        api_key="k",
        api_secret="s",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=10.0,
    )
    risk = RiskManager(0.1)
    cm = CapitalManager(cfg, None, risk, fraccion_kelly=1.0, min_order_eur=10.0, min_order_symbol=None)
    qty_before = asyncio.run(cm.calcular_cantidad_async("BTC/EUR", 100))

    def fake_ajustar(self, metrics):
        self.umbral = 0.05
        return self.umbral

    monkeypatch.setattr(risk, "ajustar_umbral", fake_ajustar.__get__(risk, RiskManager))
    risk.ajustar_umbral({})
    qty_after = asyncio.run(cm.calcular_cantidad_async("BTC/EUR", 100))
    assert qty_after < qty_before
    assert cm.risk.umbral == 0.05
    assert qty_after * 100 == pytest.approx(50.0)