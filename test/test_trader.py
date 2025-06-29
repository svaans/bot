import asyncio
from config.config_manager import Config
from core.trader_modular import Trader
from core.orders.order_service import OrderServiceSimulado


class DummyClient:
    def fetch_balance(self):
        return {"total": {"EUR": 100}}


def test_calcular_cantidad(monkeypatch):
    monkeypatch.setattr("binance_api.cliente.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.calcular_fraccion_kelly", lambda: 1.0)
    monkeypatch.setattr("core.trader_modular.cargar_pesos_estrategias", lambda: {})
    monkeypatch.setattr(
        "config.configuracion.cargar_configuracion_simbolo",
        lambda s: {"riesgo_maximo_diario": 1.0},
    )
    cfg = Config(
        api_key="k",
        api_secret="s",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=10,
    )

    trader = Trader(cfg, order_service=OrderServiceSimulado())
    qty = trader._calcular_cantidad("BTC/EUR", 10)
    assert qty == 10.0


def _patch_trader_deps(monkeypatch):
    monkeypatch.setattr("binance_api.cliente.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.calcular_fraccion_kelly", lambda: 1.0)
    monkeypatch.setattr("core.trader_modular.cargar_pesos_estrategias", lambda: {})
    monkeypatch.setattr("core.data_feed.DataFeed.detener", lambda self: asyncio.sleep(0))
    monkeypatch.setattr("core.contexto_externo.StreamContexto.detener", lambda self: asyncio.sleep(0))
    monkeypatch.setattr(
        "config.configuracion.cargar_configuracion_simbolo",
        lambda s: {"riesgo_maximo_diario": 1.0},
    )

def test_trader_carga_estado(monkeypatch):
    _patch_trader_deps(monkeypatch)

    called = {}

    def fake_cargar():
        called["ok"] = True
        return {"x": {}}, {"x": 1.0}

    monkeypatch.setattr("core.trader_modular.cargar_estado", fake_cargar)
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

    trader = Trader(cfg, order_service=OrderServiceSimulado())
    assert called.get("ok")
    assert trader.historial_cierres == {"x": {}}
    assert trader.capital_por_simbolo.get("x") == 1.0


def test_trader_guarda_estado(monkeypatch):
    _patch_trader_deps(monkeypatch)

    saved = {}

    def fake_guardar(hist, cap, carpeta="estado"):
        saved["hist"] = hist
        saved["cap"] = cap

    monkeypatch.setattr("core.trader_modular.guardar_estado", fake_guardar)

    cfg = Config(
        api_key="k",
        api_secret="s",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=10,
    )

    trader = Trader(cfg, order_service=OrderServiceSimulado())
    asyncio.run(trader.cerrar())

    assert saved["hist"] is trader.historial_cierres
    assert saved["cap"] is trader.capital_por_simbolo


def test_tareas_se_cancelan_al_cerrar(monkeypatch):
    _patch_trader_deps(monkeypatch)

    canceladas = set()

    def dummy(name):
        async def _d(*a, **k):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                canceladas.add(name)
                raise
        return _d

    monkeypatch.setattr("core.data_feed.DataFeed.escuchar", dummy("feed"))
    monkeypatch.setattr(
        "core.data_feed.DataFeed.detener", lambda self: asyncio.sleep(0)
    )
    monkeypatch.setattr(
        "core.trader_modular.monitorear_estado_periodicamente", dummy("estado")
    )
    monkeypatch.setattr(
        "core.contexto_externo.StreamContexto.escuchar", dummy("contexto")
    )
    monkeypatch.setattr(
        "core.contexto_externo.StreamContexto.detener", lambda self: asyncio.sleep(0)
    )
    monkeypatch.setattr(
        "core.orders.order_service.OrderServiceSimulado.flush_periodico",
        dummy("flush"),
    )
    monkeypatch.setattr("core.trader_modular.Trader._heartbeat", dummy("hb"))
    monkeypatch.setattr("core.trader_modular.Trader._run_watchdog", dummy("wd"))

    cfg = Config(
        api_key="k",
        api_secret="s",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=10,
    )

    trader = Trader(cfg, order_service=OrderServiceSimulado())

    async def run():
        t = asyncio.create_task(trader.ejecutar())
        await asyncio.sleep(0.1)
        await trader.cerrar()
        await asyncio.wait_for(t, timeout=1)

    asyncio.run(run())

    assert not trader.tasks._tasks
    assert canceladas == {"feed", "estado", "contexto", "flush", "hb", "wd"}