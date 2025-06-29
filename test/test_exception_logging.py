import asyncio
import logging
import pandas as pd
import pytest
from _pytest.logging import LogCaptureHandler

from config.config_manager import Config
from config.development import DevelopmentConfig
from core.trader_modular import Trader
from core.orders.order_service import OrderServiceSimulado
from core.strategies.evaluador_tecnico import cargar_pesos_tecnicos, _pesos_cache
from core.strategies.entry.verificar_entradas import _verificar_entrada_impl


class DummyClient:
    def fetch_balance(self):
        return {"total": {"EUR": 100}}


def _patch_trader_base(monkeypatch):
    monkeypatch.setattr("binance_api.cliente.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.crear_cliente", lambda config=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.calcular_fraccion_kelly", lambda: 1.0)
    monkeypatch.setattr("core.trader_modular.cargar_pesos_estrategias", lambda: {})
    monkeypatch.setattr(
        "core.orders.order_service.OrderServiceReal.cargar_ordenes",
        lambda self, symbols=None: {},
    )

def test_trader_init_logs_exception(monkeypatch):
    _patch_trader_base(monkeypatch)
    def fail(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(
        "core.orders.order_service.OrderServiceSimulado.cargar_ordenes",
        fail,
    )
    cfg = Config(api_key="k", api_secret="s", modo_real=False, intervalo_velas="1m", symbols=["BTC/EUR"], umbral_riesgo_diario=0.1, min_order_eur=DevelopmentConfig().min_order_eur)
    logger = logging.getLogger("trader")
    handler = LogCaptureHandler()
    logger.addHandler(handler)
    with pytest.raises(RuntimeError):
        Trader(cfg, order_service=OrderServiceSimulado())
    logger.removeHandler(handler)
    assert any("Error cargando" in r.getMessage() for r in handler.records)


def test_cargar_pesos_logs(monkeypatch):
    monkeypatch.setattr("os.path.exists", lambda p: True)
    monkeypatch.setattr("builtins.open", lambda *a, **k: (_ for _ in ()).throw(IOError("fail")))
    _pesos_cache.clear() if isinstance(_pesos_cache, dict) else None
    logger = logging.getLogger("eval_tecnico")
    handler = LogCaptureHandler()
    logger.addHandler(handler)
    cargar_pesos_tecnicos("AAA")
    logger.removeHandler(handler)
    assert any("Usando pesos por defecto" in r.getMessage() for r in handler.records)


class DummyPersistencia:
    peso_extra = 0.0
    def actualizar(self, *a, **k):
        pass
    def es_persistente(self, *a, **k):
        return True
    def ajustar_minimo(self, *a, **k):
        pass

class DummyEngine:
    def evaluar_entrada(self, *a, **k):
        return {"estrategias_activas": {"e": True}}

class DummyTrader:
    def __init__(self):
        self.config_por_simbolo = {"AAA": {}}
        self.estado_tendencia = {}
        self.engine = DummyEngine()
        self.persistencia = DummyPersistencia()
        self.pesos_por_simbolo = {"AAA": {}}
        self.historial_cierres = {}
        self.state_lock = asyncio.Lock()
        self.usar_score_tecnico = False
    async def _calcular_cantidad_async(self, *a, **k):
        raise ValueError("fail")
    def _validar_puntaje(self, *a, **k):
        return True
    async def _validar_diversidad(self, *a, **k):
        return True
    def _validar_estrategia(self, *a, **k):
        return True
    def _evaluar_persistencia(self, *a, **k):
        return True, 1.0, 0.5
    def _validar_reentrada_tendencia(self, *a, **k):
        return True


def test_verificar_entrada_log_exception(monkeypatch):
    monkeypatch.setattr("core.strategies.entry.verificar_entradas.adaptar_configuracion", lambda *a, **k: {})
    monkeypatch.setattr("core.strategies.entry.verificar_entradas.adaptar_configuracion_base", lambda *a, **k: a[2])
    monkeypatch.setattr("core.strategies.entry.verificar_entradas.detectar_tendencia", lambda *a, **k: ("alcista", None))
    monkeypatch.setattr("core.strategies.entry.verificar_entradas.coincidencia_parcial", lambda *a, **k: 1.0)
    monkeypatch.setattr("core.strategies.entry.verificar_entradas.calcular_umbral_adaptativo", lambda *a, **k: 0.0)
    monkeypatch.setattr("core.strategies.entry.verificar_entradas.filtrar_por_direccion", lambda e, d: (e, []))
    monkeypatch.setattr(
        "core.strategies.entry.verificar_entradas.obtener_parametros_persistencia",
        lambda *a, **k: (0.0, 1),
    )
    trader = DummyTrader()
    estado = type("E", (), {"buffer": [{"close": 1, "open":1, "high":1, "low":1, "volume":1}], "ultimo_umbral":0})()
    df = pd.DataFrame({"close": [1]*30, "open": [1]*30, "high": [1]*30, "low": [1]*30, "volume": [1]*30})

    logger = logging.getLogger("verificar_entrada")
    handler = LogCaptureHandler()
    logger.addHandler(handler)
    with pytest.raises(ValueError):
        asyncio.run(_verificar_entrada_impl(trader, "AAA", df, estado))
    logger.removeHandler(handler)
    assert any("Error calculando cantidad" in r.getMessage() for r in handler.records)