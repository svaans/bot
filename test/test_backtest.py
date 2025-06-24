import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))



import asyncio
import pandas as pd
from config.config_manager import Config
from backtesting.backtest import BacktestTrader


class DummyClient:
    def fetch_balance(self):
        return {"total": {"EUR": 1000}}


async def fake_fetch_balance_async(client):
    return {"total": {"EUR": 1000}}


def generar_velas(n=40, symbol="BTC/EUR"):
    ts = pd.date_range("2024-01-01", periods=n, freq="T")
    velas = []
    precio = 100
    for i, t in enumerate(ts):
        vela = {
            "symbol": symbol,
            "timestamp": int(t.timestamp() * 1000),
            "open": precio,
            "high": precio + 1,
            "low": precio - 1,
            "close": precio + 1,
            "volume": 10,
        }
        velas.append(vela)
        precio += 1
    return velas


def crear_trader(monkeypatch):
    monkeypatch.setattr("core.trader_modular.crear_cliente", lambda cfg: DummyClient())
    monkeypatch.setattr("binance_api.cliente.crear_cliente", lambda cfg=None: DummyClient())
    monkeypatch.setattr("core.trader_modular.cargar_pesos_estrategias", lambda: {"BTC/EUR": {"s1": 1}})
    monkeypatch.setattr("core.ordenes_reales.obtener_todas_las_ordenes", lambda: {})
    monkeypatch.setattr("core.ordenes_reales.sincronizar_ordenes_binance", lambda symbols: {})
    monkeypatch.setattr("core.trader_modular.calcular_fraccion_kelly", lambda: 1.0)
    monkeypatch.setattr("pandas.read_parquet", lambda *a, **k: pd.DataFrame({"close": [1, 2, 3, 4]}))
    monkeypatch.setattr("core.trader_modular.fetch_balance_async", fake_fetch_balance_async)

    cfg = Config(
        api_key="",
        api_secret="",
        modo_real=False,
        intervalo_velas="1m",
        symbols=["BTC/EUR"],
        umbral_riesgo_diario=0.1,
        min_order_eur=10,
        persistencia_minima=1,
        usar_score_tecnico=False,
    )
    return BacktestTrader(cfg)


def alimentar_velas(trader, n=40):
    velas = generar_velas(n)
    for vela in velas:
        asyncio.run(trader._procesar_vela(vela))


def test_abre_operacion(monkeypatch):
    trader = crear_trader(monkeypatch)
    opened = {}

    async def fake_eval(symbol, df, estado):
        if trader.risk.riesgo_superado(1000):
            return None
        return {
            "symbol": symbol,
            "precio": df["close"].iloc[-1],
            "sl": 90,
            "tp": 110,
            "estrategias": {"s1": True},
            "tendencia": "alcista",
            "direccion": "long",
            "puntaje": 5.0,
            "umbral": 3.0,
        }

    async def fake_open(*args, **info):
        opened["symbol"] = args[0]

    monkeypatch.setattr(trader, "evaluar_condiciones_de_entrada", fake_eval)
    monkeypatch.setattr(trader.orders, "abrir_async", fake_open)
    monkeypatch.setattr(trader.risk, "riesgo_superado", lambda capital: False)
    async def qty(*args, **kwargs):
        return 1.0
    monkeypatch.setattr(trader, "_calcular_cantidad_async", qty)

    alimentar_velas(trader)

    assert opened.get("symbol") == "BTC/EUR"


def test_riesgo_bloquea(monkeypatch):
    trader = crear_trader(monkeypatch)
    called = {}

    async def fake_eval(symbol, df, estado):
        if trader.risk.riesgo_superado(1000):
            return None
        return {
            "symbol": symbol,
            "precio": df["close"].iloc[-1],
            "sl": 90,
            "tp": 110,
            "estrategias": {"s1": True},
            "tendencia": "alcista",
            "direccion": "long",
            "puntaje": 5.0,
            "umbral": 3.0,
        }

    monkeypatch.setattr(trader, "evaluar_condiciones_de_entrada", fake_eval)

    async def fake_open(*args, **info):
        called["symbol"] = args[0]

    monkeypatch.setattr(trader.orders, "abrir_async", fake_open)
    monkeypatch.setattr(trader.risk, "riesgo_superado", lambda capital: True)
    async def qty(*args, **kwargs):
        return 1.0
    monkeypatch.setattr(trader, "_calcular_cantidad_async", qty)

    alimentar_velas(trader)

    assert not called


def test_capital_insuficiente(monkeypatch):
    trader = crear_trader(monkeypatch)
    called = {}

    async def fake_eval(symbol, df, estado):
        return {
            "symbol": symbol,
            "precio": df["close"].iloc[-1],
            "sl": 90,
            "tp": 110,
            "estrategias": {"s1": True},
            "tendencia": "alcista",
            "direccion": "long",
            "puntaje": 5.0,
            "umbral": 3.0,
        }

    monkeypatch.setattr(trader, "evaluar_condiciones_de_entrada", fake_eval)
    async def fake_open_async(*args, **info):
        called["symbol"] = args[0]
    monkeypatch.setattr(trader.orders, "abrir_async", fake_open_async)
    monkeypatch.setattr(trader.risk, "riesgo_superado", lambda capital: False)
    async def qty_zero(*args, **kwargs):
        return 0.0
    monkeypatch.setattr(trader, "_calcular_cantidad_async", qty_zero)

    alimentar_velas(trader)

    assert not called


def test_puntaje_insuficiente(monkeypatch):
    trader = crear_trader(monkeypatch)
    called = {}

    async def fake_eval(symbol, df, estado):
        return None

    monkeypatch.setattr(trader, "evaluar_condiciones_de_entrada", fake_eval)
    async def fake_open_async2(*args, **info):
        called["symbol"] = args[0]
    monkeypatch.setattr(trader.orders, "abrir_async", fake_open_async2)
    monkeypatch.setattr(trader.risk, "riesgo_superado", lambda capital: False)
    async def qty(*args, **kwargs):
        return 1.0
    monkeypatch.setattr(trader, "_calcular_cantidad_async", qty)

    alimentar_velas(trader)

    assert not called