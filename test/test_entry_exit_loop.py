import asyncio
import pandas as pd
import pytest
from types import SimpleNamespace

from core.strategies.entry.verificar_entradas import verificar_entrada
from core.strategies.exit.verificar_salidas import verificar_salidas
from core.orders.order_manager import OrderManager
from core.orders.order_model import Order


@pytest.mark.asyncio
async def test_verificar_entrada_datos_invalidos(monkeypatch):
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.verificar_integridad_datos',
        lambda df: False,
    )
    trader = SimpleNamespace(
        config_por_simbolo={},
        estado_tendencia={},
        pesos_por_simbolo={},
        persistencia=SimpleNamespace(peso_extra=0, actualizar=lambda *a, **k: None, es_persistente=lambda *a, **k: True),
        historial_cierres={},
        config=SimpleNamespace(max_perdidas_diarias=6),
        engine=SimpleNamespace(),
    )
    estado = SimpleNamespace(buffer=[{}] * 120)
    df = pd.DataFrame({'close': [1], 'timestamp': [1]})
    res = await verificar_entrada(trader, 'BTC/EUR', df, estado)
    assert res is None


@pytest.mark.asyncio
async def test_verificar_salidas_manejo_errores(monkeypatch):
    order = Order(
        symbol='BTC/EUR',
        precio_entrada=100,
        cantidad=1,
        stop_loss=90,
        take_profit=110,
        timestamp='t',
        estrategias_activas={'e1': True},
        tendencia='alcista',
        max_price=100,
    )
    ordenes = {'BTC/EUR': order}
    trader = SimpleNamespace(
        orders=SimpleNamespace(obtener=lambda s: ordenes.get(s)),
        config_por_simbolo={},
        estado_tendencia={},
    )
    monkeypatch.setattr(
        'core.strategies.exit.verificar_salidas.load_exit_config', lambda s: {}
    )
    trader._piramidar = lambda *a, **k: None
    async def falso(*args, **kwargs):
        return False

    monkeypatch.setattr(
        'core.strategies.exit.verificar_salidas._chequear_contexto_macro',
        falso,
    )
    monkeypatch.setattr(
        'core.strategies.exit.verificar_salidas._manejar_stop_loss',
        falso,
    )
    monkeypatch.setattr(
        'core.strategies.exit.verificar_salidas._procesar_take_profit',
        falso,
    )
    monkeypatch.setattr(
        'core.strategies.exit.verificar_salidas._manejar_trailing_stop',
        falso,
    )
    monkeypatch.setattr(
        'core.strategies.exit.verificar_salidas._manejar_cambio_tendencia',
        falso,
    )
    def raise_key(*a, **k):
        raise KeyError('x')
    monkeypatch.setattr(
        'core.strategies.exit.verificar_salidas.evaluar_salidas', raise_key
    )
    df = pd.DataFrame({'close': [101], 'high': [101], 'low': [99]})
    await verificar_salidas(trader, 'BTC/EUR', df)
    assert order.duracion_en_velas == 1


@pytest.mark.asyncio
async def test_ciclo_completo_sin_bloqueos(monkeypatch):
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.verificar_integridad_datos',
        lambda df: True,
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.adaptar_configuracion',
        lambda *a, **k: {},
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.detectar_tendencia',
        lambda *a, **k: ('alcista', None),
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.coincidencia_parcial',
        lambda *a, **k: 1.0,
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.calcular_umbral_adaptativo',
        lambda *a, **k: 1.0,
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.filtrar_por_direccion',
        lambda estr, dirc: (estr, []),
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.distancia_minima_valida',
        lambda *a, **k: True,
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.calcular_tp_sl_adaptativos',
        lambda *a, **k: (90, 110),
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.calcular_rsi', lambda df: 50
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.calcular_momentum',
        lambda df: 0.1,
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.calcular_slope',
        lambda df: 0.1,
    )
    monkeypatch.setattr(
        'core.strategies.entry.verificar_entradas.calcular_atr', lambda df: 1.0
    )
    monkeypatch.setattr(
        "core.strategies.entry.verificar_entradas.obtener_puntaje_contexto",
        lambda *a, **k: 0.0,
    )
    trader = SimpleNamespace(
        config_por_simbolo={},
        estado_tendencia={},
        config=SimpleNamespace(max_perdidas_diarias=6),
        pesos_por_simbolo={'e1': 1.0},
        persistencia=SimpleNamespace(
            peso_extra=0.0,
            actualizar=lambda *a, **k: None,
            es_persistente=lambda *a, **k: True,
        ),
        historial_cierres={},
        capital_por_simbolo={'BTC/EUR': 100},
        usar_score_tecnico=False,
        orders=OrderManager(False),
        engine=SimpleNamespace(
            evaluar_entrada=lambda *a, **k: {
                'estrategias_activas': {'e1': True},
                'puntaje_total': 1.0,
            }
        ),
        _evaluar_persistencia=lambda *a, **k: (True, 0, 0),
        _validar_puntaje=lambda *a, **k: True,
        _validar_diversidad=lambda *a, **k: True,
        _validar_estrategia=lambda *a, **k: True,
        _calcular_correlaciones=lambda *a, **k: pd.DataFrame(),
    )
    estado = SimpleNamespace(buffer=[{'t': i} for i in range(120)])
    df = pd.DataFrame(
        {
            'close': [100, 101, 102, 103, 104],
            'high': [100, 101, 102, 103, 104],
            'low': [99, 100, 101, 102, 103],
            'timestamp': range(5),
        }
    )

    async def fake_cerrar(order, precio, motivo, **k):
        order.cantidad_abierta = 0
        return True

    trader._cerrar_y_reportar = fake_cerrar
    trader._piramidar = lambda *a, **k: None

    # Ejecutar ciclo simple
    for i in range(5):
        sub = df.iloc[: i + 1].copy()
        await asyncio.wait_for(
            verificar_salidas(trader, 'BTC/EUR', sub),
            timeout=1,
        )
        if not trader.orders.obtener('BTC/EUR'):
            res = await asyncio.wait_for(
                verificar_entrada(trader, 'BTC/EUR', sub, estado),
                timeout=1,
            )
            if res:
                await trader.orders.abrir_async(
                    res['symbol'],
                    res['precio'],
                    res['sl'],
                    res['tp'],
                    res['estrategias'],
                    res['tendencia'],
                )
    assert 'BTC/EUR' in trader.orders.historial or trader.orders.obtener('BTC/EUR')