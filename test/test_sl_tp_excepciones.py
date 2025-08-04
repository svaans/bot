import pytest
from core.trader_modular import Trader


class DummyNotifier:
    def __init__(self):
        self.messages = []

    async def enviar_async(self, mensaje):
        self.messages.append(mensaje)


class DummyOrders:
    def __init__(self):
        self.called = False

    async def abrir_async(self, **kwargs):
        self.called = True


class DummyCapitalManager:
    def __init__(self):
        self.capital_por_simbolo = {'BTC/EUR': 1000}
        self.cliente = None


@pytest.mark.asyncio
async def test_abrir_operacion_real_error_en_validar_sl_tp_rechaza():
    trader = Trader.__new__(Trader)

    async def fake_calcular_cantidad(symbol, precio, stop_loss):
        return 100

    trader._calcular_cantidad_async = fake_calcular_cantidad
    trader.capital_manager = DummyCapitalManager()
    trader.piramide_fracciones = 1
    trader.orders = DummyOrders()
    trader.notificador = DummyNotifier()
    rechazos = []

    def fake_rechazo(symbol, motivo, puntaje=None, peso_total=None, estrategias=None):
        rechazos.append((symbol, motivo, estrategias))

    trader._rechazo = fake_rechazo

    def raise_validar_sl_tp(symbol, precio, sl, tp):
        raise Exception("fallo ATR")

    trader._validar_sl_tp = raise_validar_sl_tp

    estrategias = {'e1': 1.0}

    await trader._abrir_operacion_real(
        'BTC/EUR',
        100.0,
        95.0,
        105.0,
        estrategias,
        'alcista',
        'long'
    )

    assert not trader.orders.called
    assert rechazos and rechazos[0][1] == 'error_validacion_sl_tp'
    assert trader.notificador.messages