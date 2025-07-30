import pandas as pd
import logging
from core.strategies.exit.salida_trailing_stop import salida_trailing_stop, log as trailing_log


def test_trailing_stop_error_logged(monkeypatch, caplog):
    def fake_get_atr(df):
        raise KeyError("sin atr")

    monkeypatch.setattr(
        'core.strategies.exit.salida_trailing_stop.get_atr', fake_get_atr
    )
    orden = {'precio_entrada': 1, 'direccion': 'long', 'symbol': 'TST'}
    df = pd.DataFrame({'close': [1, 2, 3]})
    trailing_log.propagate = True
    with caplog.at_level(logging.ERROR):
        res = salida_trailing_stop(orden, df)
    assert res['cerrar'] is False
    assert 'sin atr' in caplog.text
    assert 'TST' in caplog.text