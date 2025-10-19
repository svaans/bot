import logging

import pytest

from config import config_manager


def test_cargar_float_emite_warning_con_valor_invalido(monkeypatch, caplog):
    monkeypatch.setenv('MIN_DIST_PCT', 'abc')
    monkeypatch.setattr(config_manager.log, 'propagate', True)
    caplog.set_level(logging.WARNING, logger='config_manager')

    with caplog.at_level(logging.WARNING, logger='config_manager'):
        valor = config_manager._cargar_float('MIN_DIST_PCT', 0.005)

    assert valor == pytest.approx(0.005)
    assert 'Valor inválido para MIN_DIST_PCT' in caplog.text


def test_parse_float_mapping_warning_en_override_invalido(monkeypatch, caplog):
    monkeypatch.setenv('MIN_DIST_PCT_OVERRIDES', 'BTCUSDT:abc,ETHUSDT:0.05')
    monkeypatch.setattr(config_manager.log, 'propagate', True)
    caplog.set_level(logging.WARNING, logger='config_manager')

    with caplog.at_level(logging.WARNING, logger='config_manager'):
        overrides = config_manager._parse_float_mapping('MIN_DIST_PCT_OVERRIDES')

    assert overrides == {'ETHUSDT': pytest.approx(0.05)}
    assert 'Valor inválido en MIN_DIST_PCT_OVERRIDES para BTCUSDT' in caplog.text