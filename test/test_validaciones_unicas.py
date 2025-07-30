import sys
from types import SimpleNamespace
sys.path.insert(0, '.')
import pandas as pd
from config.config_manager import Config
from indicators.atr import calcular_atr
from core.strategies.entry.validadores import validar_volumen


def validar_sl_tp_atr(df, precio, sl, tp, config):
    """Replica la lógica de Trader._validar_sl_tp para pruebas."""
    atr = calcular_atr(df.tail(100))
    if not atr:
        return True
    min_sl = atr * getattr(config, 'factor_sl_atr', 0.5)
    min_tp = atr * getattr(config, 'factor_tp_atr', 0.5)
    return not (abs(precio - sl) < min_sl or abs(tp - precio) < min_tp)


def test_validar_sl_tp_atr():
    cfg = Config(api_key='k', api_secret='s', modo_real=False,
                 intervalo_velas='1m', symbols=['AAA'],
                 umbral_riesgo_diario=0.1, min_order_eur=10)
    df = pd.DataFrame({
        'high': [110] * 50,
        'low': [90] * 50,
        'close': [100] * 50,
    })
    assert not validar_sl_tp_atr(df, 100, 99, 101, cfg)
    assert validar_sl_tp_atr(df, 100, 80, 120, cfg)


def test_validar_volumen():
    datos = {'volume': [100] * 30}
    df_ok = pd.DataFrame(datos)
    df_low = pd.DataFrame({'volume': [100] * 29 + [50]})
    assert validar_volumen(df_ok)
    assert not validar_volumen(df_low)