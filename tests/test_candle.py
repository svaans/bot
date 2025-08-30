import pytest
from core.streams.candle_filter import CandleFilter


def _candle(ts, final=True):
    return {
        'timestamp': ts,
        'open': 1,
        'high': 1,
        'low': 1,
        'close': 1,
        'volume': 1,
        'isFinal': final,
    }


def test_dedup_y_orden():
    filtro = CandleFilter()
    intervalo_ms = 60_000
    secuencia = [_candle((i + 1) * 60_000) for i in range(100)]
    for t in [102, 103, 104, 105, 106]:
        secuencia.append(_candle(t * 60_000))
    secuencia.append(_candle(106 * 60_000))  # duplicada
    secuencia.append(_candle(101 * 60_000))  # fuera de orden
    procesadas = []
    estados = []
    for vela in secuencia:
        ready, status, warn = filtro.push(vela, intervalo_ms)
        procesadas.extend([v['timestamp'] for v in ready])
        estados.append(status)
        assert not warn
    expected = [(i + 1) * 60_000 for i in range(100)] + [t * 60_000 for t in [102, 103, 104, 105, 106]]
    assert procesadas == expected
    assert 'duplicate' in estados
    assert 'out_of_order' in estados