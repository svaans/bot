from core.reporting import ReporterDiario


def test_ultimas_operaciones_limit(tmp_path):
    rd = ReporterDiario(carpeta=str(tmp_path), max_operaciones=2)
    info = {
        'symbol': 'BTC',
        'retorno_total': 1,
        'precio_entrada': 100,
        'precio_cierre': 110,
    }
    for _ in range(4):
        rd.registrar_operacion(info)
    assert len(rd.ultimas_operaciones['BTC']) == 2