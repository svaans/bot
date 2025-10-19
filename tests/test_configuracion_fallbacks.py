import logging
from pathlib import Path

from config import configuracion


def test_backup_json_detecta_fallos_recurrentes(tmp_path, monkeypatch, caplog):
    origen = tmp_path / 'config.json'
    origen.write_text('{"valor": 1}', encoding='utf-8')

    fallback_dir = tmp_path / 'fallbacks'
    monkeypatch.setattr(configuracion, 'FALLBACK_DIR', fallback_dir)
    monkeypatch.setattr(configuracion, 'FALLBACK_ALERT_THRESHOLD', 2)
    monkeypatch.setattr(configuracion.log, 'propagate', True)

    real_datetime = configuracion.datetime

    class DummyDatetime:
        contador = 0

        @classmethod
        def now(cls):
            cls.contador += 1
            return real_datetime(2024, 1, 1, 0, 0, cls.contador)

    monkeypatch.setattr(configuracion, 'datetime', DummyDatetime)

    destino_normal = origen.parent

    def fake_copy(src, dst, *args, **kwargs):
        dst_path = Path(dst)
        if dst_path.parent == destino_normal:
            raise PermissionError('no access')
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        data = Path(src).read_bytes()
        dst_path.write_bytes(data)
        return str(dst_path)

    # ``shutil.copy`` debe fallar para el destino normal pero funcionar en fallback.
    monkeypatch.setattr(configuracion.shutil, 'copy', fake_copy)

    caplog.set_level(logging.WARNING, logger='config_service')

    with caplog.at_level(logging.WARNING, logger='config_service'):
        configuracion.backup_json(str(origen))
        configuracion.backup_json(str(origen))

    backups = list(fallback_dir.glob(f'{origen.name}.bak_*'))
    assert len(backups) == 2
    assert 'Backup redirigido a directorio temporal' in caplog.text
    assert 'Permisos denegados recurrentes al crear backup' in caplog.text