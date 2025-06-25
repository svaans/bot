import builtins
from core.config import gestor_config as gc


def test_cargar_config_optima_cache(monkeypatch, tmp_path):
    tmp_file = tmp_path / "config.json"
    tmp_file.write_text("{}")

    monkeypatch.setattr(gc, "_RUTA_CONFIG", tmp_file)
    gc._CACHE = None

    calls = {"count": 0}
    original_open = builtins.open

    def fake_open(*args, **kwargs):
        calls["count"] += 1
        return original_open(*args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)

    try:
        gc.cargar_config_optima()
        gc.cargar_config_optima()
    finally:
        monkeypatch.setattr(builtins, "open", original_open)

    assert calls["count"] == 1