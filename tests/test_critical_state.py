from importlib import reload

import pytest

import core.state.persistence as persistence


@pytest.fixture(autouse=True)
def reset_state(monkeypatch, tmp_path):
    path = tmp_path / "critical_state.json"
    monkeypatch.setenv("CRITICAL_STATE_PATH", str(path))
    reload(persistence)
    yield
    reload(persistence)


def test_persist_and_restore_roundtrip():
    captured: dict[str, dict] = {}

    def dump() -> dict[str, int]:
        return {"valor": 1}

    def load(data):
        captured.update(data)

    persistence.register_state("modulo", dump=dump, load=load)
    result = persistence.persist_critical_state(reason="test")
    assert result is not None
    state_path = persistence._state_file()
    assert state_path.exists()

    reload(persistence)
    persistence.register_state("modulo", dump=dump, load=load)
    persistence.restore_critical_state()
    assert captured == {"valor": 1}


def test_register_requires_name():
    with pytest.raises(ValueError):
        persistence.register_state("", dump=lambda: {})