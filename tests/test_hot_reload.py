"""Pruebas unitarias del manejador hot-reload con filtros de watchdog."""

from __future__ import annotations

from pathlib import Path

import pytest
from watchdog.events import FileModifiedEvent, FileMovedEvent

from core.hot_reload import _DebouncedReloader


class _DummyTimer:
    """Timer minimalista para tests que registra las llamadas a ``start``."""

    def __init__(self, interval, function, args=None, kwargs=None):  # type: ignore[no-untyped-def]
        self.interval = interval
        self.function = function
        self.args = args or ()
        self.kwargs = kwargs or {}
        self._alive = False
        self.daemon = False

    def start(self) -> None:  # pragma: no cover - trivial setter
        self._alive = True

    def cancel(self) -> None:  # pragma: no cover - trivial setter
        self._alive = False

    def is_alive(self) -> bool:  # pragma: no cover - trivial getter
        return self._alive


@pytest.fixture()
def patched_timer(monkeypatch):
    """Parchea ``threading.Timer`` para que use ``_DummyTimer`` y cuente arranques."""

    starts: list[float] = []

    def factory(interval, function, args=None, kwargs=None):  # type: ignore[no-untyped-def]
        timer = _DummyTimer(interval, function, args=args, kwargs=kwargs)

        original_start = timer.start

        def _start() -> None:
            starts.append(timer.interval)
            original_start()

        timer.start = _start  # type: ignore[assignment]
        return timer

    monkeypatch.setattr("core.hot_reload.threading.Timer", factory)
    return starts


def _build_handler(tmp_path: Path, **kwargs) -> _DebouncedReloader:
    params = dict(
        debounce_seconds=0.5,
        exclude=set(),
        verbose=False,
        watch_whitelist=[tmp_path],
        ignore_patterns=set(),
    )
    params.update(kwargs)
    return _DebouncedReloader(tmp_path, **params)


def test_reloader_triggers_only_for_real_python_files(tmp_path, patched_timer):
    handler = _build_handler(tmp_path)

    handler.dispatch(FileModifiedEvent(str(tmp_path / "module.py")))
    assert patched_timer == [0.5]

    handler.dispatch(FileModifiedEvent(str(tmp_path / "module.pyc")))
    assert patched_timer == [0.5]


def test_reloader_ignores_excluded_directories(tmp_path, patched_timer):
    handler = _build_handler(tmp_path, exclude={"__pycache__", "data"})

    handler.dispatch(FileModifiedEvent(str(tmp_path / "__pycache__" / "module.py")))
    handler.dispatch(FileModifiedEvent(str(tmp_path / "data" / "module.py")))
    assert patched_timer == []


def test_reloader_uses_destination_path_on_moves(tmp_path, patched_timer):
    handler = _build_handler(tmp_path)

    handler.dispatch(
        FileMovedEvent(
            str(tmp_path / "module.tmp"),
            str(tmp_path / "dest" / "module.py"),
        )
    )

    assert patched_timer == [0.5]
    assert handler._last_path is not None
    assert handler._last_path.name == "module.py"
