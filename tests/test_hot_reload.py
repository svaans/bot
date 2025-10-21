"""Pruebas unitarias del manejador hot-reload con filtros de watchdog."""

from __future__ import annotations

import importlib
import json
import logging
import sys
import textwrap
import time
from pathlib import Path

import pytest
from watchdog.events import FileModifiedEvent, FileMovedEvent

from core.hot_reload import (
    ModularReloadController,
    ModularReloadRule,
    _DebouncedReloader,
    _NOISY_WATCHDOG_LOGGERS,
    _configure_watchdog_logging,
    _log_hot_reload_event,
)
from observability.metrics import (
    HOT_RELOAD_DEBOUNCE_SECONDS,
    HOT_RELOAD_ERRORS_TOTAL,
    HOT_RELOAD_RESTARTS_TOTAL,
)


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


def _reset_metric(metric) -> None:
    if hasattr(metric, "_value"):
        metric._value = 0.0
    children = getattr(metric, "_children", None)
    if not children:
        return
    for child in children.values():
        if hasattr(child, "_value"):
            child._value = 0.0
    children.clear()


def test_reloader_triggers_only_for_real_python_files(tmp_path, patched_timer):
    handler = _build_handler(tmp_path)

    handler.dispatch(FileModifiedEvent(str(tmp_path / "module.py")))
    assert patched_timer == [0.5]

    handler.dispatch(FileModifiedEvent(str(tmp_path / "module.pyc")))
    assert patched_timer == [0.5]

    handler.dispatch(FileModifiedEvent(str(tmp_path / "config.yaml")))
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


def test_reloader_accepts_extra_extensions(tmp_path, patched_timer):
    handler = _build_handler(tmp_path)
    handler.dispatch(FileModifiedEvent(str(tmp_path / "settings.yaml")))
    assert patched_timer == []

    handler = _build_handler(tmp_path, extra_extensions=[".yaml"])
    handler.dispatch(FileModifiedEvent(str(tmp_path / "settings.yaml")))
    assert patched_timer == [0.5]


def test_reloader_persists_state_before_restart(monkeypatch, tmp_path):
    handler = _build_handler(tmp_path)

    calls: dict[str, str | None] = {}

    def fake_persist(*, reason=None):
        calls["reason"] = reason

    monkeypatch.setattr("core.hot_reload.persist_critical_state", fake_persist)
    monkeypatch.setattr("core.hot_reload.os.execv", lambda *_, **__: (_ for _ in ()).throw(SystemExit(0)))

    handler._last_event_ts = time.time() - 5
    handler._last_path = tmp_path / "module.py"

    with pytest.raises(SystemExit):
        handler._maybe_restart()

    assert calls["reason"] == "hot_reload"


def test_hot_reload_metrics_recorded(monkeypatch, tmp_path, patched_timer):
    handler = _build_handler(tmp_path)

    for metric in (
        HOT_RELOAD_DEBOUNCE_SECONDS,
        HOT_RELOAD_RESTARTS_TOTAL,
        HOT_RELOAD_ERRORS_TOTAL,
    ):
        _reset_metric(metric)

    source = tmp_path / "module.py"
    source.write_text("print('test')\n", encoding="utf-8")

    handler.dispatch(FileModifiedEvent(str(source)))

    scheduled = HOT_RELOAD_DEBOUNCE_SECONDS.labels(kind="scheduled")._value
    assert scheduled == pytest.approx(handler.debounce)

    monkeypatch.setattr("core.hot_reload.persist_critical_state", lambda **_: None)

    monkeypatch.setattr(
        "core.hot_reload.os.execv",
        lambda *_, **__: (_ for _ in ()).throw(SystemExit(0)),
    )

    handler._last_event_ts = time.time() - handler.debounce
    handler._last_path = source

    with pytest.raises(SystemExit):
        handler._maybe_restart()

    elapsed = HOT_RELOAD_DEBOUNCE_SECONDS.labels(kind="elapsed")._value
    assert elapsed == pytest.approx(handler.debounce, rel=1e-2, abs=1e-2)

    restarts = HOT_RELOAD_RESTARTS_TOTAL.labels(reason="file_change")._value
    assert restarts == pytest.approx(1.0)

    assert HOT_RELOAD_ERRORS_TOTAL._children == {}


def test_restart_cooldown_defers_consecutive_restarts(monkeypatch, tmp_path, patched_timer):
    handler = _build_handler(tmp_path, restart_cooldown_seconds=2.0)

    for metric in (
        HOT_RELOAD_DEBOUNCE_SECONDS,
        HOT_RELOAD_RESTARTS_TOTAL,
    ):
        _reset_metric(metric)

    monkeypatch.setattr("core.hot_reload.persist_critical_state", lambda **_: None)
    monkeypatch.setattr(
        "core.hot_reload.os.execv",
        lambda *_, **__: (_ for _ in ()).throw(SystemExit(0)),
    )

    source = tmp_path / "module.py"
    source.write_text("print('test')\n", encoding="utf-8")

    handler.dispatch(FileModifiedEvent(str(source)))
    handler._last_event_ts = time.time() - handler.debounce
    handler._last_path = source

    with pytest.raises(SystemExit):
        handler._maybe_restart()

    assert HOT_RELOAD_RESTARTS_TOTAL.labels(reason="file_change")._value == pytest.approx(1.0)

    handler.dispatch(FileModifiedEvent(str(source)))
    handler._last_event_ts = time.time() - handler.debounce
    handler._last_path = source

    previous_timer_count = len(patched_timer)
    handler._maybe_restart()

    assert len(patched_timer) == previous_timer_count + 1
    cooldown_interval = patched_timer[-1]
    assert cooldown_interval == pytest.approx(2.0, rel=0.3)
    assert HOT_RELOAD_RESTARTS_TOTAL.labels(reason="file_change")._value == pytest.approx(1.0)
    assert HOT_RELOAD_DEBOUNCE_SECONDS.labels(kind="cooldown_wait")._value >= 0.0


def test_modular_reload_controller_reloads_pure_package(monkeypatch, tmp_path):
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("\n", encoding="utf-8")
    module_path = pkg_dir / "algo.py"
    module_path.write_text(
        textwrap.dedent(
            """
            import math

            VALUE = math.sqrt(4)
            """
        ),
        encoding="utf-8",
    )

    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()
    module = importlib.import_module("pkg.algo")

    reload_calls: list[str] = []
    restart_calls: list[str] = []

    def fake_reload(target):
        reload_calls.append(target.__name__)
        return target

    class ImmediateThread:
        def __init__(self, target=None, args=None, kwargs=None, **_):
            self._target = target
            self._args = args or ()
            self._kwargs = kwargs or {}

        def start(self) -> None:
            if self._target:
                self._target(*self._args, **self._kwargs)

    monkeypatch.setattr("core.hot_reload.importlib.reload", fake_reload)
    monkeypatch.setattr("core.hot_reload.threading.Thread", ImmediateThread)

    controller = ModularReloadController(
        root=tmp_path,
        rules=[ModularReloadRule(module="pkg")],
    )

    handled = controller.handle_change(
        module_path,
        restart_callback=lambda: restart_calls.append("restart"),
    )

    assert handled is True
    assert reload_calls == ["pkg", module.__name__]
    assert restart_calls == []

    sys.path.remove(str(tmp_path))
    for key in list(sys.modules):
        if key == "pkg" or key.startswith("pkg."):
            sys.modules.pop(key)


def test_modular_reload_controller_detects_cross_dependency(monkeypatch, tmp_path):
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("\n", encoding="utf-8")
    module_path = pkg_dir / "algo.py"
    module_path.write_text(
        textwrap.dedent(
            """
            from otherpkg.helper import helper

            VALUE = helper()
            """
        ),
        encoding="utf-8",
    )

    other_dir = tmp_path / "otherpkg"
    other_dir.mkdir()
    (other_dir / "__init__.py").write_text("\n", encoding="utf-8")
    (other_dir / "helper.py").write_text("def helper():\n    return 1\n", encoding="utf-8")

    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()
    importlib.import_module("pkg.algo")

    controller = ModularReloadController(
        root=tmp_path,
        rules=[ModularReloadRule(module="pkg")],
    )

    handled = controller.handle_change(
        module_path,
        restart_callback=lambda: None,
    )

    assert handled is False

    sys.path.remove(str(tmp_path))
    for key in list(sys.modules):
        if key in ("pkg", "otherpkg") or key.startswith("pkg.") or key.startswith("otherpkg."):
            sys.modules.pop(key)


def test_hot_reload_log_serialization_avoids_scientific_notation(caplog):
    caplog.set_level(logging.INFO, logger="hot_reload.test")
    logger = logging.getLogger("hot_reload.test")

    _log_hot_reload_event(
        logger,
        "hot_reload_scan_scheduled",
        duration_seconds=0.000064,
        filters_applied=True,
    )

    record = caplog.records[-1]
    assert "e-" not in record.message

    payload = json.loads(record.message)
    assert payload["duration_seconds"] == pytest.approx(0.000064)
    assert payload["filters_applied"] is True


def test_configure_watchdog_logging_sets_warning_when_unset():
    logger_name = _NOISY_WATCHDOG_LOGGERS[0]
    noisy_logger = logging.getLogger(logger_name)
    original_level = noisy_logger.level
    root_logger = logging.getLogger()
    original_root_level = root_logger.level

    try:
        noisy_logger.setLevel(logging.NOTSET)
        root_logger.setLevel(logging.DEBUG)

        _configure_watchdog_logging(min_level=logging.WARNING)

        assert noisy_logger.level == logging.WARNING
    finally:
        noisy_logger.setLevel(original_level)
        root_logger.setLevel(original_root_level)


def test_configure_watchdog_logging_respects_manual_level():
    logger_name = _NOISY_WATCHDOG_LOGGERS[0]
    noisy_logger = logging.getLogger(logger_name)
    original_level = noisy_logger.level

    try:
        noisy_logger.setLevel(logging.ERROR)

        _configure_watchdog_logging(min_level=logging.WARNING)

        assert noisy_logger.level == logging.ERROR
    finally:
        noisy_logger.setLevel(original_level)
