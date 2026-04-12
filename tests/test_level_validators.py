"""Tests para validate_levels (SL/TP y geometría long/short)."""

import importlib.util
from pathlib import Path

import pytest

_lv_path = Path(__file__).resolve().parents[1] / "core" / "risk" / "level_validators.py"
_spec = importlib.util.spec_from_file_location("level_validators_under_test", _lv_path)
assert _spec and _spec.loader
_lv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_lv)
validate_levels = _lv.validate_levels
LevelValidationError = _lv.LevelValidationError


def test_short_swaps_when_levels_use_long_geometry() -> None:
    """Short con SL bajo entrada y TP alto (geometría de long) se corrige intercambiando."""
    entry = 69.89
    sl_wrong = 69.855055
    tp_wrong = 69.95989
    e, sl, tp = validate_levels(
        "short",
        entry,
        sl_wrong,
        tp_wrong,
        min_dist_pct=0.0005,
        tick_size=0.0,
        step_size=0.0,
    )
    assert e == entry
    assert tp < entry < sl
    assert sl == pytest.approx(tp_wrong)
    assert tp == pytest.approx(sl_wrong)


def test_long_swaps_when_levels_use_short_geometry() -> None:
    entry = 100.0
    sl_wrong = 101.0
    tp_wrong = 99.0
    e, sl, tp = validate_levels(
        "long",
        entry,
        sl_wrong,
        tp_wrong,
        min_dist_pct=0.0005,
        tick_size=0.0,
        step_size=0.0,
    )
    assert e == entry
    assert sl < entry < tp
    assert sl == pytest.approx(99.0)
    assert tp == pytest.approx(101.0)


def test_direction_mismatch_when_neither_geometry_fits() -> None:
    with pytest.raises(LevelValidationError) as exc:
        validate_levels(
            "short",
            100.0,
            99.0,
            98.0,
            min_dist_pct=0.0005,
            tick_size=0.0,
            step_size=0.0,
        )
    assert exc.value.reason == "direction_mismatch"
