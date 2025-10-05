from __future__ import annotations

import math

from core.utils.backoff import calcular_backoff


def test_calcular_backoff_respetar_limites(monkeypatch):
    monkeypatch.setattr("random.uniform", lambda a, b: (a + b) / 2)

    assert math.isclose(calcular_backoff(1, base=1.0, max_seg=10.0), 1.0)
    assert math.isclose(calcular_backoff(3, base=1.0, max_seg=10.0), 4.0)
    assert calcular_backoff(10, base=1.0, max_seg=5.0) <= 5.0