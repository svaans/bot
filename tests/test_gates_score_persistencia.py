from types import SimpleNamespace

import pandas as pd
import pytest

from core.strategies.entry import verificar_entradas as verificar_mod


@pytest.fixture(autouse=True)
def reset_entry_metrics() -> None:
    metrics = [
        verificar_mod.DECISION_TOTAL,
        verificar_mod.GATE_SCORE_DECISIONS_TOTAL,
        verificar_mod.GATE_PERSISTENCIA_DECISIONS_TOTAL,
        verificar_mod.GATE_SCORE_LAST_VALUE,
    ]
    for metric in metrics:
        metric._children.clear()
        if hasattr(metric, "_value"):
            metric._value = 0.0
    yield
    for metric in metrics:
        metric._children.clear()
        if hasattr(metric, "_value"):
            metric._value = 0.0


@pytest.fixture
def capture_entry_logger(caplog: pytest.LogCaptureFixture):
    logger = verificar_mod.log
    logger.addHandler(caplog.handler)
    try:
        yield
    finally:
        logger.removeHandler(caplog.handler)


def _build_df() -> pd.DataFrame:
    data = [
        {
            "timestamp": 1_700_000_000,
            "open": 10.0,
            "high": 10.5,
            "low": 9.5,
            "close": 10.2,
            "volume": 100.0,
        }
    ]
    df = pd.DataFrame(data)
    df.attrs["tf"] = "1m"
    return df


def _trader(config: SimpleNamespace, repo: object | None = None) -> SimpleNamespace:
    trader = SimpleNamespace(
        config=config,
        persistencia=None,
        repo=repo,
    )
    trader._puede_evaluar_entradas = lambda _symbol: True
    return trader


def _config(**overrides: object) -> SimpleNamespace:
    base = dict(
        usar_score_tecnico=True,
        umbral_score_tecnico=50.0,
        umbral_score_overrides={},
        usar_score_overrides={},
        persistencia_strict=False,
        persistencia_strict_overrides={},
        timeout_evaluar_condiciones=1,
        timeout_evaluar_condiciones_por_symbol={},
        min_dist_pct=0.0001,
        min_dist_pct_overrides={},
        contradicciones_bloquean_entrada=False,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


async def _run_verificar(
    monkeypatch: pytest.MonkeyPatch,
    trader: SimpleNamespace,
    engine_result: dict,
    *,
    df: pd.DataFrame | None = None,
) -> object:
    async def _fake_eval(*_args, **_kwargs):
        return engine_result

    monkeypatch.setattr(verificar_mod, "_evaluar_engine", _fake_eval)
    df_use = df if df is not None else _build_df()
    return await verificar_mod.verificar_entrada(trader, "BTCUSDT", df_use, estado={}, on_event=None)


@pytest.mark.asyncio
async def test_score_missing_blocks_and_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capture_entry_logger: None,
) -> None:
    config = _config()
    trader = _trader(config)
    caplog.set_level("DEBUG")
    caplog.set_level("DEBUG", logger="entry_verifier")

    result = await _run_verificar(
        monkeypatch,
        trader,
        {"side": "long", "score": None, "persistencia_ok": True},
    )

    assert result is None

    score_metric = verificar_mod.GATE_SCORE_DECISIONS_TOTAL.labels("BTCUSDT", "1m", "false")
    assert score_metric._value == 1
    assert (
        verificar_mod.GATE_SCORE_LAST_VALUE.labels(symbol="BTCUSDT", timeframe="1m")._value
        == -1.0
    )

    decision_metric = verificar_mod.DECISION_TOTAL.labels(
        symbol="BTCUSDT", timeframe="1m", outcome="rechazada"
    )
    assert decision_metric._value == 1

    warning = [rec for rec in caplog.records if rec.msg == "gate.score_missing"]
    assert warning, "debe registrar advertencia de score faltante"

    final_logs = [rec for rec in caplog.records if rec.msg == "decision.final"]
    assert final_logs, "debe emitir decision.final"
    final_record = final_logs[-1]
    assert getattr(final_record, "permitida") is False
    assert getattr(final_record, "score") == -1.0
    assert getattr(final_record, "umbral") == pytest.approx(50.0)


@pytest.mark.asyncio
async def test_score_below_threshold_blocks(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capture_entry_logger: None,
) -> None:
    config = _config()
    trader = _trader(config)
    caplog.set_level("DEBUG")
    caplog.set_level("DEBUG", logger="entry_verifier")

    result = await _run_verificar(
        monkeypatch,
        trader,
        {"side": "long", "score": 30.0, "persistencia_ok": True},
    )

    assert result is None
    score_metric = verificar_mod.GATE_SCORE_DECISIONS_TOTAL.labels("BTCUSDT", "1m", "false")
    assert score_metric._value == 1

    final_logs = [rec for rec in caplog.records if rec.msg == "decision.final"]
    assert final_logs, "debe registrar decision.final"
    final_record = final_logs[-1]
    assert getattr(final_record, "permitida") is False
    razones = getattr(final_record, "razones", [])
    assert "score_bajo" in razones


@pytest.mark.asyncio
async def test_score_gate_allows_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(usar_score_overrides={"BTCUSDT": False})
    trader = _trader(config)

    result = await _run_verificar(
        monkeypatch,
        trader,
        {"side": "long", "score": None, "persistencia_ok": True},
    )

    assert isinstance(result, dict)
    assert result["score"] == -1.0
    metric = verificar_mod.GATE_SCORE_DECISIONS_TOTAL.labels("BTCUSDT", "1m", "true")
    assert metric._value == 1


@pytest.mark.asyncio
async def test_persistencia_repo_failure_non_strict(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capture_entry_logger: None,
) -> None:
    config = _config()
    caplog.set_level("DEBUG")
    caplog.set_level("DEBUG", logger="entry_verifier")

    class FailingRepo:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def save_evaluacion(self, **payload: object) -> None:
            self.calls.append(payload)
            raise RuntimeError("db down")

    repo = FailingRepo()
    trader = _trader(config, repo=repo)

    result = await _run_verificar(
        monkeypatch,
        trader,
        {"side": "long", "score": 80.0, "persistencia_ok": True},
    )

    assert isinstance(result, dict)
    assert result["persistencia_ok"] is False

    persist_metric = verificar_mod.GATE_PERSISTENCIA_DECISIONS_TOTAL.labels("BTCUSDT", "1m", "true")
    assert persist_metric._value == 1

    degraded = [rec for rec in caplog.records if rec.msg == "persistencia.degraded"]
    assert degraded, "debe registrar degradación"


@pytest.mark.asyncio
async def test_persistencia_repo_failure_strict_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(persistencia_strict=True)

    class FailingRepo:
        def save_evaluacion(self, **_payload: object) -> None:
            raise RuntimeError("db down")

    trader = _trader(config, repo=FailingRepo())

    result = await _run_verificar(
        monkeypatch,
        trader,
        {"side": "long", "score": 80.0, "persistencia_ok": True},
    )

    assert result is None
    metric = verificar_mod.GATE_PERSISTENCIA_DECISIONS_TOTAL.labels("BTCUSDT", "1m", "false")
    assert metric._value == 1


@pytest.mark.asyncio
async def test_final_decision_log_contains_meta(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capture_entry_logger: None,
) -> None:
    config = _config()
    trader = _trader(config)
    caplog.set_level("DEBUG")
    caplog.set_level("DEBUG", logger="entry_verifier")

    result = await _run_verificar(
        monkeypatch,
        trader,
        {"side": "long", "score": 90.0, "persistencia_ok": True},
    )

    assert isinstance(result, dict)
    assert result["meta"]["usar_score"] is True
    assert result["meta"]["umbral_score"] == pytest.approx(50.0)

    final_logs = [rec for rec in caplog.records if rec.msg == "decision.final"]
    assert final_logs
    payload_record = final_logs[-1]
    assert getattr(payload_record, "permitida") is True
    assert getattr(payload_record, "meta").get("persistencia_ok") is True


@pytest.mark.asyncio
async def test_distancias_gate_tolerates_float_rounding(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(min_dist_pct=0.0005)
    trader = _trader(config)

    df = pd.DataFrame(
        [
            {
                "timestamp": 1_700_000_100,
                "open": 120.4,
                "high": 121.0,
                "low": 120.3,
                "close": 120.5,
                "volume": 50.0,
            }
        ]
    )
    df.attrs["tf"] = "1m"

    result = await _run_verificar(
        monkeypatch,
        trader,
        {"side": "long", "score": 80.0, "persistencia_ok": True},
        df=df,
    )

    assert isinstance(result, dict)
    assert result["score"] == 80.0
    min_pct = config.min_dist_pct
    expected_sl = df.iloc[-1]["close"] * (1 - min_pct)
    expected_tp = df.iloc[-1]["close"] * (1 + 2 * min_pct)
    assert result["stop_loss"] == pytest.approx(expected_sl)
    assert result["take_profit"] == pytest.approx(expected_tp)
