import asyncio
from pathlib import Path

import pytest

from core.diag import CodeIssue, CodeReviewConfig, CodeReviewEngine


@pytest.mark.asyncio
async def test_detects_missing_docstrings_and_type_hints(tmp_path: Path) -> None:
    module = tmp_path / "modulo.py"
    module.write_text(
        "def ejemplo(a, b):\n"
        "    if a > 0:\n"
        "        if b > 0:\n"
        "            if a > b:\n"
        "                if a > 10:\n"
        "                    return a + b\n"
        "    return a\n",
        encoding="utf-8",
    )

    engine = CodeReviewEngine(CodeReviewConfig(max_function_length=3, max_arguments=1))
    issues = await engine.analyze_path(tmp_path)

    codes = {issue.code for issue in issues}
    assert "MISSING_MODULE_DOCSTRING" in codes
    assert "MISSING_DOCSTRING" in codes
    assert "MISSING_TYPE_HINT" in codes
    assert "MISSING_RETURN_HINT" in codes
    assert "FUNCTION_TOO_LONG" in codes
    assert "TOO_MANY_ARGUMENTS" in codes
    assert "EXCESSIVE_NESTING" in codes


@pytest.mark.asyncio
async def test_generate_report_counts_by_severity(tmp_path: Path) -> None:
    archivo = tmp_path / "con_error.py"
    archivo.write_text("def f(x: int) -> int:\n    return x\n", encoding="utf-8")

    engine = CodeReviewEngine(CodeReviewConfig(require_docstrings=False))
    issues = await engine.analyze_path(tmp_path)
    issues.append(
        CodeIssue(
            file=archivo,
            line=1,
            severity="high",
            code="SYNTAX_ERROR",
            message="Dummy",
        )
    )

    report = engine.generate_report(issues)
    assert report["total"] == len(issues)
    assert report["por_severidad"]["high"] == 1
    assert isinstance(report["issues"], list)


def test_cli_entry_point_runs_event_loop(monkeypatch):
    async def fake_analyze_path(self, path: Path):
        return []

    monkeypatch.setattr(CodeReviewEngine, "analyze_path", fake_analyze_path)

    loop = asyncio.new_event_loop()

    def run(coro):
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(asyncio, "run", run)

    # Importación perezosa para evitar efectos secundarios durante el monkeypatching.
    from core.diag.code_review import main as cli_main

    assert cli_main(["."]) == 0


@pytest.mark.asyncio
async def test_ignore_patterns_do_not_match_substrings(tmp_path: Path) -> None:
    builders = tmp_path / "builders"
    builders.mkdir()
    archivo = builders / "module.py"
    archivo.write_text("def f(x):\n    return x\n", encoding="utf-8")

    engine = CodeReviewEngine(CodeReviewConfig(ignore_patterns=("build",)))
    issues = await engine.analyze_path(tmp_path)

    assert any(issue.file == archivo for issue in issues)
