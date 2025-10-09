"""Herramientas internas para realizar una revisión automática de código.

Este módulo proporciona utilidades asincrónicas que recorren el árbol de
directorios de un proyecto y evalúan distintos indicadores de calidad de
código. El objetivo es disponer de una verificación ligera que pueda
ejecutarse como parte del pipeline de CI del bot o en procesos de
observabilidad, generando hallazgos estructurados y fáciles de auditar.

Las heurísticas implementadas no pretenden sustituir a linters completos,
pero sí ofrecen una primera capa de control sobre aspectos que impactan en la
robustez del bot: documentación, tipado y complejidad ciclomática básica.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Iterable, Sequence

import ast


Severity = str


@dataclass(slots=True, frozen=True)
class CodeIssue:
    """Representa un hallazgo detectado durante la revisión automática."""

    file: Path
    line: int
    severity: Severity
    code: str
    message: str
    context: str | None = None

    def to_dict(self) -> dict[str, str | int]:
        """Serializa el hallazgo a un diccionario apto para JSON."""

        data: dict[str, str | int] = {
            "file": str(self.file),
            "line": self.line,
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
        }
        if self.context is not None:
            data["context"] = self.context
        return data


@dataclass(slots=True)
class CodeReviewConfig:
    """Configura los parámetros de la revisión automática de código."""

    max_function_length: int = 80
    max_arguments: int = 6
    include_tests: bool = False
    ignore_patterns: tuple[str, ...] = ("__pycache__", "build", "dist")
    require_type_hints: bool = True
    require_docstrings: bool = True

    def should_ignore(self, path: Path) -> bool:
        """Determina si un archivo debe excluirse del análisis."""

        normalized = path.as_posix()
        return any(pattern in normalized for pattern in self.ignore_patterns)


class CodeReviewEngine:
    """Ejecutor asincrónico de las reglas de revisión de código."""

    def __init__(self, config: CodeReviewConfig | None = None) -> None:
        self._config = config or CodeReviewConfig()

    async def analyze_path(self, root: Path) -> list[CodeIssue]:
        """Analiza recursivamente los archivos Python a partir de ``root``."""

        python_files = [
            path
            for path in root.rglob("*.py")
            if self._should_include_file(root, path)
        ]
        tasks = [asyncio.create_task(self._analyze_file(path)) for path in python_files]
        results = await asyncio.gather(*tasks)
        issues = [issue for result in results for issue in result]
        issues.sort(key=lambda issue: (str(issue.file), issue.line))
        return issues

    async def _analyze_file(self, path: Path) -> list[CodeIssue]:
        return await asyncio.to_thread(self._sync_analyze_file, path)

    def _sync_analyze_file(self, path: Path) -> list[CodeIssue]:
        try:
            source = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            return [
                CodeIssue(
                    file=path,
                    line=exc.lineno or 1,
                    severity="high",
                    code="SYNTAX_ERROR",
                    message=f"Error de sintaxis: {exc.msg}",
                )
            ]
        issues: list[CodeIssue] = []
        issues.extend(self._check_module(path, tree))
        issues.extend(self._check_functions(path, tree, source.splitlines()))
        return issues

    def _should_include_file(self, root: Path, path: Path) -> bool:
        if self._config.should_ignore(path):
            return False
        if not self._config.include_tests and "tests" in path.relative_to(root).parts:
            return False
        return True

    def _check_module(self, path: Path, tree: ast.Module) -> list[CodeIssue]:
        if not self._config.require_docstrings:
            return []
        if ast.get_docstring(tree):
            return []
        return [
            CodeIssue(
                file=path,
                line=1,
                severity="medium",
                code="MISSING_MODULE_DOCSTRING",
                message="El módulo no posee docstring inicial.",
            )
        ]

    def _check_functions(
        self, path: Path, tree: ast.Module, lines: Sequence[str]
    ) -> list[CodeIssue]:
        issues: list[CodeIssue] = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                issues.extend(self._check_single_function(path, node, lines))
        return issues

    def _check_single_function(
        self, path: Path, node: ast.FunctionDef | ast.AsyncFunctionDef, lines: Sequence[str]
    ) -> list[CodeIssue]:
        issues: list[CodeIssue] = []
        if self._config.require_docstrings and not ast.get_docstring(node):
            issues.append(
                CodeIssue(
                    file=path,
                    line=node.lineno,
                    severity="medium",
                    code="MISSING_DOCSTRING",
                    message=f"La función '{node.name}' no tiene docstring.",
                )
            )
        if self._config.require_type_hints:
            issues.extend(self._check_type_hints(path, node))
        issues.extend(self._check_function_length(path, node))
        issues.extend(self._check_argument_count(path, node))
        issues.extend(self._check_nested_blocks(path, node, lines))
        return issues

    def _check_type_hints(
        self, path: Path, node: ast.FunctionDef | ast.AsyncFunctionDef
    ) -> list[CodeIssue]:
        issues: list[CodeIssue] = []
        missing_annotations = [
            arg.arg
            for arg in (*node.args.args, *node.args.kwonlyargs)
            if arg.arg not in {"self", "cls"} and arg.annotation is None
        ]
        if missing_annotations:
            issues.append(
                CodeIssue(
                    file=path,
                    line=node.lineno,
                    severity="medium",
                    code="MISSING_TYPE_HINT",
                    message=(
                        "Argumentos sin anotación de tipo: "
                        + ", ".join(sorted(missing_annotations))
                    ),
                )
            )
        if node.returns is None:
            issues.append(
                CodeIssue(
                    file=path,
                    line=node.lineno,
                    severity="medium",
                    code="MISSING_RETURN_HINT",
                    message="La función no define anotación de retorno.",
                )
            )
        return issues

    def _check_function_length(
        self, path: Path, node: ast.FunctionDef | ast.AsyncFunctionDef
    ) -> list[CodeIssue]:
        end_lineno = getattr(node, "end_lineno", None)
        if end_lineno is None:
            return []
        length = end_lineno - node.lineno + 1
        if length <= self._config.max_function_length:
            return []
        return [
            CodeIssue(
                file=path,
                line=node.lineno,
                severity="low",
                code="FUNCTION_TOO_LONG",
                message=(
                    "La función excede el largo máximo permitido: "
                    f"{length} líneas (límite {self._config.max_function_length})."
                ),
            )
        ]

    def _check_argument_count(
        self, path: Path, node: ast.FunctionDef | ast.AsyncFunctionDef
    ) -> list[CodeIssue]:
        positional = [arg for arg in node.args.args if arg.arg not in {"self", "cls"}]
        arg_count = len(positional) + len(node.args.kwonlyargs)
        if node.args.vararg is not None:
            arg_count += 1
        if node.args.kwarg is not None:
            arg_count += 1
        if arg_count <= self._config.max_arguments:
            return []
        return [
            CodeIssue(
                file=path,
                line=node.lineno,
                severity="low",
                code="TOO_MANY_ARGUMENTS",
                message=(
                    "La función acepta demasiados argumentos "
                    f"({arg_count}, límite {self._config.max_arguments})."
                ),
            )
        ]

    def _check_nested_blocks(
        self,
        path: Path,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        lines: Sequence[str],
    ) -> list[CodeIssue]:
        depth = 0
        max_depth = 0
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.For, ast.While, ast.With, ast.AsyncWith)):
                depth = self._calculate_depth(child)
                max_depth = max(max_depth, depth)
        if max_depth <= 3:
            return []
        context = self._extract_context(lines, node.lineno)
        return [
            CodeIssue(
                file=path,
                line=node.lineno,
                severity="low",
                code="EXCESSIVE_NESTING",
                message=f"Anidamiento máximo detectado: {max_depth} niveles.",
                context=context,
            )
        ]

    def _calculate_depth(self, node: ast.AST, current_depth: int = 1) -> int:
        nested_blocks = [
            child
            for child in ast.iter_child_nodes(node)
            if isinstance(child, (ast.If, ast.For, ast.While, ast.With, ast.AsyncWith))
        ]
        if not nested_blocks:
            return current_depth
        return max(self._calculate_depth(child, current_depth + 1) for child in nested_blocks)

    def _extract_context(self, lines: Sequence[str], lineno: int, radius: int = 2) -> str:
        start = max(lineno - 1 - radius, 0)
        end = min(lineno - 1 + radius + 1, len(lines))
        snippet = [
            f"{idx}: {line}"
            for idx, line in zip(range(start + 1, end + 1), islice(lines, start, end))
        ]
        return "\n".join(snippet)

    @staticmethod
    def generate_report(issues: Iterable[CodeIssue]) -> dict[str, object]:
        """Crea un resumen agregando métricas por severidad."""

        total = 0
        by_severity: dict[str, int] = {}
        details = []
        for issue in issues:
            total += 1
            by_severity[issue.severity] = by_severity.get(issue.severity, 0) + 1
            details.append(issue.to_dict())
        return {
            "total": total,
            "por_severidad": by_severity,
            "issues": details,
        }


async def run_cli(path: str, json_output: bool = False) -> int:
    """Ejecuta la revisión automática desde la línea de comandos."""

    engine = CodeReviewEngine()
    issues = await engine.analyze_path(Path(path))
    report = engine.generate_report(issues)
    if json_output:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"Total de hallazgos: {report['total']}")
        for severity, count in sorted(
            report["por_severidad"].items(), key=lambda item: item[1], reverse=True
        ):
            print(f"- {severity}: {count}")
        for issue in issues:
            context = f" Contexto: {issue.context}" if issue.context else ""
            print(
                f"[{issue.severity.upper()}] {issue.file}:{issue.line} "
                f"{issue.code} - {issue.message}.{context}"
            )
    return 0 if not issues else 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Revisión automática de código del bot")
    parser.add_argument("path", nargs="?", default=".", help="Ruta raíz a analizar")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Imprime el resultado en formato JSON estructurado",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Punto de entrada síncrono para invocar desde scripts o CLI."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(run_cli(args.path, args.json))


__all__ = [
    "CodeReviewConfig",
    "CodeReviewEngine",
    "CodeIssue",
    "main",
    "run_cli",
]
