"""Restricción de rutas de escritura para artefactos bajo ``config/``."""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONFIG_ROOT = (_REPO_ROOT / "config").resolve()


def resolve_config_output_path(path: Path) -> Path:
    """Resuelve ``path`` y exige que el fichero quede bajo ``<repo>/config``.

    Evita que herramientas CLI (p. ej. calibración) sobrescriban rutas arbitrarias.
    """

    candidate = path if path.is_absolute() else (_REPO_ROOT / path)
    resolved = candidate.resolve()
    try:
        resolved.relative_to(_CONFIG_ROOT)
    except ValueError as exc:
        raise ValueError(
            f"La ruta de salida debe estar dentro de {_CONFIG_ROOT} (recibida: {path})"
        ) from exc
    return resolved


__all__ = ["_CONFIG_ROOT", "_REPO_ROOT", "resolve_config_output_path"]
