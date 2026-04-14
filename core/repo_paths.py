"""Resolución de rutas relativas a la raíz del repositorio."""

from __future__ import annotations

import os
from pathlib import Path


def repo_root() -> Path:
    """Directorio raíz del proyecto (contiene ``core/``, ``config/``, etc.)."""

    return Path(__file__).resolve().parent.parent


def resolve_under_repo(path: str | Path) -> Path:
    """Resuelve ``path``; si es relativa, es respecto a :func:`repo_root`.

    Rutas absolutas se normalizan con :meth:`Path.resolve` sin modificar su prefijo
    (permite despliegues con datos fuera del clon).
    """

    p = Path(os.path.expandvars(os.path.expanduser(str(path))))
    if p.is_absolute():
        return p.resolve()
    return (repo_root() / p).resolve()
