"""Repositorio ligero para persistir el estado del capital asignado."""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from core.utils.logger import configurar_logger

log = configurar_logger("capital_repository", modo_silencioso=True)


@dataclass(slots=True)
class CapitalSnapshot:
    """Representa el estado persistido del capital disponible."""

    capital_por_simbolo: dict[str, float]
    disponible_global: float


class CapitalRepository:
    """Persistencia en disco usando un archivo JSON con escritura atómica."""

    def __init__(self, path: str | Path | None = None) -> None:
        base_path = Path(path) if path is not None else Path("estado/capital.json")
        self.path = base_path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------
    def load(self) -> CapitalSnapshot:
        """Carga el estado desde disco devolviendo ``CapitalSnapshot``."""

        with self._lock:
            try:
                raw = self.path.read_text(encoding="utf-8")
            except FileNotFoundError:
                return CapitalSnapshot(capital_por_simbolo={}, disponible_global=0.0)
            except Exception:
                log.warning(
                    "capital_repository.read_failed",
                    extra={"path": str(self.path)},
                    exc_info=True,
                )
                return CapitalSnapshot(capital_por_simbolo={}, disponible_global=0.0)

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning(
                "capital_repository.decode_failed",
                extra={"path": str(self.path)},
            )
            return CapitalSnapshot(capital_por_simbolo={}, disponible_global=0.0)

        capital_map = {}
        if isinstance(data, Mapping):
            raw_map = data.get("capital_por_simbolo", {})
            if isinstance(raw_map, Mapping):
                for key, value in raw_map.items():
                    if not isinstance(key, str):
                        continue
                    try:
                        capital_map[key.upper()] = max(0.0, float(value))
                    except (TypeError, ValueError):
                        continue
            disponible = data.get("disponible_global", 0.0)
            try:
                disponible_global = max(0.0, float(disponible))
            except (TypeError, ValueError):
                disponible_global = 0.0
        else:
            disponible_global = 0.0

        return CapitalSnapshot(capital_por_simbolo=capital_map, disponible_global=disponible_global)

    def save(self, capital_por_simbolo: Mapping[str, float], disponible_global: float) -> None:
        """Guarda ``capital_por_simbolo`` y ``disponible_global`` de forma atómica."""

        payload = {
            "capital_por_simbolo": {
                str(symbol): max(0.0, float(valor))
                for symbol, valor in capital_por_simbolo.items()
            },
            "disponible_global": max(0.0, float(disponible_global)),
        }
        texto = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")

        with self._lock:
            try:
                tmp_path.write_text(texto, encoding="utf-8")
                tmp_path.replace(self.path)
            except Exception:
                log.warning(
                    "capital_repository.write_failed",
                    extra={"path": str(self.path)},
                    exc_info=True,
                )
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
