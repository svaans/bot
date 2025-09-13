"""Persistencia simple de velas en SQLite/Postgres."""
from __future__ import annotations

from typing import Dict, List
import os

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    select,
    text,
)
from sqlalchemy.exc import IntegrityError

_metadata = MetaData()

_velas = Table(
    "velas",
    _metadata,
    Column("symbol", String, primary_key=True),
    Column("timestamp", Integer, primary_key=True),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", Float, nullable=False),
)


class PersistenciaVelas:
    """Gestiona la persistencia de velas por símbolo."""

    def __init__(self, url: str | None = None) -> None:
        self.url = url or os.getenv("VELAS_DB_URL", "sqlite:///velas.db")
        self.engine = create_engine(self.url)
        _metadata.create_all(self.engine)

    def guardar_candle(self, symbol: str, candle: Dict[str, float], limit: int) -> None:
        """Guarda una vela y mantiene solo ``limit`` velas recientes."""
        with self.engine.begin() as conn:
            stmt = _velas.insert().values(symbol=symbol, **candle)
            try:
                conn.execute(stmt)
            except IntegrityError:
                conn.execute(
                    _velas.update()
                    .where(
                        (_velas.c.symbol == symbol)
                        & (_velas.c.timestamp == candle["timestamp"])
                    )
                    .values(**candle)
                )
            conn.execute(
                text(
                    "DELETE FROM velas WHERE symbol = :symbol AND timestamp NOT IN ("
                    "SELECT timestamp FROM velas WHERE symbol = :symbol "
                    "ORDER BY timestamp DESC LIMIT :lim)"
                ),
                {"symbol": symbol, "lim": limit},
            )

    def cargar_ultimas(self, symbol: str, limit: int) -> List[Dict[str, float]]:
        """Recupera las últimas ``limit`` velas para ``symbol`` en orden ascendente."""
        with self.engine.begin() as conn:
            rows = conn.execute(
                select(_velas)
                .where(_velas.c.symbol == symbol)
                .order_by(_velas.c.timestamp.desc())
                .limit(limit)
            ).fetchall()
        return [
            {k: v for k, v in dict(row._mapping).items() if k != "symbol"}
            for row in reversed(rows)
        ]