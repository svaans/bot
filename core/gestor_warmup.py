"""
Gestor de Warmup — precarga de históricos mínima y segura.

Objetivo
--------
- Rellenar un buffer inicial de velas cerradas por símbolo antes de arrancar el pipeline.
- Validar alineación al intervalo y contigüidad (sin gaps).
- Ser tolerante: si no hay cliente real, intenta backfill vía `fetch_ohlcv_async` sólo si se provee.

API
---
async warmup_symbols(symbols, intervalo: str, *, cliente=None, min_bars=30,
                     base_ms: int | None = None,
                     on_event: callable | None = None) -> dict[str, list[dict]]
    Retorna un dict mapping symbol -> lista de velas (dict) listas para encolar.

Dependencias opcionales (ajusta import según tu repo)
----------------------------------------------------
- binance_api.cliente.fetch_ohlcv_async
- core.data.bootstrap.warmup_symbol (si existe)
- core.utils.utils.intervalo_a_segundos, timestamp_alineado
"""
from __future__ import annotations
import asyncio
from typing import Any, Callable, Dict, Iterable, List, Optional

try:
    from binance_api.cliente import fetch_ohlcv_async  # pragma: no cover
except Exception:  # pragma: no cover
    fetch_ohlcv_async = None  # será opcional

try:
    from core.data.bootstrap import warmup_symbol  # pragma: no cover
except Exception:  # pragma: no cover
    warmup_symbol = None

try:
    from core.utils.utils import intervalo_a_segundos, timestamp_alineado  # pragma: no cover
except Exception:  # pragma: no cover
    def intervalo_a_segundos(x: str) -> int:
        m = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}
        return m.get(x, 60)
    def timestamp_alineado(ts: int, intervalo: str) -> bool:
        base = intervalo_a_segundos(intervalo) * 1000
        return ts % base == 0


def _emit(on_event: Optional[Callable[[str, dict], None]], evt: str, data: dict) -> None:
    if on_event:
        try:
            on_event(evt, data)
        except Exception:
            pass


async def _backfill_fetch(cliente, symbol: str, intervalo: str, *, min_bars: int, base_ms: int) -> List[dict]:
    if fetch_ohlcv_async is None:
        return []
    if cliente is None:
        return []
    # Petición única para minimizar latencia; si quieres más, itera por chunks de 100
    limit = max(1, min(500, min_bars))
    ohlcv = await fetch_ohlcv_async(cliente, symbol, intervalo, since=None, limit=limit)
    velas: List[dict] = []
    for o in ohlcv or []:
        ts = int(o[0])
        if ts % base_ms != 0:
            continue
        velas.append(
            {
                "symbol": symbol,
                "timestamp": ts,
                "open": float(o[1]),
                "high": float(o[2]),
                "low": float(o[3]),
                "close": float(o[4]),
                "volume": float(o[5]),
                "is_closed": True,
            }
        )
    return velas[-min_bars:]


def _validar_contiguo(velas: List[dict], base_ms: int) -> bool:
    if len(velas) < 2:
        return True
    ts = [int(v["timestamp"]) for v in velas]
    for a, b in zip(ts, ts[1:]):
        if b - a != base_ms:
            return False
    return True


async def warmup_symbols(
    symbols: Iterable[str],
    intervalo: str,
    *,
    cliente: Any = None,
    min_bars: int = 30,
    base_ms: Optional[int] = None,
    on_event: Optional[Callable[[str, dict], None]] = None,
) -> Dict[str, List[dict]]:
    """Precarga histórica simple para un conjunto de símbolos.

    Política: best-effort. Intenta `warmup_symbol` si existe, si no, usa
    `_backfill_fetch`. Valida alineación y contigüidad, descarta velas fuera de base.
    """
    symbols = list({s.upper() for s in symbols})
    base = base_ms or intervalo_a_segundos(intervalo) * 1000
    res: Dict[str, List[dict]] = {}

    async def carga(sym: str) -> None:
        # 1) intento con helper del proyecto si existe
        velas: List[dict] = []
        if warmup_symbol is not None:
            try:
                df = await warmup_symbol(sym, intervalo, cliente, min_bars=min_bars)
                if df is not None and not df.empty:
                    rows = df.sort_values("timestamp").tail(min_bars).to_dict("records")
                    for r in rows:
                        ts = int(r.get("timestamp"))
                        if ts % base != 0:
                            continue
                        velas.append(
                            {
                                "symbol": sym,
                                "timestamp": ts,
                                "open": float(r.get("open")),
                                "high": float(r.get("high")),
                                "low": float(r.get("low")),
                                "close": float(r.get("close")),
                                "volume": float(r.get("volume", 0.0)),
                                "is_closed": True,
                            }
                        )
            except Exception:
                velas = []
        # 2) backfill directo si faltan
        if len(velas) < min_bars:
            try:
                faltan = min_bars - len(velas)
                extra = await _backfill_fetch(cliente, sym, intervalo, min_bars=faltan, base_ms=base)
                velas = (velas + extra)[-min_bars:]
            except Exception:
                pass
        # 3) filtrar y validar
        velas = [v for v in velas if timestamp_alineado(int(v["timestamp"]), intervalo)]
        velas = sorted(velas, key=lambda x: x["timestamp"])[:]
        if not _validar_contiguo(velas, base):
            _emit(on_event, "warmup_gap", {"symbol": sym, "count": len(velas)})
            res[sym] = []
            return
        res[sym] = velas
        _emit(on_event, "warmup_ok", {"symbol": sym, "count": len(velas)})

    await asyncio.gather(*(carga(s) for s in symbols))
    return res
