"""Alinear el reloj del sistema con la hora UTC de Binance (solo Windows).

``SetSystemTime`` exige proceso elevado (administrador). Sin admin la llamada
falla y las peticiones firmadas siguen dependiendo de CCXT ``adjustForTimeDifference``.

Uso manual (consola **como administrador**), desde la raíz del repo::

    python -m core.utils.binance_time_sync

Arranque del bot: define ``BINANCE_SYNC_SYSTEM_TIME=true`` en ``claves.env`` para
intentar la sincronización una vez antes del chequeo de drift.
"""
from __future__ import annotations

import json
import sys
import urllib.request
from datetime import datetime, timezone
from typing import Final

_BINANCE_TIME_URL: Final = "https://api.binance.com/api/v3/time"


def try_sync_windows_system_time_from_binance(*, timeout: float = 5.0) -> bool:
    """Ajusta el reloj del sistema a la hora UTC de Binance. Solo Windows; requiere admin.

    Returns
    -------
    bool
        ``True`` si ``SetSystemTime`` tuvo éxito.
    """
    if sys.platform != "win32":
        return False

    import ctypes
    from ctypes import wintypes

    try:
        req = urllib.request.Request(
            _BINANCE_TIME_URL,
            headers={"User-Agent": "bot-binance-time-sync/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        server_ms = int(data.get("serverTime", 0))
        if not server_ms:
            return False
    except Exception:
        return False

    secs = server_ms / 1000.0
    ms_part = int(server_ms % 1000)
    t = datetime.fromtimestamp(secs, tz=timezone.utc)

    class SYSTEMTIME(ctypes.Structure):
        _fields_ = [
            ("wYear", wintypes.WORD),
            ("wMonth", wintypes.WORD),
            ("wDayOfWeek", wintypes.WORD),
            ("wDay", wintypes.WORD),
            ("wHour", wintypes.WORD),
            ("wMinute", wintypes.WORD),
            ("wSecond", wintypes.WORD),
            ("wMilliseconds", wintypes.WORD),
        ]

    st = SYSTEMTIME()
    st.wYear = t.year
    st.wMonth = t.month
    st.wDayOfWeek = 0
    st.wDay = t.day
    st.wHour = t.hour
    st.wMinute = t.minute
    st.wSecond = t.second
    st.wMilliseconds = ms_part

    return bool(ctypes.windll.kernel32.SetSystemTime(ctypes.byref(st)))


def main() -> None:
    if sys.platform != "win32":
        print("Solo disponible en Windows.")
        sys.exit(1)
    if try_sync_windows_system_time_from_binance():
        print("Reloj del sistema actualizado con la hora UTC de Binance.")
        sys.exit(0)
    print(
        "No se pudo actualizar el reloj. Ejecuta esta consola como administrador "
        "o revisa conectividad con api.binance.com."
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
