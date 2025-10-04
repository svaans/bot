import time
import logging
from contextlib import contextmanager

log = logging.getLogger("phase")


@contextmanager
def phase(name: str, extra: dict | None = None):
    t0 = time.perf_counter()
    log.info(f"➡️ start:{name}", extra=extra or {})
    try:
        yield
        dt = (time.perf_counter() - t0) * 1000
        log.info(
            f"✅ end:{name}",
            extra={"elapsed_ms": round(dt, 2), **(extra or {})},
        )
    except Exception as e:
        dt = (time.perf_counter() - t0) * 1000
        log.error(
            f"💥 fail:{name}",
            extra={"elapsed_ms": round(dt, 2), "error": repr(e), **(extra or {})},
        )
        raise