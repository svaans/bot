"""Compat patches executed at interpreter startup.

Currently this module ensures that :mod:`asyncio` exposes ``TaskGroup``
whenever the interpreter is older than Python 3.11.  It delegates to the
shared utilities so the same logic is available when importing our packages
outside of regular interpreter start-up (e.g. within pytest workers).
"""

from core.utils.asyncio_compat import ensure_taskgroup

ensure_taskgroup()


