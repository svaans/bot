# binance_api/cliente.py
import os
import asyncio
import functools
import json
import random
import re
import time
import math
from typing import Any, Callable

import aiohttp
import ccxt
import websockets
from config.config_manager import Config
from core.utils.utils import configurar_logger
from core.metrics import registrar_binance_weight

log = configurar_logger('binance_client')

AUTH_WARNING_EMITTED = False
CB_SILENCE_SECONDS = float(os.getenv("CB_SILENCE_SECONDS", "60"))

# Estado del circuit breaker y métricas por endpoint
_CIRCUIT_BREAKERS: dict[str, dict[str, Any]] = {}

# Tasks for the Binance user data stream
_USER_STREAM_TASK: asyncio.Task | None = None
_KEEPALIVE_TASK: asyncio.Task | None = None

_HTTP_SESSION: aiohttp.ClientSession | None = None


def _get_session() -> aiohttp.ClientSession:
    """Devuelve una sesión HTTP reutilizable."""
    global _HTTP_SESSION
    if _HTTP_SESSION is None or _HTTP_SESSION.closed:
        connector = aiohttp.TCPConnector(
            limit=int(os.getenv("HTTP_CONN_LIMIT", "100")),
            ttl_dns_cache=int(os.getenv("HTTP_DNS_CACHE", "300")),
        )
        timeout = aiohttp.ClientTimeout(
            total=float(os.getenv("HTTP_TOTAL_TIMEOUT", "15")),
            connect=float(os.getenv("HTTP_CONNECT_TIMEOUT", "10")),
            sock_read=float(os.getenv("HTTP_SOCK_READ_TIMEOUT", "10")),
        )
        _HTTP_SESSION = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _HTTP_SESSION


class BinanceError(Exception):
    """Excepción propia con código y sugerencia."""

    def __init__(self, code: int, reason: str, suggestion: str) -> None:
        super().__init__(f"{code}: {reason} - {suggestion}")
        self.code = code
        self.reason = reason
        self.suggestion = suggestion


class CircuitBreakerOpen(BinanceError):
    def __init__(self, endpoint: str, remaining: float):
        super().__init__(429, "Circuit breaker", f"Espera {remaining:.1f}s")
        self.endpoint = endpoint
        self.remaining = remaining


def _extract_code(exc: Exception) -> int | None:
    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code
    try:
        data = json.loads(str(exc))
        return int(data.get("code"))
    except Exception:
        match = re.search(r'"code":(-?\d+)', str(exc))
        if match:
            return int(match.group(1))
    return None


def binance_call(
    fn: Callable[[], Any], *, signed: bool = False, endpoint: str | None = None, symbol: str | None = None
) -> Any:
    """
    Envoltura con circuit breaker, backoff e integración de métricas de weight.
    Nota: esta función es síncrona; se ejecuta dentro de run_in_executor.
    """
    endpoint = endpoint or getattr(fn, "__name__", "unknown")
    state = _CIRCUIT_BREAKERS.setdefault(
        endpoint,
        {
            "fails": 0,       # fallos consecutivos para circuit breaker
            "until": 0.0,     # epoch hasta que se reintenta
            "silence": 0.0,   # ventana de silencio para logs
            "attempts": 0,    # total de intentos
            "failures": 0,    # total de fallos
            "last_code": None,
        },
    )
    state["attempts"] += 1
    now = time.time()
    if state["until"] > now:
        remaining = max(0.0, state["until"] - now)
        if now >= state.get("silence", 0.0):
            log.error(f"Circuit breaker activo para {endpoint} ({remaining:.1f}s restantes)")
            state["silence"] = now + CB_SILENCE_SECONDS
        raise CircuitBreakerOpen(endpoint, remaining)

    max_attempts = 5
    base = 0.5
    jitter = 0.1
    time_synced = False
    attempt = 1
    while attempt <= max_attempts:
        try:
            result = fn()
            # Extrae weight de cabeceras si ccxt lo expone
            exchange = getattr(fn, "__self__", None)
            used = 0
            if exchange and getattr(exchange, "last_response", None):
                hdrs = exchange.last_response.headers or {}
                try:
                    used = int(
                        hdrs.get("X-MBX-USED-WEIGHT-1m")
                        or hdrs.get("X-MBX-USED-IP-WEIGHT-1m")
                        or hdrs.get("X-MBX-ORDER-COUNT-1m", 0)
                    )
                except Exception:
                    used = 0
            registrar_binance_weight(used)
            if used > 1000:
                log.warning(f"⚠️ Weight {used}/1200 used")

            state["fails"] = 0
            state["until"] = 0.0
            state["last_code"] = None
            return result

        except Exception as exc:
            code = _extract_code(exc)
            state["last_code"] = code
            state["failures"] += 1
            status = getattr(exc, "http_status", None) or getattr(exc, "status", None)

            # Límite de tasa
            if code == -1003 or (isinstance(status, int) and status in (429, 418)):
                headers = getattr(getattr(exc, "response", None), "headers", {}) or {}
                retry_after = headers.get("Retry-After")
                try:
                    wait = math.ceil(float(retry_after))
                except Exception:
                    wait = 60
                state["until"] = time.time() + wait
                if time.time() >= state.get("silence", 0.0):
                    log.error(f"🕒 Límite de tasa excedido en {endpoint}, pausando {wait} s")
                    state["silence"] = time.time() + CB_SILENCE_SECONDS
                raise BinanceError(code or status or 429, "Rate limit hit", f"Backoff {wait}s")

            # 5xx → suma fallos; abre CB corto si se repite
            is_5xx = isinstance(status, int) and 500 <= status < 600
            if is_5xx:
                state["fails"] += 1
                if state["fails"] > 3:
                    state["until"] = time.time() + 30
            else:
                state["fails"] = 0

            # errores finales sin reintento
            final = False
            if code in (-1013, -1100, -1102, -1130):  # parámetro inválido / filtro
                final = True

            # Desfase horario
            if code == -1021 and not time_synced:
                exchange = getattr(fn, "__self__", None)
                if exchange and hasattr(exchange, "load_time_difference"):
                    try:
                        exchange.load_time_difference()
                    except Exception:
                        pass
                time_synced = True
                attempt += 1
                continue

            # Auth inválida prolongada
            if code == -2015:
                state["until"] = time.time() + random.randint(600, 900)
                if time.time() >= state.get("silence", 0.0):
                    log.error(f"Error -2015 en {endpoint}: {exc}")
                    state["silence"] = time.time() + CB_SILENCE_SECONDS
                final = True
            elif state["fails"] > 3 or attempt >= max_attempts:
                final = True

            # Anotar metadatos útiles en la excepción
            exc.endpoint = endpoint
            exc.symbol = symbol
            exc.signed = signed
            exc.attempts = attempt

            if final:
                if code != -2015 and time.time() >= state.get("silence", 0.0):
                    log.error(f"Error final en {endpoint}: {exc}")
                    state["silence"] = time.time() + CB_SILENCE_SECONDS
                raise

            log.debug(f"Intento {attempt} fallido en {endpoint}: {exc}")
            espera = base * (2 ** (attempt - 1)) + random.random() * jitter
            # Nota: binance_call es síncrona; este sleep corre en el executor.
            time.sleep(espera)
            attempt += 1


# === user data stream ===
async def _start_user_stream(exchange) -> None:
    """
    Inicializa el user data stream de Binance con base URL correcta y tareas de keepalive/WS.
    Soporta Spot y USDM Futures según exchange.options['defaultType'].
    """
    global _USER_STREAM_TASK, _KEEPALIVE_TASK
    # Evita duplicar listener si ya está activo
    if _USER_STREAM_TASK and not _USER_STREAM_TASK.done():
        return
    # Cancela keepalive previo si sigue vivo
    if _KEEPALIVE_TASK and not _KEEPALIVE_TASK.done():
        _KEEPALIVE_TASK.cancel()

    default_type = (getattr(exchange, "options", {}) or {}).get("defaultType", "spot")
    if default_type == "future":
        rest_base = "https://fapi.binance.com"
        ws_user_builder = lambda lk: f"wss://fstream.binance.com/ws/{lk}"
        user_stream_path = "/fapi/v1/listenKey"
    else:
        rest_base = "https://api.binance.com"
        ws_user_builder = lambda lk: f"wss://stream.binance.com:9443/ws/{lk}"
        user_stream_path = "/api/v3/userDataStream"

    headers = {"X-MBX-APIKEY": exchange.apiKey or ""}

    session = _get_session()
    try:
        resp = await session.post(f"{rest_base}{user_stream_path}", headers=headers)
        if resp.status >= 400:
            txt = await resp.text()
            log.error(f"UserDataStream HTTP {resp.status}: {txt}")
            return
        data = await resp.json()
    except Exception as e:
        log.error(f"No se pudo iniciar user data stream: {e}")
        return

    listen_key = data.get("listenKey")
    if not listen_key:
        log.error("Respuesta sin listenKey al iniciar user data stream")
        return

    # Lanzar tareas
    _USER_STREAM_TASK = asyncio.create_task(_user_stream_ws_generic(ws_user_builder(listen_key), exchange))
    _KEEPALIVE_TASK = asyncio.create_task(_keepalive_listen_key_generic(rest_base, user_stream_path, headers, listen_key))


async def _keepalive_listen_key_generic(rest_base: str, path: str, headers: dict[str, str], listen_key: str) -> None:
    session = _get_session()
    while True:
        try:
            r = await session.put(f"{rest_base}{path}", headers=headers, params={"listenKey": listen_key})
            if r.status >= 400:
                body = await r.text()
                log.error(f"Keepalive listenKey falló ({r.status}): {body}")
        except Exception as e:
            log.error(f"Error renovando listenKey: {e}")
        await asyncio.sleep(30 * 60)


async def _user_stream_ws_generic(ws_url: str, exchange) -> None:
    """Escucha user stream (Spot o Futures) y mantiene caché local de órdenes."""
    retries = 0
    while True:
        try:
            async with websockets.connect(
                ws_url,
                open_timeout=int(os.getenv("WS_OPEN_TIMEOUT", "30")),
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                max_queue=0,
                compression="deflate",
            ) as ws:
                retries = 0
                async for msg in ws:
                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue
                    # Spot: e="executionReport"
                    # Futures: a veces viene {"stream":..., "data": {...}}
                    payload = data.get("data") if "stream" in data else data
                    if not isinstance(payload, dict):
                        continue
                    evt = payload.get("e")
                    if evt != "executionReport":
                        continue
                    status = payload.get("X")
                    if status not in {"FILLED", "EXPIRED"}:
                        continue
                    raw = payload.get("s", "")
                    try:
                        symbol = exchange.market_id_to_symbol(raw)
                    except Exception:
                        try:
                            symbol = exchange.safe_symbol(raw)
                        except Exception:
                            log.warning(f"No pude normalizar símbolo de user stream: {raw}")
                            symbol = raw
                    try:
                        from core.orders import real_orders
                        real_orders.eliminar_orden(symbol, forzar_log=True)
                    except Exception as err:
                        log.error(f"Error actualizando orden {symbol} desde user stream: {err}")
        except Exception as e:
            log.error(f"Error en websocket de user stream: {e}")
            retries += 1
            delay = min(60, 2 ** min(6, retries)) + random.random()
            await asyncio.sleep(delay)


def auth_guard(default: Any = None):
    """Bloquea llamadas privadas en modo simulado o sin auth."""

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            global AUTH_WARNING_EMITTED
            if not self.modo_real or not self.authenticated:
                if not AUTH_WARNING_EMITTED:
                    log.warning("🔒 AuthGuard: llamadas privadas bloqueadas en modo SIMULADO")
                    AUTH_WARNING_EMITTED = True
                if isinstance(default, Exception):
                    raise default
                if callable(default):
                    return default()
                return default
            return await func(self, *args, **kwargs)

        return wrapper

    return decorator


class BinanceClient:
    """Wrapper asíncrono mínimo sobre el cliente Binance."""

    def __init__(self, config: Config | None = None) -> None:
        self.exchange = crear_cliente(config)
        self.authenticated = bool(
            getattr(self.exchange, "apiKey", None) and getattr(self.exchange, "secret", None)
        )
        self.modo_real = getattr(self.exchange, "_modo_real", True)

    async def execute(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        loop = asyncio.get_running_loop()
        parcial = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, parcial)

    @auth_guard(lambda: {'total': {'EUR': 1000.0}, 'free': {'EUR': 1000.0}})
    async def fetch_balance(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_balance, *args, **kwargs)

    @auth_guard(PermissionError('Auth required'))
    async def create_order(self, *args, **kwargs):
        return await self.execute(self.exchange.create_order, *args, **kwargs)

    @auth_guard(PermissionError('Auth required'))
    async def create_market_buy_order(self, *args, **kwargs):
        return await self.execute(self.exchange.create_market_buy_order, *args, **kwargs)

    @auth_guard(PermissionError('Auth required'))
    async def create_market_sell_order(self, *args, **kwargs):
        return await self.execute(self.exchange.create_market_sell_order, *args, **kwargs)

    @auth_guard(lambda: [])
    async def fetch_open_orders(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_open_orders, *args, **kwargs)

    async def fetch_ticker(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_ticker, *args, **kwargs)

    async def fetch_order_book(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_order_book, *args, **kwargs)

    async def load_markets(self, *args, **kwargs):
        return await self.execute(self.exchange.load_markets, *args, **kwargs)

    async def fetch_ohlcv(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_ohlcv, *args, **kwargs)


def crear_cliente(config: Config | None = None):
    if config is None:
        try:
            from config import config as app_config
            config = getattr(app_config, "cfg", None)
        except Exception:
            config = None
    modo_real = getattr(config, "modo_real", True)

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_SECRET") or os.getenv("BINANCE_API_SECRET")
    base = os.getenv("BINANCE_BASE")
    testnet = os.getenv("BINANCE_TESTNET", "false").lower() == "true"
    default_type = os.getenv("BINANCE_DEFAULT_TYPE", "spot")

    exchange = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": default_type,
                "adjustForTimeDifference": True,
                "warnOnFetchOpenOrdersWithoutSymbol": False,
            },
        }
    )
    if base:
        exchange.urls["api"] = base
    exchange.set_sandbox_mode(testnet or not modo_real)
    try:
        exchange.load_time_difference()
    except Exception:
        pass
    if modo_real and not testnet and api_key:
        try:
            asyncio.get_event_loop().create_task(_start_user_stream(exchange))
        except RuntimeError:
            # No hay loop activo; el caller deberá iniciar user stream más tarde.
            pass
    exchange._modo_real = modo_real

    def _wrap(name: str, signed: bool) -> None:
        original = getattr(exchange, name, None)
        if not callable(original):
            return

        def caller(*args, **kwargs):
            symbol = kwargs.get('symbol')
            if symbol is None and args:
                first = args[0]
                if isinstance(first, str):
                    symbol = first
            return binance_call(lambda: original(*args, **kwargs), signed=signed, endpoint=name, symbol=symbol)

        setattr(exchange, name, caller)

    wrappers = {
        'fetch_balance': True,
        'create_order': True,
        'create_market_buy_order': True,
        'create_market_sell_order': True,
        'fetch_open_orders': True,
        'fetch_ticker': False,
        'load_markets': False,
        'fetch_ohlcv': False,
        'fetch_order_book': False,
        'fetch_my_trades': True,
        'cancel_order': True,
        'fetch_order': False,
    }
    for nombre, firmado in wrappers.items():
        _wrap(nombre, firmado)

    return exchange


obtener_cliente = crear_cliente


def filtrar_simbolos_activos(symbols: list[str], config: Config | None = None) -> tuple[list[str], list[str]]:
    """Devuelve dos listas con símbolos activos e inactivos en Binance.

    Si ocurre cualquier error al consultar los mercados, se asume que todos
    los símbolos son válidos para no interrumpir la ejecución.
    """
    try:
        cliente = crear_cliente(config)
        mercados = cliente.load_markets()
    except Exception:
        return symbols, []

    activos: list[str] = []
    inactivos: list[str] = []
    for sym in symbols:
        info = mercados.get(sym)
        if not info or not info.get("active", True):
            inactivos.append(sym)
        else:
            activos.append(sym)
    return activos, inactivos


async def fetch_balance_async(cliente, *args, **kwargs):
    """
    Obtiene el balance de forma asíncrona.
    Si ``cliente`` es una instancia de ``BinanceClient`` se usan sus
    reintentos. Si no hay claves API (modo simulado), devuelve un balance
    ficticio.
    """
    if isinstance(cliente, BinanceClient):
        return await cliente.fetch_balance(*args, **kwargs)
    if not getattr(cliente, 'apiKey', None) or not getattr(cliente, 'secret', None):
        return {'total': {'EUR': 1000.0}, 'free': {'EUR': 1000.0}}
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.fetch_balance, *args, **kwargs)


async def create_order_async(cliente, *args, **kwargs):
    """Versión asíncrona de ``create_order`` con reintentos."""
    if isinstance(cliente, BinanceClient):
        return await cliente.create_order(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.create_order, *args, **kwargs)


async def create_market_buy_order_async(cliente, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        return await cliente.create_market_buy_order(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.create_market_buy_order, *args, **kwargs)


async def create_market_sell_order_async(cliente, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        return await cliente.create_market_sell_order(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.create_market_sell_order, *args, **kwargs)


async def fetch_open_orders_async(cliente, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        return await cliente.fetch_open_orders(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.fetch_open_orders, *args, **kwargs)


async def fetch_ticker_async(cliente, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        return await cliente.fetch_ticker(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.fetch_ticker, *args, **kwargs)


async def fetch_order_book_async(cliente, *args, **kwargs):
    """Versión asíncrona de ``fetch_order_book`` con reintentos."""
    if isinstance(cliente, BinanceClient):
        return await cliente.fetch_order_book(*args, **kwargs)
    loop = asyncio.get_running_loop()
    func = functools.partial(cliente.fetch_order_book, *args, **kwargs)
    return await loop.run_in_executor(None, func)


async def load_markets_async(cliente, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        return await cliente.load_markets(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.load_markets, *args, **kwargs)


async def fetch_ohlcv_async(cliente, *args, **kwargs):
    """Versión asíncrona de `fetch_ohlcv` con reintentos."""
    if isinstance(cliente, BinanceClient):
        return await cliente.fetch_ohlcv(*args, **kwargs)
    loop = asyncio.get_running_loop()
    func = functools.partial(cliente.fetch_ohlcv, *args, **kwargs)
    return await loop.run_in_executor(None, func)


async def close_http_session() -> None:
    """Cierra la sesión HTTP reutilizable si sigue abierta."""
    global _HTTP_SESSION
    if _HTTP_SESSION and not _HTTP_SESSION.closed:
        await _HTTP_SESSION.close()

