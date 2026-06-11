#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backtester rápido y autónomo (solo librería estándar, sin pandas/numpy).

Replica el espíritu del bot: entradas por score ponderado de señales
técnicas con umbral, y salidas por SL/TP basados en ATR con trailing
(sl_ratio=1.5, tp_ratio=3.0, riesgo_por_trade=2%, igual que
config/configuraciones_optimas.json).

Uso:
    python backtesting/backtest_rapido.py                       # 5 símbolos, 1h, 730 días
    python backtesting/backtest_rapido.py --symbol BTCEUR --days 365
    python backtesting/backtest_rapido.py --sweep               # barrido de umbrales

Los datos se cachean en backtesting/cache/ — la primera ejecución
descarga de Binance; las siguientes son instantáneas.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
import traceback
import urllib.request
from dataclasses import dataclass, field

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
BINANCE = "https://api.binance.com/api/v3/klines"
CC_BASE = "https://min-api.cryptocompare.com/data/v2"  # fallback cloud-friendly
MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}

# Parámetros alineados con config/configuraciones_optimas.json
SL_RATIO = 1.5          # sl_ratio: stop loss a 1.5 * ATR
TP_RATIO = 3.0          # tp_ratio: take profit a 3.0 * ATR
RIESGO_POR_TRADE = 0.02 # riesgo_por_trade
COOLDOWN_TRAS_PERDIDA = 3
FEE = 0.001             # comisión spot Binance por lado (0.1%)
SLIPPAGE = 0.0005       # deslizamiento estimado por lado


# ---------------------------------------------------------------- datos

def _symbol_a_cc(symbol: str) -> tuple[str, str]:
    """BTCEUR → ('BTC','EUR'), ETHUSDT → ('ETH','USDT')."""
    for quote in ("USDT", "EUR", "USD", "GBP", "BTC", "ETH"):
        if symbol.endswith(quote):
            return symbol[: -len(quote)], quote
    raise ValueError(f"No se puede parsear símbolo: {symbol}")


def _descargar_klines_cc(symbol: str, days: int) -> list[list[float]]:
    """Fallback CryptoCompare (solo 1d). Requiere API key desde 2024."""
    base, quote = _symbol_a_cc(symbol)
    url = f"{CC_BASE}/histoday?fsym={base}&tsym={quote}&limit={days}"
    print(f"  [cryptocompare] {url}")
    with urllib.request.urlopen(url, timeout=15) as r:
        data = json.load(r)
    if data.get("Response") != "Success":
        raise RuntimeError(f"CryptoCompare error {symbol}: {data.get('Message', data)}")
    velas = []
    for item in data["Data"]["Data"]:
        if item["open"] == 0 and item["close"] == 0:
            continue
        velas.append([float(item["time"]) * 1000,
                      float(item["open"]), float(item["high"]),
                      float(item["low"]),  float(item["close"]),
                      float(item.get("volumefrom", 0))])
    velas.sort(key=lambda v: v[0])
    return velas


def _descargar_klines_yf(symbol: str, days: int) -> list[list[float]]:
    """Fallback Yahoo Finance vía yfinance (pip install yfinance).
    Funciona desde GitHub Actions sin autenticación."""
    try:
        import yfinance as yf
    except ImportError:
        raise RuntimeError("yfinance no instalado — ejecutar: pip install yfinance")

    # BTCEUR → BTC-EUR, ETHEUR → ETH-EUR
    for quote in ("USDT", "EUR", "USD", "GBP"):
        if symbol.endswith(quote):
            yf_sym = f"{symbol[:-len(quote)]}-{quote}"
            break
    else:
        raise ValueError(f"No se puede mapear a Yahoo Finance: {symbol}")

    import datetime
    fin = datetime.datetime.utcnow()
    inicio = fin - datetime.timedelta(days=days + 10)
    print(f"  [yfinance] descargando {yf_sym} desde {inicio.date()} hasta {fin.date()}")

    df = yf.download(
        yf_sym,
        start=inicio.strftime("%Y-%m-%d"),
        end=fin.strftime("%Y-%m-%d"),
        interval="1d",
        progress=False,
        auto_adjust=True,
    )
    if df is None or df.empty:
        raise RuntimeError(f"Yahoo Finance: sin datos para {yf_sym}")

    # yfinance >= 0.2.31 puede devolver MultiIndex — aplanamos
    if hasattr(df.columns, "levels"):
        df.columns = df.columns.get_level_values(0)

    velas: list[list[float]] = []
    for ts, row in df.iterrows():
        try:
            o, h, lo, c = float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"])
            v = float(row.get("Volume", 0) or 0)
        except (KeyError, TypeError, ValueError):
            continue
        if any(x != x for x in (o, h, lo, c)):  # NaN check
            continue
        ts_ms = float(ts.value) / 1_000_000  # ns → ms
        velas.append([ts_ms, o, h, lo, c, v])
    velas.sort(key=lambda x: x[0])
    return velas


def descargar_klines(symbol: str, interval: str, days: int) -> list[list[float]]:
    """Descarga velas (con caché en CSV).
    Cadena de fallback: Binance → CryptoCompare → Yahoo Finance (yfinance).
    Solo 1d soportado en fallback."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    ruta = os.path.join(CACHE_DIR, f"{symbol}_{interval}_{days}d.csv")
    if os.path.exists(ruta):
        with open(ruta, newline="") as f:
            return [[float(x) for x in row] for row in csv.reader(f)]

    fin = int(time.time() * 1000)
    inicio = fin - days * 86_400_000
    velas: list[list[float]] = []
    cursor = inicio
    try:
        while cursor < fin:
            url = (f"{BINANCE}?symbol={symbol}&interval={interval}"
                   f"&startTime={cursor}&limit=1000")
            with urllib.request.urlopen(url, timeout=15) as r:
                lote = json.load(r)
            if not lote:
                break
            for k in lote:
                velas.append([float(k[0]), float(k[1]), float(k[2]),
                              float(k[3]), float(k[4]), float(k[5])])
            cursor = int(lote[-1][0]) + MS[interval]
            time.sleep(0.15)
    except Exception as exc:
        print(f"  [binance] {symbol}/{interval} bloqueado: {exc!r}")
        velas = []

    if not velas:
        if interval != "1d":
            raise RuntimeError(
                f"Binance bloqueado e intervalo {interval!r} no soportado en fallback. "
                "Usa --interval 1d o ejecuta localmente."
            )
        for nombre, fn in [
            ("CryptoCompare", lambda: _descargar_klines_cc(symbol, days)),
            ("Yahoo Finance",  lambda: _descargar_klines_yf(symbol, days)),
        ]:
            try:
                print(f"  [fallback] probando {nombre} para {symbol}…")
                velas = fn()
                if velas:
                    print(f"  [{nombre}] {symbol}: {len(velas)} velas OK")
                    break
            except Exception as exc2:
                print(f"  [{nombre}] falló: {exc2!r}")
        else:
            raise RuntimeError(
                f"Todas las fuentes fallaron para {symbol}. "
                "Comprueba conectividad o ejecuta localmente."
            )

    with open(ruta, "w", newline="") as f:
        csv.writer(f).writerows(velas)
    return velas


# ------------------------------------------------------------ indicadores
# Todos en una sola pasada O(n), listas pre-asignadas: es lo que lo hace rápido.

def _calcular_adx(high: list[float], low: list[float], close: list[float],
                  periodo: int = 14) -> list[float]:
    """ADX de Wilder. Requiere >= 2*periodo+1 velas; el resto queda como nan."""
    n = len(close)
    adx_out = [math.nan] * n
    if n < 2 * periodo + 1:
        return adx_out

    tr_r = [0.0] * n
    pdm_r = [0.0] * n
    mdm_r = [0.0] * n
    for i in range(1, n):
        tr_r[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]),
                      abs(low[i] - close[i - 1]))
        up = high[i] - high[i - 1]
        dn = low[i - 1] - low[i]
        pdm_r[i] = up if (up > dn and up > 0) else 0.0
        mdm_r[i] = dn if (dn > up and dn > 0) else 0.0

    # primer suavizado Wilder: suma simple de las primeras `periodo` velas
    atr_w = sum(tr_r[1: periodo + 1])
    pdm_w = sum(pdm_r[1: periodo + 1])
    mdm_w = sum(mdm_r[1: periodo + 1])

    dx_buf: list[float] = []
    adx_val = 0.0
    adx_ready = False

    for i in range(periodo, n):
        if i > periodo:
            atr_w = atr_w - atr_w / periodo + tr_r[i]
            pdm_w = pdm_w - pdm_w / periodo + pdm_r[i]
            mdm_w = mdm_w - mdm_w / periodo + mdm_r[i]
        if atr_w == 0:
            dx_buf.append(0.0)
            continue
        pdi = 100.0 * pdm_w / atr_w
        mdi = 100.0 * mdm_w / atr_w
        d_sum = pdi + mdi
        dx = 100.0 * abs(pdi - mdi) / d_sum if d_sum > 0 else 0.0
        dx_buf.append(dx)
        if not adx_ready:
            if len(dx_buf) >= periodo:
                adx_val = sum(dx_buf) / periodo
                adx_ready = True
                adx_out[i] = adx_val
        else:
            adx_val = (adx_val * (periodo - 1) + dx) / periodo
            adx_out[i] = adx_val

    return adx_out


def calcular_indicadores(velas: list[list[float]]) -> dict[str, list[float]]:
    n = len(velas)
    close = [v[4] for v in velas]
    high = [v[2] for v in velas]
    low = [v[3] for v in velas]
    vol = [v[5] for v in velas]

    def ema(serie: list[float], periodo: int) -> list[float]:
        out = [math.nan] * len(serie)
        k = 2.0 / (periodo + 1)
        prev = serie[0]
        for i, x in enumerate(serie):
            prev = x * k + prev * (1 - k)
            out[i] = prev
        return out

    ema_fast = ema(close, 12)
    ema_slow = ema(close, 26)
    macd = [f - s for f, s in zip(ema_fast, ema_slow)]
    macd_signal = ema(macd, 9)
    ema9 = ema(close, 9)
    ema21 = ema(close, 21)
    ema200 = ema(close, 200)

    # RSI(14) Wilder
    rsi = [math.nan] * n
    avg_g = avg_l = 0.0
    for i in range(1, n):
        d = close[i] - close[i - 1]
        g, l = max(d, 0.0), max(-d, 0.0)
        if i <= 14:
            avg_g += g / 14
            avg_l += l / 14
        else:
            avg_g = (avg_g * 13 + g) / 14
            avg_l = (avg_l * 13 + l) / 14
        if i >= 14:
            rsi[i] = 100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l)

    # ATR(14) Wilder
    atr = [math.nan] * n
    a = 0.0
    for i in range(1, n):
        tr = max(high[i] - low[i], abs(high[i] - close[i - 1]),
                 abs(low[i] - close[i - 1]))
        a = a + tr / 14 if i <= 14 else (a * 13 + tr) / 14
        if i >= 14:
            atr[i] = a

    # media móvil de volumen (20)
    vol_ma = [math.nan] * n
    s = 0.0
    for i in range(n):
        s += vol[i]
        if i >= 20:
            s -= vol[i - 20]
            vol_ma[i] = s / 20

    # máximo de los 20 high previos (canal Donchian, excluye la vela actual)
    don_hi = [math.nan] * n
    for i in range(20, n):
        don_hi[i] = max(high[i - 20:i])

    adx = _calcular_adx(high, low, close)

    return {"close": close, "ema9": ema9, "ema21": ema21, "ema200": ema200,
            "rsi": rsi, "macd": macd, "macd_signal": macd_signal, "atr": atr,
            "vol": vol, "vol_ma": vol_ma, "don_hi": don_hi, "adx": adx}


# -------------------------------------------------------------- estrategia

def score_entrada(ind: dict[str, list[float]], i: int) -> float:
    """Score ponderado 0-10 imitando el enfoque multi-estrategia del bot."""
    s = 0.0
    # cruce EMA 9/21 alcista reciente (peso 3)
    if ind["ema9"][i] > ind["ema21"][i] and ind["ema9"][i - 1] <= ind["ema21"][i - 1]:
        s += 3.0
    elif ind["ema9"][i] > ind["ema21"][i]:
        s += 1.0
    # MACD por encima de señal y subiendo (peso 2.5)
    if ind["macd"][i] > ind["macd_signal"][i]:
        s += 1.5
        if ind["macd"][i] > ind["macd"][i - 1]:
            s += 1.0
    # RSI saliendo de sobreventa (peso 2.5)
    r, r1 = ind["rsi"][i], ind["rsi"][i - 1]
    if not math.isnan(r):
        if r1 < 30 <= r:
            s += 2.5
        elif 40 <= r <= 60:
            s += 0.5
    # volumen por encima de la media (peso 2)
    if not math.isnan(ind["vol_ma"][i]) and ind["vol"][i] > 1.5 * ind["vol_ma"][i]:
        s += 2.0
    return s


def score_entrada_v2(ind: dict[str, list[float]], i: int) -> float:
    """Score v1 + ruptura Donchian-20 (seguimiento de tendencia)."""
    s = score_entrada(ind, i)
    dh = ind["don_hi"][i]
    if not math.isnan(dh) and ind["close"][i] > dh:
        s += 3.0
    return s


@dataclass
class Resultado:
    symbol: str
    trades: int = 0
    ganadores: int = 0
    pnl_total: float = 0.0
    capital_final: float = 0.0
    max_drawdown: float = 0.0
    buy_hold: float = 0.0
    bruto_ganado: float = 0.0
    bruto_perdido: float = 0.0
    retornos: list[float] = field(default_factory=list)
    be_activados: int = 0

    @property
    def winrate(self) -> float:
        return 100.0 * self.ganadores / self.trades if self.trades else 0.0

    @property
    def profit_factor(self) -> float:
        return (self.bruto_ganado / self.bruto_perdido
                if self.bruto_perdido > 0 else float("inf"))


def backtest(velas: list[list[float]], symbol: str, capital0: float = 1000.0,
             umbral: float = 4.0, use_trailing: bool = True,
             trend_filter: bool = False,
             ind: dict[str, list[float]] | None = None,
             i0: int = 0, i1: int | None = None,
             sl_ratio: float = SL_RATIO, tp_ratio: float = TP_RATIO,
             vol_guard: bool = False, riesgo: float = RIESGO_POR_TRADE,
             senal_v2: bool = False,
             btc_ind: dict[str, list[float]] | None = None,
             be_atr: float = 0.0,
             adx_min: float = 0.0) -> Resultado:
    """vol_guard: modo adaptativo — no entra cuando la volatilidad (ATR/precio)
    supera 2x su media de 100 velas (régimen anómalo).
    btc_ind: si se pasa, filtro macro — solo entra cuando BTC > su EMA200
    (las altcoins caen con BTC; se asume el mismo índice temporal).
    senal_v2: añade ruptura Donchian-20 al score.
    be_atr: break-even stop — cuando el precio gana be_atr*ATR, SL sube a entrada
    (0 = desactivado). Reduce el riesgo en operaciones que se vuelven ganadoras.
    adx_min: filtro ADX — solo entra cuando ADX(14) >= adx_min (0 = desactivado).
    Evita entradas en mercados laterales sin tendencia definida."""
    if ind is None:
        ind = calcular_indicadores(velas)
    close = ind["close"]
    n = i1 if i1 is not None else len(velas)

    # media móvil del ratio ATR/precio para la guardia de volatilidad
    atr_ratio_ma: list[float] = []
    if vol_guard:
        atr_ratio_ma = [math.nan] * len(close)
        s = cnt = 0.0
        buf: list[float] = []
        for j in range(len(close)):
            a = ind["atr"][j]
            r = a / close[j] if not math.isnan(a) and close[j] > 0 else math.nan
            buf.append(r)
            if not math.isnan(r):
                s += r
                cnt += 1
            if len(buf) > 100:
                viejo = buf.pop(0)
                if not math.isnan(viejo):
                    s -= viejo
                    cnt -= 1
            if cnt >= 50:
                atr_ratio_ma[j] = s / cnt
    res = Resultado(symbol=symbol, capital_final=capital0)
    capital = capital0
    pico = capital
    en_pos = False
    qty = entrada = sl = tp = maximo = 0.0
    cooldown = 0
    be_activado = False
    entrada_mercado = 0.0

    for i in range(max(30, i0), n - 1):
        if cooldown > 0:
            cooldown -= 1

        if en_pos:
            h, l = velas[i][2], velas[i][3]
            maximo = max(maximo, h)
            if use_trailing:
                # trailing por ATR sobre el máximo alcanzado
                sl = max(sl, maximo - sl_ratio * ind["atr"][i])
            # break-even: cuando el precio sube be_atr*ATR, SL sube a entrada
            if be_atr > 0.0 and not be_activado and not math.isnan(ind["atr"][i]):
                if close[i] >= entrada_mercado + be_atr * ind["atr"][i]:
                    sl = max(sl, entrada_mercado)
                    be_activado = True
                    res.be_activados += 1
            precio_salida = None
            if l <= sl:
                precio_salida = sl
            elif h >= tp:
                precio_salida = tp
            if precio_salida is not None:
                salida_neta = precio_salida * (1 - FEE - SLIPPAGE)
                pnl = qty * (salida_neta - entrada)
                capital += qty * salida_neta
                res.trades += 1
                res.pnl_total += pnl
                res.retornos.append(pnl / capital0)
                if pnl > 0:
                    res.ganadores += 1
                    res.bruto_ganado += pnl
                else:
                    res.bruto_perdido += -pnl
                    cooldown = COOLDOWN_TRAS_PERDIDA
                en_pos = False
            pico = max(pico, capital if not en_pos else capital + qty * close[i])
            eq = capital if not en_pos else capital + qty * close[i]
            res.max_drawdown = max(res.max_drawdown, (pico - eq) / pico * 100)
            continue

        # entrada
        if trend_filter and not close[i] > ind["ema200"][i]:
            continue
        if vol_guard and not math.isnan(atr_ratio_ma[i]) and close[i] > 0:
            ratio = ind["atr"][i] / close[i]
            if not math.isnan(ratio) and ratio > 2.0 * atr_ratio_ma[i]:
                continue  # régimen de volatilidad anómala: no operar
        if btc_ind is not None and i < len(btc_ind["close"]):
            if not btc_ind["close"][i] > btc_ind["ema200"][i]:
                continue  # mercado macro bajista: no comprar
        # filtro ADX: solo entrar en mercados con tendencia definida
        if adx_min > 0.0:
            adx_val = ind["adx"][i]
            if math.isnan(adx_val) or adx_val < adx_min:
                continue
        score_fn = score_entrada_v2 if senal_v2 else score_entrada
        if cooldown == 0 and score_fn(ind, i) >= umbral and not math.isnan(ind["atr"][i]):
            precio = close[i] * (1 + FEE + SLIPPAGE)
            riesgo_unitario = sl_ratio * ind["atr"][i]
            qty = (capital * riesgo) / riesgo_unitario
            qty = min(qty, capital / precio)  # sin apalancamiento
            if qty * precio < 10:  # mínimo de orden ~10 EUR
                continue
            entrada = precio
            entrada_mercado = close[i]
            be_activado = False
            sl = close[i] - sl_ratio * ind["atr"][i]
            tp = close[i] + tp_ratio * ind["atr"][i]
            maximo = close[i]
            capital -= qty * precio  # capital restante queda líquido
            en_pos = True

    if en_pos:  # cerrar al final
        salida_neta = close[n - 1] * (1 - FEE - SLIPPAGE)
        pnl = qty * (salida_neta - entrada)
        capital += qty * salida_neta
        res.trades += 1
        res.pnl_total += pnl
        res.retornos.append(pnl / capital0)
        if pnl > 0:
            res.ganadores += 1
            res.bruto_ganado += pnl
        else:
            res.bruto_perdido += -pnl

    res.capital_final = capital
    res.buy_hold = (close[n - 1] / close[max(30, i0)] - 1) * 100
    return res


# ----------------------------------------------------------------- estudio

def estudio_timeframes(symbols: list[str], days: int, capital0: float) -> None:
    """Grid timeframe x umbral x trailing x filtro de tendencia.

    Optimiza sobre el 70% inicial (train) y valida sobre el 30% final
    (test, fuera de muestra) para detectar sobreajuste.
    """
    intervalos = ["15m", "1h", "4h", "1d"]
    filas = []
    for itv in intervalos:
        datos, indicadores = {}, {}
        for s in symbols:
            datos[s] = descargar_klines(s, itv, days)
            indicadores[s] = calcular_indicadores(datos[s])
        n_min = min(len(v) for v in datos.values())
        corte = int(n_min * 0.7)
        print(f"[{itv}] velas={n_min} train=0..{corte} test={corte}..{n_min}")

        for umbral in (3.0, 4.0, 5.0):
            for trailing in (True, False):
                for tendencia in (True, False):
                    agg = {}
                    for fase, a, b in (("train", 0, corte), ("test", corte, None)):
                        cap = gan = per = 0.0
                        ntr = 0
                        for s in symbols:
                            r = backtest(datos[s], s, capital0, umbral,
                                         use_trailing=trailing,
                                         trend_filter=tendencia,
                                         ind=indicadores[s], i0=a, i1=b)
                            cap += r.capital_final
                            gan += r.bruto_ganado
                            per += r.bruto_perdido
                            ntr += r.trades
                        agg[fase] = {
                            "ret": (cap / (capital0 * len(symbols)) - 1) * 100,
                            "pf": gan / per if per > 0 else float("inf"),
                            "trades": ntr,
                        }
                    filas.append((itv, umbral, trailing, tendencia, agg))

    filas.sort(key=lambda f: f[4]["test"]["pf"] if f[4]["test"]["pf"] != float("inf") else -1,
               reverse=True)
    print(f"\n{'tf':>4s} {'umbral':>6s} {'trail':>5s} {'tend':>4s} | "
          f"{'PF train':>8s} {'ret train':>9s} {'n':>4s} | "
          f"{'PF test':>8s} {'ret test':>9s} {'n':>4s}")
    for itv, u, tr, te, agg in filas:
        t0, t1 = agg["train"], agg["test"]
        pf0 = f"{t0['pf']:.2f}" if t0["pf"] != float("inf") else "inf"
        pf1 = f"{t1['pf']:.2f}" if t1["pf"] != float("inf") else "inf"
        print(f"{itv:>4s} {u:6.1f} {str(tr):>5s} {str(te):>4s} | "
              f"{pf0:>8s} {t0['ret']:+8.2f}% {t0['trades']:4d} | "
              f"{pf1:>8s} {t1['ret']:+8.2f}% {t1['trades']:4d}")


def estudio_profundo(symbols: list[str], days: int, capital0: float) -> None:
    """Grid amplio en 4h/1d: umbral x sl x tp x tendencia x guardia de vol.

    Train 0-70%, test 70-100% (fuera de muestra). Imprime el top 25 por PF
    de test entre las configs con PF>1 en train y >=30 trades en train.
    """
    filas = []
    for itv in ("4h", "1d"):
        datos, indicadores = {}, {}
        for s in symbols:
            datos[s] = descargar_klines(s, itv, days)
            indicadores[s] = calcular_indicadores(datos[s])
        n_por_simbolo = {s: len(v) for s, v in datos.items()}
        print(f"[{itv}] velas por símbolo: {n_por_simbolo}")

        for umbral in (4.0, 5.0):
            for sl in (1.0, 1.5, 2.0):
                for tp in (2.0, 3.0, 4.5):
                    for tendencia in (True, False):
                        for guardia in (True, False):
                            agg = {}
                            for fase in ("train", "test"):
                                cap = gan = per = 0.0
                                ntr = 0
                                for s in symbols:
                                    n_s = n_por_simbolo[s]
                                    corte = int(n_s * 0.7)
                                    a, b = (0, corte) if fase == "train" else (corte, None)
                                    r = backtest(
                                        datos[s], s, capital0, umbral,
                                        use_trailing=False,
                                        trend_filter=tendencia,
                                        ind=indicadores[s], i0=a, i1=b,
                                        sl_ratio=sl, tp_ratio=tp,
                                        vol_guard=guardia)
                                    cap += r.capital_final
                                    gan += r.bruto_ganado
                                    per += r.bruto_perdido
                                    ntr += r.trades
                                agg[fase] = {
                                    "ret": (cap / (capital0 * len(symbols)) - 1) * 100,
                                    "pf": gan / per if per > 0 else float("inf"),
                                    "trades": ntr,
                                }
                            filas.append((itv, umbral, sl, tp, tendencia,
                                          guardia, agg))

    robustas = [f for f in filas
                if f[6]["train"]["pf"] > 1.0 and f[6]["train"]["trades"] >= 30]
    robustas.sort(key=lambda f: min(f[6]["train"]["pf"], f[6]["test"]["pf"])
                  if f[6]["test"]["pf"] != float("inf") else 0, reverse=True)
    print(f"\nConfigs con PF>1 en train y >=30 trades: {len(robustas)} de {len(filas)}")
    print(f"\n{'tf':>4s} {'umb':>4s} {'sl':>4s} {'tp':>4s} {'tend':>5s} {'vgrd':>5s} | "
          f"{'PF tr':>6s} {'ret tr':>8s} {'n':>4s} | {'PF te':>6s} {'ret te':>8s} {'n':>4s}")
    for itv, u, sl, tp, te, vg, agg in robustas[:25]:
        t0, t1 = agg["train"], agg["test"]
        pf1 = f"{t1['pf']:.2f}" if t1["pf"] != float("inf") else "inf"
        print(f"{itv:>4s} {u:4.1f} {sl:4.1f} {tp:4.1f} {str(te):>5s} {str(vg):>5s} | "
              f"{t0['pf']:6.2f} {t0['ret']:+7.2f}% {t0['trades']:4d} | "
              f"{pf1:>6s} {t1['ret']:+7.2f}% {t1['trades']:4d}")


def estudio_mejoras(symbols: list[str], days: int, capital0: float) -> None:
    """Palancas de rentabilidad sobre la base ganadora del study2.

    Base fija: 1d, umbral 5, SL 1.0xATR, sin trailing, guardia vol ON.
    Grid: señal v1/v2 (Donchian) x filtro macro BTC x TP x riesgo/trade.
    """
    datos, indicadores = {}, {}
    for s in symbols:
        datos[s] = descargar_klines(s, "1d", days)
        indicadores[s] = calcular_indicadores(datos[s])
    btc = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    filas = []
    for v2 in (False, True):
        for btc_f in (False, True):
            for tp in (3.0, 4.5, 6.0):
                for riesgo in (0.02, 0.04):
                    agg = {}
                    for fase in ("train", "test"):
                        cap = gan = per = 0.0
                        ntr = 0
                        dd = 0.0
                        for s in symbols:
                            n_s = len(datos[s])
                            corte = int(n_s * 0.7)
                            a, b = (0, corte) if fase == "train" else (corte, None)
                            r = backtest(
                                datos[s], s, capital0, 5.0,
                                use_trailing=False, trend_filter=False,
                                ind=indicadores[s], i0=a, i1=b,
                                sl_ratio=1.0, tp_ratio=tp, vol_guard=True,
                                riesgo=riesgo, senal_v2=v2,
                                btc_ind=btc if btc_f else None)
                            cap += r.capital_final
                            gan += r.bruto_ganado
                            per += r.bruto_perdido
                            ntr += r.trades
                            dd = max(dd, r.max_drawdown)
                        agg[fase] = {
                            "ret": (cap / (capital0 * len(symbols)) - 1) * 100,
                            "pf": gan / per if per > 0 else float("inf"),
                            "trades": ntr,
                            "dd": dd,
                        }
                    filas.append((v2, btc_f, tp, riesgo, agg))

    filas.sort(key=lambda f: min(f[4]["train"]["pf"], f[4]["test"]["pf"])
               if f[4]["test"]["pf"] != float("inf") else 0, reverse=True)
    dias_train = days * 0.7
    dias_test = days * 0.3
    print(f"\n{'don':>4s} {'btcF':>4s} {'tp':>4s} {'rsg':>4s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'n':>4s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'n':>4s} {'DDmax':>6s}")
    for v2, btc_f, tp, riesgo, agg in filas:
        t0, t1 = agg["train"], agg["test"]
        an0 = ((1 + t0["ret"] / 100) ** (365 / dias_train) - 1) * 100
        an1 = ((1 + t1["ret"] / 100) ** (365 / dias_test) - 1) * 100
        pf1 = f"{t1['pf']:.2f}" if t1["pf"] != float("inf") else "inf"
        print(f"{str(v2):>4s} {str(btc_f):>4s} {tp:4.1f} {riesgo:4.2f} | "
              f"{t0['pf']:6.2f} {an0:+8.2f}% {t0['trades']:4d} | "
              f"{pf1:>6s} {an1:+8.2f}% {t1['trades']:4d} {t1['dd']:5.1f}%")


def backtest_rotacion(symbols: list[str], days: int, capital0: float) -> None:
    """Estrategia alternativa de referencia: rotación de momentum.

    Cada 7 días invierte a partes iguales en las 2 monedas con mejor
    retorno de 90 días (si es positivo) siempre que BTC > EMA200;
    si no, queda en efectivo. Comisiones en cada rebalanceo.
    """
    datos = {s: descargar_klines(s, "1d", days) for s in symbols}
    n = min(len(v) for v in datos.values())
    closes = {s: [v[4] for v in datos[s][-n:]] for s in symbols}
    btc_ind = calcular_indicadores(datos["BTCEUR"][-n:])

    for fase, a, b in (("train", 200, int(n * 0.7)), ("test", int(n * 0.7), n)):
        capital = capital0
        pico = capital
        dd = 0.0
        posicion: dict[str, float] = {}  # symbol -> unidades
        rebalanceos = 0
        for i in range(a, b):
            eq = capital + sum(q * closes[s][i] for s, q in posicion.items())
            pico = max(pico, eq)
            dd = max(dd, (pico - eq) / pico * 100)
            if (i - a) % 7 != 0:
                continue
            alcista = btc_ind["close"][i] > btc_ind["ema200"][i]
            ranking = sorted(
                ((closes[s][i] / closes[s][i - 90] - 1, s) for s in symbols
                 if i >= 90),
                reverse=True)
            objetivo = [s for roc, s in ranking[:2] if roc > 0] if alcista else []
            actuales = set(posicion)
            if set(objetivo) == actuales:
                continue
            # vender todo y comprar el objetivo (simplificado)
            for s, q in posicion.items():
                capital += q * closes[s][i] * (1 - FEE - SLIPPAGE)
            posicion = {}
            if objetivo:
                por_moneda = capital / len(objetivo)
                for s in objetivo:
                    precio = closes[s][i] * (1 + FEE + SLIPPAGE)
                    posicion[s] = por_moneda / precio
                capital = 0.0
            rebalanceos += 1
        eq = capital + sum(q * closes[s][b - 1] for s, q in posicion.items())
        dias_fase = b - a
        anual = ((eq / capital0) ** (365 / dias_fase) - 1) * 100
        print(f"rotacion[{fase}] {capital0:.0f} -> {eq:.2f} EUR "
              f"({(eq / capital0 - 1) * 100:+.2f}%, {anual:+.2f}% anualizado) "
              f"maxDD={dd:.1f}% rebalanceos={rebalanceos}")


def estudio_v4(symbols: list[str], days: int, capital0: float) -> None:
    """Break-even stop + filtro ADX sobre la base ganadora del study3.

    Base fija: 1d, umbral 5, SL 1.0xATR, sin trailing, guardia vol ON,
    filtro macro BTC ON.  Grid: be_atr x adx_min x TP x riesgo/trade.

    Hipótesis: BE stop reduce el DD al convertir en breakeven las
    operaciones que revierten; ADX filtra entradas en mercados sin
    tendencia.  Juntos permiten usar 6% riesgo/trade manteniendo DD
    aceptable y alcanzar el objetivo 16-20% anualizado.
    """
    datos, indicadores = {}, {}
    for s in symbols:
        datos[s] = descargar_klines(s, "1d", days)
        indicadores[s] = calcular_indicadores(datos[s])
    btc = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    filas = []
    for be in (0.0, 0.8, 1.0, 1.5):
        for adx_min in (0, 18, 22, 25):
            for tp in (3.0, 4.5):
                for riesgo in (0.04, 0.06):
                    agg: dict[str, dict[str, float]] = {}
                    for fase in ("train", "test"):
                        cap = gan = per = 0.0
                        ntr = 0
                        dd = 0.0
                        for s in symbols:
                            n_s = len(datos[s])
                            corte = int(n_s * 0.7)
                            a, b = (0, corte) if fase == "train" else (corte, None)
                            r = backtest(
                                datos[s], s, capital0, 5.0,
                                use_trailing=False, trend_filter=False,
                                ind=indicadores[s], i0=a, i1=b,
                                sl_ratio=1.0, tp_ratio=tp, vol_guard=True,
                                riesgo=riesgo, senal_v2=False,
                                btc_ind=btc,
                                be_atr=be, adx_min=float(adx_min))
                            cap += r.capital_final
                            gan += r.bruto_ganado
                            per += r.bruto_perdido
                            ntr += r.trades
                            dd = max(dd, r.max_drawdown)
                        agg[fase] = {
                            "ret": (cap / (capital0 * len(symbols)) - 1) * 100,
                            "pf": gan / per if per > 0 else float("inf"),
                            "trades": ntr,
                            "dd": dd,
                        }
                    filas.append((be, adx_min, tp, riesgo, agg))

    filas.sort(key=lambda f: min(f[4]["train"]["pf"], f[4]["test"]["pf"])
               if f[4]["test"]["pf"] != float("inf") else 0, reverse=True)
    dias_train = days * 0.7
    dias_test = days * 0.3
    print(f"\n{'be':>5s} {'adx':>4s} {'tp':>4s} {'rsg':>4s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'n':>4s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'n':>4s} {'DDmax':>6s}")
    for be, adx_min, tp, riesgo, agg in filas:
        t0, t1 = agg["train"], agg["test"]
        an0 = ((1 + t0["ret"] / 100) ** (365 / dias_train) - 1) * 100
        an1 = ((1 + t1["ret"] / 100) ** (365 / dias_test) - 1) * 100
        pf1 = f"{t1['pf']:.2f}" if t1["pf"] != float("inf") else "inf"
        print(f"{be:5.1f} {adx_min:4d} {tp:4.1f} {riesgo:4.2f} | "
              f"{t0['pf']:6.2f} {an0:+8.2f}% {t0['trades']:4d} | "
              f"{pf1:>6s} {an1:+8.2f}% {t1['trades']:4d} {t1['dd']:5.1f}%")


def estudio_simbolos(symbols: list[str], days: int, capital0: float) -> None:
    """Ranking de símbolos con la config óptima validada (study3/study4).

    Config fija: 1d, umbral 5, SL 1.0×ATR, TP 3.0×ATR, vol_guard ON,
    filtro macro BTC ON, riesgo 4%/trade, be=0, adx=0.

    Evalúa cada símbolo individualmente con validación 70% train / 30% test
    (out-of-sample). Admite símbolos actuales y candidatos mezclados.
    Los símbolos candidatos se marcan con (*) en la tabla de resultados.
    """
    SIMBOLOS_ACTUALES = {"BTCEUR", "ETHEUR", "SOLEUR", "XRPEUR", "AVAXEUR"}

    # BTC siempre descargado para filtro macro
    btc_data = descargar_klines("BTCEUR", "1d", days)
    btc_ind = calcular_indicadores(btc_data)

    filas = []
    for s in symbols:
        print(f"  descargando {s}...")
        try:
            data = descargar_klines(s, "1d", days)
            ind = calcular_indicadores(data)
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
            continue

        n = len(data)
        corte = int(n * 0.7)
        fila: dict = {"symbol": s, "candidato": s not in SIMBOLOS_ACTUALES}

        for fase, a, b in (("train", 0, corte), ("test", corte, n)):
            r = backtest(
                data, s, capital0, 5.0,
                use_trailing=False, trend_filter=False,
                ind=ind, i0=a, i1=b,
                sl_ratio=1.0, tp_ratio=3.0, vol_guard=True,
                riesgo=0.04, senal_v2=False,
                btc_ind=btc_ind,
                be_atr=0.0, adx_min=0.0,
            )
            dias_fase = b - a
            anual = ((r.capital_final / capital0) ** (365 / dias_fase) - 1) * 100 if dias_fase > 0 else 0.0
            pf_val = r.profit_factor if r.profit_factor != float("inf") else 999.0
            fila[fase] = {
                "pf": pf_val,
                "anual": anual,
                "trades": r.trades,
                "wr": r.winrate,
                "dd": r.max_drawdown,
                "bh": r.buy_hold,
            }
        filas.append(fila)

    # ordenar por PF test descendente
    filas.sort(key=lambda f: f.get("test", {}).get("pf", 0), reverse=True)

    print(f"\n{'Símbolo':>10s}  {'':1s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'n tr':>5s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'n te':>5s} {'WR te':>6s} {'DD te':>6s} {'B&H te':>7s}")
    print("-" * 90)
    for f in filas:
        mark = "*" if f["candidato"] else " "
        tr, te = f.get("train", {}), f.get("test", {})
        pf_tr = f"{tr['pf']:.2f}" if tr.get("pf", 0) < 900 else "inf"
        pf_te = f"{te['pf']:.2f}" if te.get("pf", 0) < 900 else "inf"
        print(f"{f['symbol']:>10s}  {mark:1s} | "
              f"{pf_tr:>6s} {tr.get('anual', 0):+8.2f}% {tr.get('trades', 0):5d} | "
              f"{pf_te:>6s} {te.get('anual', 0):+8.2f}% {te.get('trades', 0):5d} "
              f"{te.get('wr', 0):5.1f}% {te.get('dd', 0):5.1f}% {te.get('bh', 0):+6.2f}%")

    print("\n(*) = candidato nuevo  |  config: 1d umbral=5 SL=1×ATR TP=3×ATR riesgo=4% vol_guard+BTC_macro")

    # resumen cartera: símbolos actuales vs selección óptima
    actuales_en_lista = [f for f in filas if not f["candidato"]]
    positivos_test = [f for f in filas if f.get("test", {}).get("pf", 0) >= 1.2]
    print(f"\nActuales en la lista: {len(actuales_en_lista)} | "
          f"Símbolos con PF test ≥1.2: {len(positivos_test)} "
          f"({', '.join(f['symbol'] for f in positivos_test)})")


# ----------------------------------------------------------------- main

def fmt(res: Resultado, capital0: float, dias: int) -> str:
    ret = (res.capital_final / capital0 - 1) * 100
    anual = ((res.capital_final / capital0) ** (365.0 / dias) - 1) * 100 if dias else 0
    pf = f"{res.profit_factor:.2f}" if res.profit_factor != float("inf") else "inf"
    return (f"{res.symbol:8s} trades={res.trades:4d} winrate={res.winrate:5.1f}% "
            f"PF={pf:>5s} ret={ret:+7.2f}% anualizado={anual:+7.2f}% "
            f"maxDD={res.max_drawdown:5.1f}% buy&hold={res.buy_hold:+7.2f}%")


def main() -> None:
    p = argparse.ArgumentParser(description="Backtest rápido sin dependencias")
    p.add_argument("--symbol", action="append",
                   help="ej. BTCEUR (repetible); por defecto los 5 del bot")
    p.add_argument("--interval", default="1h", choices=list(MS))
    p.add_argument("--days", type=int, default=730)
    p.add_argument("--capital", type=float, default=1000.0)
    p.add_argument("--umbral", type=float, default=4.0)
    p.add_argument("--sweep", action="store_true",
                   help="barrido de umbrales 2.0..6.0")
    p.add_argument("--study", action="store_true",
                   help="estudio timeframe x parametros con validacion 70/30")
    p.add_argument("--study2", action="store_true",
                   help="estudio profundo 4h/1d: sl/tp/tendencia/guardia vol")
    p.add_argument("--study3", action="store_true",
                   help="palancas: Donchian, filtro BTC, TP amplio, riesgo")
    p.add_argument("--study4", action="store_true",
                   help="break-even stop + filtro ADX: camino a 16-20% anual")
    p.add_argument("--study_simbolos", action="store_true",
                   help="ranking de simbolos: actuales + candidatos con config optima validada")
    p.add_argument("--rotacion", action="store_true",
                   help="estrategia de referencia: rotacion de momentum")
    args = p.parse_args()

    CANDIDATOS = ["ADAEUR", "BNBEUR", "LINKEUR", "DOGEEUR", "LTCEUR", "DOTEUR"]
    symbols = args.symbol or ["BTCEUR", "ETHEUR", "SOLEUR", "XRPEUR", "AVAXEUR"]

    if args.study:
        t0 = time.perf_counter()
        estudio_timeframes(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio completo: {time.perf_counter() - t0:.1f}s")
        return

    if args.study2:
        t0 = time.perf_counter()
        estudio_profundo(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio profundo: {time.perf_counter() - t0:.1f}s")
        return

    if args.study3:
        t0 = time.perf_counter()
        estudio_mejoras(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio mejoras: {time.perf_counter() - t0:.1f}s")
        return

    if args.study4:
        t0 = time.perf_counter()
        estudio_v4(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio v4 (BE+ADX): {time.perf_counter() - t0:.1f}s")
        return

    if args.study_simbolos:
        t0 = time.perf_counter()
        # si el usuario no pasó --symbol explícito, usar actuales + candidatos
        all_syms = args.symbol or (["BTCEUR", "ETHEUR", "SOLEUR", "XRPEUR", "AVAXEUR"] + CANDIDATOS)
        estudio_simbolos(all_syms, args.days, args.capital)
        print(f"\n[tiempo] estudio simbolos: {time.perf_counter() - t0:.1f}s")
        return

    if args.rotacion:
        t0 = time.perf_counter()
        backtest_rotacion(symbols, args.days, args.capital)
        print(f"\n[tiempo] rotacion: {time.perf_counter() - t0:.1f}s")
        return

    t0 = time.perf_counter()
    datos = {}
    for s in symbols:
        datos[s] = descargar_klines(s, args.interval, args.days)
        print(f"[datos] {s}: {len(datos[s])} velas {args.interval}")
    t_datos = time.perf_counter() - t0

    t0 = time.perf_counter()
    if args.sweep:
        for u in [2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0]:
            print(f"\n--- umbral {u} ---")
            for s in symbols:
                print(fmt(backtest(datos[s], s, args.capital, u),
                          args.capital, args.days))
    else:
        print(f"\nResultados ({args.days} días, {args.interval}, "
              f"capital inicial {args.capital:.0f} EUR por símbolo, "
              f"comisión {FEE*100:.1f}%/lado + slippage {SLIPPAGE*100:.2f}%):\n")
        total0 = total1 = 0.0
        for s in symbols:
            r = backtest(datos[s], s, args.capital, args.umbral)
            print(fmt(r, args.capital, args.days))
            total0 += args.capital
            total1 += r.capital_final
        ret = (total1 / total0 - 1) * 100
        print(f"\nCartera total: {total0:.0f} -> {total1:.2f} EUR ({ret:+.2f}%)")
    t_bt = time.perf_counter() - t0
    print(f"\n[tiempo] descarga/caché: {t_datos:.2f}s | backtest: {t_bt:.3f}s")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("\n[FATAL] Error no capturado:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
