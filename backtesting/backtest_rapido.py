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
             adx_min: float = 0.0,
             fg_mask: list[bool] | None = None,
             eq_dd_pausa: float = 0.0,
             eq_dd_reduccion: float = 0.0,
             trail_activacion_atr: float = 0.0) -> Resultado:
    """vol_guard: modo adaptativo — no entra cuando la volatilidad (ATR/precio)
    supera 2x su media de 100 velas (régimen anómalo).
    btc_ind: si se pasa, filtro macro — solo entra cuando BTC > su EMA200.
    senal_v2: añade ruptura Donchian-20 al score.
    be_atr: break-even stop — SL sube a entrada cuando precio gana be_atr×ATR.
    adx_min: filtro ADX mínimo para entrar (0 = desactivado).
    fg_mask: lista bool por vela; False = bloquear entrada (Fear&Greed filter).
    eq_dd_pausa: DD de equity (0-1) a partir del cual se pausan nuevas entradas.
      Ej: 0.15 = pausar si el capital cae >15% desde su máximo. 0 = desactivado.
    eq_dd_reduccion: DD de equity (0-1) a partir del cual se reduce riesgo a la mitad.
      Ej: 0.10 = reducir riesgo al 50% si DD >10%. 0 = desactivado.
    trail_activacion_atr: trailing diferido — solo activa el trailing cuando el precio
      gana trail_activacion_atr×ATR desde la entrada. 0 = inmediato (comportamiento
      clásico). Requiere use_trailing=True para tener efecto."""
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
    trailing_activo = False
    entrada_mercado = 0.0

    for i in range(max(30, i0), n - 1):
        if cooldown > 0:
            cooldown -= 1

        if en_pos:
            h, l = velas[i][2], velas[i][3]
            maximo = max(maximo, h)
            if use_trailing:
                if trail_activacion_atr > 0.0 and not trailing_activo:
                    # activación diferida: esperar a que el precio gane el umbral
                    if not math.isnan(ind["atr"][i]) and ind["atr"][i] > 0:
                        if close[i] >= entrada_mercado + trail_activacion_atr * ind["atr"][i]:
                            trailing_activo = True
                else:
                    trailing_activo = True  # inmediato (trail_activacion_atr == 0)
                if trailing_activo:
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
        if fg_mask is not None and i < len(fg_mask) and not fg_mask[i]:
            continue  # Fear & Greed filter bloquea esta vela
        # Equity Curve Filter: ajuste dinámico de riesgo según drawdown actual
        riesgo_efectivo = riesgo
        if eq_dd_pausa > 0.0 or eq_dd_reduccion > 0.0:
            dd_actual = (pico - capital) / pico if pico > 0 else 0.0
            if eq_dd_pausa > 0.0 and dd_actual >= eq_dd_pausa:
                continue  # pausa total de entradas
            if eq_dd_reduccion > 0.0 and dd_actual >= eq_dd_reduccion:
                riesgo_efectivo = riesgo * 0.5  # mitad del riesgo
        score_fn = score_entrada_v2 if senal_v2 else score_entrada
        if cooldown == 0 and score_fn(ind, i) >= umbral and not math.isnan(ind["atr"][i]):
            precio = close[i] * (1 + FEE + SLIPPAGE)
            riesgo_unitario = sl_ratio * ind["atr"][i]
            qty = (capital * riesgo_efectivo) / riesgo_unitario
            qty = min(qty, capital / precio)  # sin apalancamiento
            if qty * precio < 10:  # mínimo de orden ~10 EUR
                continue
            entrada = precio
            entrada_mercado = close[i]
            be_activado = False
            trailing_activo = False
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


def _descargar_fear_greed(dias: int = 1825) -> dict[int, int]:
    """Descarga el Fear & Greed Index histórico de alternative.me (sin API key).

    Devuelve un dict {timestamp_dia_ms: valor_0_100}.
    Cachea en backtesting/cache/fear_greed.json para no pedir en cada run.
    """
    ruta = os.path.join(CACHE_DIR, "fear_greed.json")
    os.makedirs(CACHE_DIR, exist_ok=True)

    # usar caché si tiene menos de 24h
    if os.path.exists(ruta):
        mtime = os.path.getmtime(ruta)
        if time.time() - mtime < 86_400:
            with open(ruta) as f:
                raw = json.load(f)
            return {int(k): int(v) for k, v in raw.items()}

    url = f"https://api.alternative.me/fng/?limit={dias + 30}&format=json"
    print(f"  [fear&greed] descargando histórico desde alternative.me…")
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            data = json.load(r)
    except Exception as exc:
        print(f"  [fear&greed] falló: {exc!r}")
        return {}

    resultado: dict[int, int] = {}
    for item in data.get("data", []):
        ts_s = int(item["timestamp"])
        # normalizar al inicio del día UTC (truncar a día)
        dia_ms = (ts_s // 86_400) * 86_400 * 1000
        resultado[dia_ms] = int(item["value"])

    with open(ruta, "w") as f:
        json.dump({str(k): v for k, v in resultado.items()}, f)
    print(f"  [fear&greed] {len(resultado)} días descargados OK")
    return resultado


def _fg_valor(fg_map: dict[int, int], ts_ms: float) -> int | None:
    """Retorna el valor F&G para el timestamp dado (ms), buscando el día exacto
    o el día anterior si no hay dato exacto. Retorna None si no hay dato."""
    dia_ms = (int(ts_ms) // (86_400 * 1000)) * 86_400 * 1000
    for offset in (0, -86_400_000, -2 * 86_400_000):
        v = fg_map.get(dia_ms + offset)
        if v is not None:
            return v
    return None


FUTURES_SYMBOL_MAP: dict[str, str] = {
    "BTCEUR": "BTCUSDT",
    "ETHEUR": "ETHUSDT",
    "SOLEUR": "SOLUSDT",
    "XRPEUR": "XRPUSDT",
    "AVAXEUR": "AVAXUSDT",
}

OKX_SYMBOL_MAP: dict[str, str] = {
    "BTCUSDT": "BTC-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "SOLUSDT": "SOL-USDT-SWAP",
    "XRPUSDT": "XRP-USDT-SWAP",
    "AVAXUSDT": "AVAX-USDT-SWAP",
}


def _descargar_funding_rate_historico(symbol_futures: str, days: int) -> dict[int, float]:
    """Descarga histórico de funding rate de Binance Futures USDTM (sin API key).

    Binance liquida el funding rate cada 8h → 3 por día.
    Retorna {timestamp_inicio_dia_utc_ms: fr_promedio_diario}.
    Cachea en backtesting/cache/fr_{symbol}.json para no repetir la descarga.
    Intenta Binance fapi primero; si falla por geo-bloqueo (HTTP 451) usa Bybit
    como fallback (los funding rates están altamente correlacionados entre exchanges).
    """
    ruta = os.path.join(CACHE_DIR, f"fr_{symbol_futures}.json")
    os.makedirs(CACHE_DIR, exist_ok=True)

    if os.path.exists(ruta):
        mtime = os.path.getmtime(ruta)
        if time.time() - mtime < 86_400:
            with open(ruta) as f:
                raw = json.load(f)
            return {int(k): float(v) for k, v in raw.items()}

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 86_400_000

    def _fetch_binance() -> list[dict]:
        rates: list[dict] = []
        current_start = start_ms
        while current_start < end_ms:
            url = (f"https://fapi.binance.com/fapi/v1/fundingRate"
                   f"?symbol={symbol_futures}&startTime={current_start}&limit=1000")
            with urllib.request.urlopen(url, timeout=15) as resp:
                batch = json.loads(resp.read())
            if not batch:
                break
            rates.extend(batch)
            last_ts = int(batch[-1]["fundingTime"])
            if len(batch) < 1000 or last_ts >= end_ms:
                break
            current_start = last_ts + 1
        return [{"fundingTime": int(r["fundingTime"]), "fundingRate": float(r["fundingRate"])}
                for r in rates]

    def _fetch_bybit() -> list[dict]:
        rates: list[dict] = []
        cursor_end = end_ms
        while True:
            url = (f"https://api.bybit.com/v5/market/funding/history"
                   f"?category=linear&symbol={symbol_futures}"
                   f"&startTime={start_ms}&endTime={cursor_end}&limit=200")
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = json.loads(resp.read())
            items = data.get("result", {}).get("list", [])
            if not items:
                break
            for item in items:
                rates.append({
                    "fundingTime": int(item["fundingRateTimestamp"]),
                    "fundingRate": float(item["fundingRate"]),
                })
            if len(items) < 200:
                break
            cursor_end = int(items[-1]["fundingRateTimestamp"]) - 1
            if cursor_end <= start_ms:
                break
        return rates

    def _fetch_okx() -> list[dict]:
        okx_sym = OKX_SYMBOL_MAP.get(symbol_futures)
        if not okx_sym:
            return []
        rates: list[dict] = []
        cursor_after = ""
        while True:
            url = (f"https://www.okx.com/api/v5/public/funding-rate-history"
                   f"?instId={okx_sym}&limit=100"
                   + (f"&after={cursor_after}" if cursor_after else ""))
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            if data.get("code") != "0":
                break
            items = data.get("data", [])
            if not items:
                break
            for item in items:
                ts = int(item["fundingTime"])
                if ts < start_ms:
                    return rates
                rates.append({"fundingTime": ts, "fundingRate": float(item["fundingRate"])})
            if len(items) < 100:
                break
            cursor_after = items[-1]["fundingTime"]
        return rates

    all_rates: list[dict] = []
    for fuente, fetch_fn in [("Binance", _fetch_binance),
                              ("Bybit",   _fetch_bybit),
                              ("OKX",     _fetch_okx)]:
        print(f"  [funding] descargando {symbol_futures} desde {fuente}…")
        try:
            all_rates = fetch_fn()
            if all_rates:
                print(f"  [funding] {symbol_futures}: {len(all_rates)} registros ({fuente}) OK")
                break
        except Exception as exc:
            print(f"  [funding] {symbol_futures} ({fuente}): {exc!r}")

    daily: dict[int, list[float]] = {}
    for entry in all_rates:
        ts_ms = int(entry["fundingTime"])
        ts_day = (ts_ms // 86_400_000) * 86_400_000
        daily.setdefault(ts_day, []).append(float(entry["fundingRate"]))

    resultado = {ts: sum(v) / len(v) for ts, v in daily.items()}
    with open(ruta, "w") as f:
        json.dump({str(k): v for k, v in resultado.items()}, f)
    if not resultado:
        print(f"  [funding] {symbol_futures}: sin datos — filtro FR desactivado para este símbolo")
    return resultado


def _fr_valor(fr_map: dict[int, float], ts_ms: float) -> float | None:
    """Retorna el funding rate diario para el timestamp dado (ms)."""
    dia_ms = (int(ts_ms) // 86_400_000) * 86_400_000
    for offset in (0, -86_400_000, -2 * 86_400_000):
        v = fr_map.get(dia_ms + offset)
        if v is not None:
            return v
    return None


def estudio_eth_riesgo(days: int, capital0: float) -> None:
    """Busca el riesgo óptimo por trade para ETH con la config validada.

    Grid: riesgo_por_trade de 2% a 10% en pasos de 1%.
    Config base fija: 1d, umbral=5, SL=1×ATR, TP=3×ATR, vol_guard ON,
    filtro macro BTC ON, be=0, adx=0.

    Muestra train/test con DD y retorno para encontrar el sweet-spot
    entre rentabilidad máxima y drawdown aceptable (<20%).
    """
    print("  descargando ETHEUR…")
    eth_data = descargar_klines("ETHEUR", "1d", days)
    eth_ind = calcular_indicadores(eth_data)
    btc_data = descargar_klines("BTCEUR", "1d", days)
    btc_ind = calcular_indicadores(btc_data)

    n = len(eth_data)
    corte = int(n * 0.7)
    dias_train = corte
    dias_test = n - corte

    riesgos = [0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10]

    print(f"\n{'riesgo':>7s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'DD tr':>6s} {'n tr':>4s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'DD te':>6s} {'n te':>4s}  decisión")
    print("-" * 88)

    for riesgo in riesgos:
        resultados = {}
        for fase, a, b in (("train", 0, corte), ("test", corte, n)):
            r = backtest(
                eth_data, "ETHEUR", capital0, 5.0,
                use_trailing=False, trend_filter=False,
                ind=eth_ind, i0=a, i1=b,
                sl_ratio=1.0, tp_ratio=3.0, vol_guard=True,
                riesgo=riesgo, senal_v2=False,
                btc_ind=btc_ind,
                be_atr=0.0, adx_min=0.0,
            )
            dias_f = b - a
            anual = ((r.capital_final / capital0) ** (365 / dias_f) - 1) * 100
            pf = r.profit_factor if r.profit_factor != float("inf") else 999.0
            resultados[fase] = {"pf": pf, "anual": anual, "dd": r.max_drawdown, "n": r.trades}

        tr, te = resultados["train"], resultados["test"]
        pf_tr = f"{tr['pf']:.2f}" if tr["pf"] < 900 else "inf"
        pf_te = f"{te['pf']:.2f}" if te["pf"] < 900 else "inf"

        # decisión: marcamos el riesgo si DD test < 20% y mejora vs riesgo 4%
        decision = ""
        if te["dd"] <= 20.0 and te["anual"] > 0:
            decision = "✓ viable"
        if te["dd"] > 25.0:
            decision = "✗ DD alto"

        print(f"{riesgo*100:6.0f}% | "
              f"{pf_tr:>6s} {tr['anual']:+8.2f}% {tr['dd']:5.1f}% {tr['n']:4d} | "
              f"{pf_te:>6s} {te['anual']:+8.2f}% {te['dd']:5.1f}% {te['n']:4d}  {decision}")

    print(f"\nconfig fija: 1d umbral=5 SL=1×ATR TP=3×ATR vol_guard+BTC_macro  "
          f"(ETH, {days}d, 70/30 OOS)")


def estudio_fear_greed(symbols: list[str], days: int, capital0: float) -> None:
    """Evalúa el Fear & Greed Index como filtro de entradas (alternative.me).

    Compara 4 regímenes de filtro vs baseline sin filtro:
      - sin_filtro:    todas las entradas (baseline)
      - evitar_codicia: bloquea entradas cuando F&G > 75
      - evitar_miedo:   bloquea entradas cuando F&G < 25
      - zona_neutral:  solo entra cuando 25 ≤ F&G ≤ 75
      - solo_miedo:    solo entra cuando F&G < 40 (teoría contrarian)

    Usa la config óptima validada sobre todos los símbolos del portfolio.
    """
    fg_map = _descargar_fear_greed(days)
    if not fg_map:
        print("  [fear&greed] sin datos históricos — omitiendo estudio")
        return

    print("  descargando datos de mercado…")
    datos, indicadores = {}, {}
    for s in symbols:
        try:
            datos[s] = descargar_klines(s, "1d", days)
            indicadores[s] = calcular_indicadores(datos[s])
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
    btc_ind = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    # variantes de filtro: None = sin límite
    variantes = [
        ("sin_filtro",     None,   None),
        ("evitar_codicia", None,   75),
        ("evitar_miedo",   25,     None),
        ("zona_neutral",   25,     75),
        ("solo_miedo",     None,   40),
    ]

    print(f"\n{'variante':>16s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'n tr':>5s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'n te':>5s} {'DD te':>6s}")
    print("-" * 82)

    for nombre, fg_min, fg_max in variantes:
        agg: dict[str, dict] = {}
        for fase in ("train", "test"):
            cap = gan = per = 0.0
            ntr = 0
            dd = 0.0
            for s, data in datos.items():
                n_s = len(data)
                corte = int(n_s * 0.7)
                a, b = (0, corte) if fase == "train" else (corte, n_s)
                ind = indicadores[s]

                # construir máscara F&G: lista de bool (True = permitido operar)
                fg_mask: list[bool] | None = None
                if fg_map and (fg_min is not None or fg_max is not None):
                    fg_mask = []
                    for v in data:
                        fg_val = _fg_valor(fg_map, v[0])
                        permitido = True
                        if fg_val is not None:
                            if fg_min is not None and fg_val < fg_min:
                                permitido = False
                            if fg_max is not None and fg_val > fg_max:
                                permitido = False
                        fg_mask.append(permitido)

                r = backtest(
                    data, s, capital0, 5.0,
                    use_trailing=False, trend_filter=False,
                    ind=ind, i0=a, i1=b,
                    sl_ratio=1.0, tp_ratio=3.0, vol_guard=True,
                    riesgo=0.04, senal_v2=False,
                    btc_ind=btc_ind,
                    be_atr=0.0, adx_min=0.0,
                    fg_mask=fg_mask,
                )
                cap += r.capital_final
                gan += r.bruto_ganado
                per += r.bruto_perdido
                ntr += r.trades
                dd = max(dd, r.max_drawdown)

            n_sym = len(datos)
            dias_fase = (corte if fase == "train" else (len(list(datos.values())[0]) - corte)) if datos else 1
            ret = (cap / (capital0 * n_sym) - 1) * 100 if n_sym else 0
            anual = ((cap / (capital0 * n_sym)) ** (365 / dias_fase) - 1) * 100 if n_sym and dias_fase > 0 else 0
            pf = gan / per if per > 0 else float("inf")
            agg[fase] = {"pf": pf, "anual": anual, "n": ntr, "dd": dd}

        tr, te = agg["train"], agg["test"]
        pf_tr = f"{tr['pf']:.2f}" if tr["pf"] != float("inf") else "inf"
        pf_te = f"{te['pf']:.2f}" if te["pf"] != float("inf") else "inf"
        print(f"{nombre:>16s} | "
              f"{pf_tr:>6s} {tr['anual']:+8.2f}% {tr['n']:5d} | "
              f"{pf_te:>6s} {te['anual']:+8.2f}% {te['n']:5d} {te['dd']:5.1f}%")

    print(f"\nFuente: alternative.me/fng  |  {len(fg_map)} días históricos  |  "
          f"config: 1d umbral=5 SL=1×ATR TP=3×ATR riesgo=4% vol_guard+BTC_macro")


def _sharpe(retornos: list[float], dias_año: float = 365.0) -> float:
    """Sharpe anualizado desde lista de retornos diarios (fracción del capital0)."""
    n = len(retornos)
    if n < 2:
        return 0.0
    media = sum(retornos) / n
    var = sum((r - media) ** 2 for r in retornos) / (n - 1)
    std = math.sqrt(var) if var > 0 else 0.0
    if std == 0:
        return 0.0
    return (media / std) * math.sqrt(dias_año)


def estudio_sharpe_allocation(symbols: list[str], days: int, capital0: float) -> None:
    """Calcula la asignación óptima de riesgo por símbolo basada en Sharpe ratio.

    Cada símbolo recibe un riesgo_por_trade proporcional a su Sharpe en
    el período de TEST (out-of-sample). El total permanece igual que usar
    4% en todos (presupuesto de riesgo constante).

    Rango permitido: 2% min — 8% max por símbolo para evitar concentración.

    Muestra:
    - Sharpe test de cada símbolo con riesgo base 4%
    - Riesgo asignado con ponderación Sharpe
    - Backtest con nueva asignación vs baseline igualitario
    """
    print("  descargando datos…")
    datos, indicadores = {}, {}
    for s in symbols:
        try:
            datos[s] = descargar_klines(s, "1d", days)
            indicadores[s] = calcular_indicadores(datos[s])
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
    if not datos:
        return
    btc_ind = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    # --- fase 1: medir Sharpe en test (out-of-sample) con riesgo 4% igual ---
    sharpes: dict[str, float] = {}
    for s, data in datos.items():
        n_s = len(data)
        corte = int(n_s * 0.7)
        r = backtest(
            data, s, capital0, 5.0,
            use_trailing=False, trend_filter=False,
            ind=indicadores[s], i0=corte, i1=n_s,
            sl_ratio=1.0, tp_ratio=3.0, vol_guard=True,
            riesgo=0.04, senal_v2=False, btc_ind=btc_ind,
        )
        sharpes[s] = _sharpe(r.retornos)

    # --- fase 2: normalizar Sharpe → asignación de riesgo ---
    sharpe_pos = {s: max(v, 0.01) for s, v in sharpes.items()}  # mín 0.01 para no excluir
    total_sharpe = sum(sharpe_pos.values())
    n_sym = len(sharpe_pos)
    riesgo_base_total = 0.04 * n_sym  # mismo presupuesto total que baseline
    RIESGO_MIN, RIESGO_MAX = 0.02, 0.08

    asignaciones: dict[str, float] = {}
    for s, sh in sharpe_pos.items():
        raw = (sh / total_sharpe) * riesgo_base_total
        asignaciones[s] = max(RIESGO_MIN, min(RIESGO_MAX, raw))

    print(f"\n{'símbolo':>10s} {'Sharpe te':>10s} {'riesgo base':>12s} {'riesgo Sharpe':>14s}")
    print("-" * 50)
    for s in sorted(asignaciones, key=lambda x: sharpes[x], reverse=True):
        print(f"{s:>10s} {sharpes[s]:+9.3f}  {'4.0%':>12s}  {asignaciones[s]*100:>12.1f}%")

    # --- fase 3: comparar baseline vs asignación Sharpe en test ---
    def _eval_portfolio(riesgo_map: dict[str, float]) -> dict:
        cap = gan = per = 0.0
        ntr = 0
        dd = 0.0
        for s, data in datos.items():
            n_s = len(data)
            corte = int(n_s * 0.7)
            r = backtest(
                data, s, capital0, 5.0,
                use_trailing=False, trend_filter=False,
                ind=indicadores[s], i0=corte, i1=n_s,
                sl_ratio=1.0, tp_ratio=3.0, vol_guard=True,
                riesgo=riesgo_map.get(s, 0.04), senal_v2=False,
                btc_ind=btc_ind,
            )
            cap += r.capital_final
            gan += r.bruto_ganado
            per += r.bruto_perdido
            ntr += r.trades
            dd = max(dd, r.max_drawdown)
        dias_test = int(len(list(datos.values())[0]) * 0.3)
        anual = ((cap / (capital0 * n_sym)) ** (365 / dias_test) - 1) * 100
        pf = gan / per if per > 0 else float("inf")
        return {"anual": anual, "pf": pf, "n": ntr, "dd": dd}

    baseline = _eval_portfolio({s: 0.04 for s in datos})
    sharpe_alloc = _eval_portfolio(asignaciones)

    print(f"\n{'config':>16s} {'PF te':>6s} {'anual te':>9s} {'trades':>7s} {'DD te':>6s}")
    print("-" * 50)
    pf_b = f"{baseline['pf']:.2f}" if baseline["pf"] != float("inf") else "inf"
    pf_s = f"{sharpe_alloc['pf']:.2f}" if sharpe_alloc["pf"] != float("inf") else "inf"
    print(f"{'4% igual (base)':>16s} {pf_b:>6s} {baseline['anual']:+8.2f}% "
          f"{baseline['n']:7d} {baseline['dd']:5.1f}%")
    print(f"{'ponderado Sharpe':>16s} {pf_s:>6s} {sharpe_alloc['anual']:+8.2f}% "
          f"{sharpe_alloc['n']:7d} {sharpe_alloc['dd']:5.1f}%")

    mejora = sharpe_alloc["anual"] - baseline["anual"]
    print(f"\nMejora: {mejora:+.2f}% anual  "
          f"({'↑ Sharpe allocation MEJOR' if mejora > 0 else '↓ asignación igual más robusta'})")
    print("\nAsignaciones recomendadas para config/configuraciones_optimas.json:")
    for s, r in sorted(asignaciones.items(), key=lambda x: x[1], reverse=True):
        sym_slash = s[:-3] + "/" + s[-3:]  # ETHEUR → ETH/EUR
        print(f"  {sym_slash}: riesgo_por_trade = {r:.2f}  (Sharpe={sharpes[s]:+.3f})")



def estudio_completo(symbols: list[str], days: int, capital0: float) -> None:
    """Backtest con la configuración completa actual del portfolio.

    Compara 3 escenarios en train (70%) y test (30%):
      baseline:     4% riesgo igual, sin filtro F&G
      solo_sharpe:  asignación Sharpe (ETH 8%, BTC/SOL/XRP 3%, AVAX 2%)
      completo:     Sharpe + Fear&Greed zona_neutral (25-75)

    Todos los escenarios incluyen vol_guard + BTC_macro (siempre activos).
    """
    # Asignación Sharpe validada por study_sharpe (2026-06)
    SHARPE_RIESGO = {
        "ETHEUR": 0.08, "BTCEUR": 0.03,
        "SOLEUR": 0.03, "XRPEUR": 0.03, "AVAXEUR": 0.02,
    }

    print("  descargando Fear&Greed histórico…")
    fg_map = _descargar_fear_greed(days)
    if not fg_map:
        print("  [aviso] sin datos F&G — se omite el filtro zona_neutral")

    print("  descargando datos de mercado…")
    datos: dict[str, list] = {}
    indicadores: dict[str, list] = {}
    for s in symbols:
        try:
            datos[s] = descargar_klines(s, "1d", days)
            indicadores[s] = calcular_indicadores(datos[s])
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
    if not datos:
        return

    btc_ind = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    def _fg_mask_zona_neutral(data: list) -> list[bool] | None:
        if not fg_map:
            return None
        mask = []
        for v in data:
            fg_val = _fg_valor(fg_map, v[0])
            ok = True
            if fg_val is not None:
                ok = 25 <= fg_val <= 75
            mask.append(ok)
        return mask

    # TP validados por study_tp_por_simbolo (2026-06)
    TP_OPT = {"ETHEUR": 3.5, "BTCEUR": 3.0, "SOLEUR": 2.5, "XRPEUR": 4.0, "AVAXEUR": 3.0}

    def _eval_config(riesgo_map: dict[str, float], usar_fg: bool,
                     tp_map: dict[str, float] | None = None) -> dict[str, dict]:
        resultado: dict[str, dict] = {}
        corte_ref = 0
        for fase in ("train", "test"):
            cap = gan = per = 0.0
            ntr = 0
            dd = 0.0
            for s, data in datos.items():
                n_s = len(data)
                corte = int(n_s * 0.7)
                corte_ref = corte
                a, b = (0, corte) if fase == "train" else (corte, n_s)
                fg_mask = _fg_mask_zona_neutral(data) if usar_fg else None
                tp = (tp_map.get(s, 3.0) if tp_map else 3.0)
                r = backtest(
                    data, s, capital0, 5.0,
                    use_trailing=False, trend_filter=False,
                    ind=indicadores[s], i0=a, i1=b,
                    sl_ratio=1.0, tp_ratio=tp, vol_guard=True,
                    riesgo=riesgo_map.get(s, 0.04), senal_v2=False,
                    btc_ind=btc_ind, be_atr=0.0, adx_min=0.0,
                    fg_mask=fg_mask,
                )
                cap += r.capital_final
                gan += r.bruto_ganado
                per += r.bruto_perdido
                ntr += r.trades
                dd = max(dd, r.max_drawdown)
            n_sym = len(datos)
            first_data = list(datos.values())[0] if datos else []
            dias = (corte_ref if fase == "train" else
                    len(first_data) - corte_ref) if first_data else 1
            anual = ((cap / (capital0 * n_sym)) ** (365 / dias) - 1) * 100 if n_sym and dias > 0 else 0
            pf = gan / per if per > 0 else float("inf")
            resultado[fase] = {"pf": pf, "anual": anual, "n": ntr, "dd": dd}
        return resultado

    escenarios = [
        ("baseline",     {s: 0.04 for s in datos},  False,         None),
        ("solo_sharpe",  SHARPE_RIESGO,              False,         None),
        ("completo",     SHARPE_RIESGO,              bool(fg_map),  None),
        ("completo+tp",  SHARPE_RIESGO,              bool(fg_map),  TP_OPT),
    ]

    print(f"\n{'escenario':>14s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'n tr':>5s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'n te':>5s} {'DD te':>6s}")
    print("-" * 78)

    for nombre, riesgo_map, usar_fg, tp_map in escenarios:
        res = _eval_config(riesgo_map, usar_fg, tp_map)
        tr, te = res["train"], res["test"]
        pf_tr = f"{tr['pf']:.2f}" if tr["pf"] != float("inf") else " inf"
        pf_te = f"{te['pf']:.2f}" if te["pf"] != float("inf") else " inf"
        print(f"{nombre:>14s} | "
              f"{pf_tr:>6s} {tr['anual']:+8.2f}% {tr['n']:5d} | "
              f"{pf_te:>6s} {te['anual']:+8.2f}% {te['n']:5d} {te['dd']:5.1f}%")

    n_sym = len(datos)
    n_test = int(len(list(datos.values())[0]) * 0.3) if datos else 0
    print(f"\nconfig: {n_sym} símbolos × 1d | "
          f"test ~{n_test} días | vol_guard+BTC_macro+Sharpe+FG_zona_neutral")
    print("asignación Sharpe: "
          + "  ".join(f"{s.replace('EUR','')}/{r*100:.0f}%" for s, r in SHARPE_RIESGO.items()))
    print("TP óptimos: "
          + "  ".join(f"{s.replace('EUR','')}/{v}" for s, v in TP_OPT.items()))


def estudio_funding_rate(symbols: list[str], days: int, capital0: float) -> None:
    """Evalúa el Funding Rate de Binance Futures como filtro de entradas.

    Cuando el funding rate es alto (>umbral), los longs están pagando una prima
    elevada → mercado sobrecalentado → alta probabilidad de reversión → bloquear.

    Compara 5 umbrales vs baseline (sin filtro FR), sobre config completa validada:
      sin_fr       — baseline actual (Sharpe+FG_zona_neutral+vol_guard+BTC_macro+TP_opt)
      fr_0.05%     — bloquear si FR > 0.0005 (0.05%)
      fr_0.10%     — bloquear si FR > 0.0010 (0.10%)
      fr_0.15%     — bloquear si FR > 0.0015 (0.15%)
      fr_0.20%     — bloquear si FR > 0.0020 (0.20%)
    """
    SHARPE_RIESGO = {
        "ETHEUR": 0.08, "BTCEUR": 0.03,
        "SOLEUR": 0.03, "XRPEUR": 0.03, "AVAXEUR": 0.02,
    }
    TP_OPT = {"ETHEUR": 3.5, "BTCEUR": 3.0, "SOLEUR": 2.5, "XRPEUR": 4.0, "AVAXEUR": 3.0}

    print("  descargando Fear&Greed histórico…")
    fg_map = _descargar_fear_greed(days)

    print("  descargando Funding Rate histórico por símbolo…")
    fr_maps: dict[str, dict[int, float]] = {}
    for s in symbols:
        fut_sym = FUTURES_SYMBOL_MAP.get(s)
        if fut_sym:
            fr_maps[s] = _descargar_funding_rate_historico(fut_sym, days)
        else:
            print(f"  [funding] sin mapping para {s}")

    print("  descargando datos de mercado…")
    datos: dict[str, list] = {}
    indicadores: dict[str, list] = {}
    for s in symbols:
        try:
            datos[s] = descargar_klines(s, "1d", days)
            indicadores[s] = calcular_indicadores(datos[s])
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
    if not datos:
        return

    btc_ind = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    def _mask_combinada(data: list, s: str, fr_umbral: float) -> list[bool]:
        fr_map = fr_maps.get(s, {})
        mask = []
        for v in data:
            ts_ms = v[0]
            fg_ok = True
            if fg_map:
                fg_val = _fg_valor(fg_map, ts_ms)
                if fg_val is not None:
                    fg_ok = 25 <= fg_val <= 75
            fr_ok = True
            if fr_umbral > 0 and fr_map:
                fr_val = _fr_valor(fr_map, ts_ms)
                if fr_val is not None:
                    fr_ok = fr_val <= fr_umbral
            mask.append(fg_ok and fr_ok)
        return mask

    def _eval(fr_umbral: float) -> dict[str, dict]:
        resultado: dict[str, dict] = {}
        corte_ref = 0
        for fase in ("train", "test"):
            cap = gan = per = 0.0
            ntr = 0
            dd = 0.0
            bloqueados = 0
            for s, data in datos.items():
                n_s = len(data)
                corte = int(n_s * 0.7)
                corte_ref = corte
                a, b = (0, corte) if fase == "train" else (corte, n_s)
                mask = _mask_combinada(data, s, fr_umbral)
                bloqueados += sum(1 for i in range(a, b) if not mask[i])
                r = backtest(
                    data, s, capital0, 5.0,
                    use_trailing=False, trend_filter=False,
                    ind=indicadores[s], i0=a, i1=b,
                    sl_ratio=1.0, tp_ratio=TP_OPT.get(s, 3.0), vol_guard=True,
                    riesgo=SHARPE_RIESGO.get(s, 0.03), senal_v2=False,
                    btc_ind=btc_ind, be_atr=0.0, adx_min=0.0,
                    fg_mask=mask,
                )
                cap += r.capital_final
                gan += r.bruto_ganado
                per += r.bruto_perdido
                ntr += r.trades
                dd = max(dd, r.max_drawdown)
            n_sym = len(datos)
            first_data = list(datos.values())[0] if datos else []
            dias = (corte_ref if fase == "train" else
                    len(first_data) - corte_ref) if first_data else 1
            anual = ((cap / (capital0 * n_sym)) ** (365 / dias) - 1) * 100 if n_sym and dias > 0 else 0
            pf = gan / per if per > 0 else float("inf")
            resultado[fase] = {"pf": pf, "anual": anual, "n": ntr, "dd": dd,
                               "bloqueados": bloqueados}
        return resultado

    escenarios = [
        ("sin_fr",    0.0),
        ("fr_0.05%",  0.0005),
        ("fr_0.10%",  0.0010),
        ("fr_0.15%",  0.0015),
        ("fr_0.20%",  0.0020),
    ]

    print(f"\n{'escenario':>11s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'n tr':>5s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'n te':>5s} {'DD te':>6s} {'bloq':>5s}")
    print("-" * 80)

    for nombre, fr_umbral in escenarios:
        res = _eval(fr_umbral)
        tr, te = res["train"], res["test"]
        pf_tr = f"{tr['pf']:.2f}" if tr["pf"] != float("inf") else " inf"
        pf_te = f"{te['pf']:.2f}" if te["pf"] != float("inf") else " inf"
        marker = " ← actual" if nombre == "sin_fr" else ""
        print(f"{nombre:>11s} | "
              f"{pf_tr:>6s} {tr['anual']:+8.2f}% {tr['n']:5d} | "
              f"{pf_te:>6s} {te['anual']:+8.2f}% {te['n']:5d} {te['dd']:5.1f}% "
              f"{te['bloqueados']:5d}{marker}")

    n_test = int(len(list(datos.values())[0]) * 0.3) if datos else 0
    print(f"\nconfig: {len(datos)} símbolos × 1d | test ~{n_test} días | "
          "Sharpe+FG_zona_neutral+vol_guard+BTC_macro+TP_opt")
    print("bloq = días de entrada bloqueados por FR en la ventana test")


def estudio_sl_por_simbolo(symbols: list[str], days: int, capital0: float) -> None:
    """Optimiza el SL ratio por símbolo de forma independiente (fuera de muestra).

    Para cada símbolo prueba SL en [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0]
    usando la config completa validada (Sharpe + F&G zona_neutral + vol_guard + BTC_macro
    + TP óptimo por símbolo).

    Fase 1 — ranking por símbolo: curva SL vs anual en TEST.
    Fase 2 — comparación agregada: SL uniforme 1.0 (actual) vs SL óptimo por símbolo.
    """
    SHARPE_RIESGO = {
        "ETHEUR": 0.08, "BTCEUR": 0.03,
        "SOLEUR": 0.03, "XRPEUR": 0.03, "AVAXEUR": 0.02,
    }
    TP_OPT = {"ETHEUR": 3.5, "BTCEUR": 3.0, "SOLEUR": 2.5, "XRPEUR": 4.0, "AVAXEUR": 3.0}
    SL_GRID = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0]

    print("  descargando Fear&Greed histórico…")
    fg_map = _descargar_fear_greed(days)

    print("  descargando datos de mercado…")
    datos: dict[str, list] = {}
    indicadores: dict[str, list] = {}
    for s in symbols:
        try:
            datos[s] = descargar_klines(s, "1d", days)
            indicadores[s] = calcular_indicadores(datos[s])
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
    if not datos:
        return

    btc_ind = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    def _fg_mask(data: list) -> list[bool] | None:
        if not fg_map:
            return None
        mask = []
        for v in data:
            fg_val = _fg_valor(fg_map, v[0])
            ok = True
            if fg_val is not None:
                ok = 25 <= fg_val <= 75
            mask.append(ok)
        return mask

    # ── Fase 1: tabla SL × símbolo en TEST ──────────────────────────────────
    print("\n── Fase 1: ranking SL por símbolo (test, fuera de muestra) ──")
    sl_hdr = "  ".join(f"{sl:.1f}" for sl in SL_GRID)
    print(f"\n{'symbol':>8s}  {'SL':>4s}  {sl_hdr}")
    print(f"{'':>8s}  {'':>4s}  " + "  ".join("-" * 4 for _ in SL_GRID))

    mejores: dict[str, float] = {}

    for s, data in datos.items():
        n_s = len(data)
        corte = int(n_s * 0.7)
        dias_test = n_s - corte
        fg_mask = _fg_mask(data)

        anuals_test: list[float] = []
        for sl in SL_GRID:
            r = backtest(
                data, s, capital0, 5.0,
                use_trailing=False, trend_filter=False,
                ind=indicadores[s], i0=corte, i1=n_s,
                sl_ratio=sl, tp_ratio=TP_OPT.get(s, 3.0), vol_guard=True,
                riesgo=SHARPE_RIESGO.get(s, 0.03), senal_v2=False,
                btc_ind=btc_ind, be_atr=0.0, adx_min=0.0,
                fg_mask=fg_mask,
            )
            anual = ((r.capital_final / capital0) ** (365 / dias_test) - 1) * 100 if dias_test > 0 else 0.0
            anuals_test.append(anual)

        mejor_idx = anuals_test.index(max(anuals_test))
        mejores[s] = SL_GRID[mejor_idx]

        valores = "  ".join(
            f"{'→' if i == mejor_idx else ' '}{anuals_test[i]:+5.1f}%"
            for i in range(len(SL_GRID))
        )
        print(f"{s:>8s}  anual  {valores}")

    # ── Fase 2: comparación agregada ────────────────────────────────────────
    print("\n── Fase 2: comparación agregada (train 70% / test 30%) ──")
    print(f"\n{'config':>14s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'n tr':>5s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'n te':>5s} {'DD te':>6s}")
    print("-" * 74)

    configs = [
        ("sl_uniforme_1",  {s: 1.0 for s in datos}),
        ("sl_por_simbolo", mejores),
    ]

    for nombre, sl_map in configs:
        for fase in ("train", "test"):
            cap = gan = per = 0.0
            ntr = 0
            dd = 0.0
            corte_ref = 0
            for s, data in datos.items():
                n_s = len(data)
                corte = int(n_s * 0.7)
                corte_ref = corte
                a, b = (0, corte) if fase == "train" else (corte, n_s)
                fg_mask = _fg_mask(data)
                r = backtest(
                    data, s, capital0, 5.0,
                    use_trailing=False, trend_filter=False,
                    ind=indicadores[s], i0=a, i1=b,
                    sl_ratio=sl_map.get(s, 1.0),
                    tp_ratio=TP_OPT.get(s, 3.0), vol_guard=True,
                    riesgo=SHARPE_RIESGO.get(s, 0.03), senal_v2=False,
                    btc_ind=btc_ind, be_atr=0.0, adx_min=0.0,
                    fg_mask=fg_mask,
                )
                cap += r.capital_final
                gan += r.bruto_ganado
                per += r.bruto_perdido
                ntr += r.trades
                dd = max(dd, r.max_drawdown)

            first_data = list(datos.values())[0] if datos else []
            dias = (corte_ref if fase == "train" else
                    len(first_data) - corte_ref) if first_data else 1
            n_sym = len(datos)
            anual = ((cap / (capital0 * n_sym)) ** (365 / dias) - 1) * 100 if n_sym and dias > 0 else 0
            pf = gan / per if per > 0 else float("inf")

            if fase == "train":
                tr = {"pf": pf, "anual": anual, "n": ntr, "dd": dd}
            else:
                te = {"pf": pf, "anual": anual, "n": ntr, "dd": dd}

        pf_tr = f"{tr['pf']:.2f}" if tr["pf"] != float("inf") else " inf"
        pf_te = f"{te['pf']:.2f}" if te["pf"] != float("inf") else " inf"
        print(f"{nombre:>14s} | "
              f"{pf_tr:>6s} {tr['anual']:+8.2f}% {tr['n']:5d} | "
              f"{pf_te:>6s} {te['anual']:+8.2f}% {te['n']:5d} {te['dd']:5.1f}%")

    print(f"\nSL óptimos (test): " + "  ".join(
        f"{s.replace('EUR','')}/{mejores[s]:.1f}" for s in datos))
    n_test = int(len(list(datos.values())[0]) * 0.3) if datos else 0
    print(f"config base: Sharpe+FG_zona_neutral+vol_guard+BTC_macro+TP_opt | test ~{n_test} días")


def estudio_trailing(symbols: list[str], days: int, capital0: float) -> None:
    """Evalúa trailing stop diferido: activa el trailing solo cuando el trade
    ya lleva X × ATR de ganancia, dejando al precio correr más allá del TP fijo.

    Escenarios comparados sobre config completa validada (Sharpe+FG+vol_guard+BTC):
      sin_trailing   — TP fijo por símbolo (config actual, baseline)
      trail_inm      — trailing inmediato sin TP (tp=20×, use_trailing=True)
      trail_1.5atr   — trailing activa cuando ganancia ≥ 1.5 × ATR
      trail_2.0atr   — trailing activa cuando ganancia ≥ 2.0 × ATR
      trail_2.5atr   — trailing activa cuando ganancia ≥ 2.5 × ATR
      trail_3.0atr   — trailing activa cuando ganancia ≥ 3.0 × ATR
    """
    SHARPE_RIESGO = {
        "ETHEUR": 0.08, "BTCEUR": 0.03,
        "SOLEUR": 0.03, "XRPEUR": 0.03, "AVAXEUR": 0.02,
    }
    # TP validados por study_tp_por_simbolo
    TP_OPT = {"ETHEUR": 3.5, "BTCEUR": 3.0, "SOLEUR": 2.5, "XRPEUR": 4.0, "AVAXEUR": 3.0}

    print("  descargando Fear&Greed histórico…")
    fg_map = _descargar_fear_greed(days)
    if not fg_map:
        print("  [aviso] sin datos F&G — se omite el filtro zona_neutral")

    print("  descargando datos de mercado…")
    datos: dict[str, list] = {}
    indicadores: dict[str, list] = {}
    for s in symbols:
        try:
            datos[s] = descargar_klines(s, "1d", days)
            indicadores[s] = calcular_indicadores(datos[s])
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
    if not datos:
        return

    btc_ind = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    def _fg_mask(data: list) -> list[bool] | None:
        if not fg_map:
            return None
        mask = []
        for v in data:
            fg_val = _fg_valor(fg_map, v[0])
            ok = True
            if fg_val is not None:
                ok = 25 <= fg_val <= 75
            mask.append(ok)
        return mask

    def _eval(use_trailing: bool, trail_act: float,
              tp_map: dict[str, float]) -> dict[str, dict]:
        resultado: dict[str, dict] = {}
        corte_ref = 0
        for fase in ("train", "test"):
            cap = gan = per = 0.0
            ntr = 0
            dd = 0.0
            for s, data in datos.items():
                n_s = len(data)
                corte = int(n_s * 0.7)
                corte_ref = corte
                a, b = (0, corte) if fase == "train" else (corte, n_s)
                fg_mask = _fg_mask(data)
                r = backtest(
                    data, s, capital0, 5.0,
                    use_trailing=use_trailing, trend_filter=False,
                    ind=indicadores[s], i0=a, i1=b,
                    sl_ratio=1.0, tp_ratio=tp_map.get(s, 3.0), vol_guard=True,
                    riesgo=SHARPE_RIESGO.get(s, 0.03), senal_v2=False,
                    btc_ind=btc_ind, be_atr=0.0, adx_min=0.0,
                    fg_mask=fg_mask,
                    trail_activacion_atr=trail_act,
                )
                cap += r.capital_final
                gan += r.bruto_ganado
                per += r.bruto_perdido
                ntr += r.trades
                dd = max(dd, r.max_drawdown)
            n_sym = len(datos)
            first_data = list(datos.values())[0] if datos else []
            dias = (corte_ref if fase == "train" else
                    len(first_data) - corte_ref) if first_data else 1
            anual = ((cap / (capital0 * n_sym)) ** (365 / dias) - 1) * 100 if n_sym and dias > 0 else 0
            pf = gan / per if per > 0 else float("inf")
            resultado[fase] = {"pf": pf, "anual": anual, "n": ntr, "dd": dd}
        return resultado

    # TP muy amplio para variantes con trailing (sin techo fijo)
    TP_LIBRE = {s: 20.0 for s in datos}

    escenarios = [
        ("sin_trailing",  False, 0.0,  TP_OPT),
        ("trail_inm",     True,  0.0,  TP_LIBRE),
        ("trail_1.5atr",  True,  1.5,  TP_LIBRE),
        ("trail_2.0atr",  True,  2.0,  TP_LIBRE),
        ("trail_2.5atr",  True,  2.5,  TP_LIBRE),
        ("trail_3.0atr",  True,  3.0,  TP_LIBRE),
    ]

    print(f"\n{'escenario':>14s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'n tr':>5s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'n te':>5s} {'DD te':>6s}")
    print("-" * 74)

    for nombre, use_tr, trail_act, tp_map in escenarios:
        res = _eval(use_tr, trail_act, tp_map)
        tr, te = res["train"], res["test"]
        pf_tr = f"{tr['pf']:.2f}" if tr["pf"] != float("inf") else " inf"
        pf_te = f"{te['pf']:.2f}" if te["pf"] != float("inf") else " inf"
        marker = " ← actual" if nombre == "sin_trailing" else ""
        print(f"{nombre:>14s} | "
              f"{pf_tr:>6s} {tr['anual']:+8.2f}% {tr['n']:5d} | "
              f"{pf_te:>6s} {te['anual']:+8.2f}% {te['n']:5d} {te['dd']:5.1f}%"
              f"{marker}")

    n_test = int(len(list(datos.values())[0]) * 0.3) if datos else 0
    print(f"\nconfig: {len(datos)} símbolos × 1d | test ~{n_test} días | "
          "Sharpe+FG_zona_neutral+vol_guard+BTC_macro")
    print("trailing: SL sigue max_precio − 1×ATR desde activación")


def estudio_senal_v2(symbols: list[str], days: int, capital0: float) -> None:
    """Compara señal v1 (RSI+MACD+volumen) vs v2 (+Donchian-20 breakout).

    Ambas versiones usan la config completa validada:
      Sharpe allocation + Fear&Greed zona_neutral (25-75) + vol_guard + BTC_macro

    Muestra resultados agregados train/test y desglose por símbolo en test.
    Donchian-20 breakout añade +3.0 al score cuando close > max(high, 20 velas).
    """
    SHARPE_RIESGO = {
        "ETHEUR": 0.08, "BTCEUR": 0.03,
        "SOLEUR": 0.03, "XRPEUR": 0.03, "AVAXEUR": 0.02,
    }

    print("  descargando Fear&Greed histórico…")
    fg_map = _descargar_fear_greed(days)
    if not fg_map:
        print("  [aviso] sin datos F&G — se omite el filtro zona_neutral")

    print("  descargando datos de mercado…")
    datos: dict[str, list] = {}
    indicadores: dict[str, list] = {}
    for s in symbols:
        try:
            datos[s] = descargar_klines(s, "1d", days)
            indicadores[s] = calcular_indicadores(datos[s])
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
    if not datos:
        return

    btc_ind = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    def _fg_mask(data: list) -> list[bool] | None:
        if not fg_map:
            return None
        mask = []
        for v in data:
            fg_val = _fg_valor(fg_map, v[0])
            ok = True
            if fg_val is not None:
                ok = 25 <= fg_val <= 75
            mask.append(ok)
        return mask

    def _eval_agregado(usar_v2: bool) -> dict[str, dict]:
        resultado: dict[str, dict] = {}
        corte_ref = 0
        for fase in ("train", "test"):
            cap = gan = per = 0.0
            ntr = 0
            dd = 0.0
            for s, data in datos.items():
                n_s = len(data)
                corte = int(n_s * 0.7)
                corte_ref = corte
                a, b = (0, corte) if fase == "train" else (corte, n_s)
                fg_mask = _fg_mask(data)
                r = backtest(
                    data, s, capital0, 5.0,
                    use_trailing=False, trend_filter=False,
                    ind=indicadores[s], i0=a, i1=b,
                    sl_ratio=1.0, tp_ratio=3.0, vol_guard=True,
                    riesgo=SHARPE_RIESGO.get(s, 0.03), senal_v2=usar_v2,
                    btc_ind=btc_ind, be_atr=0.0, adx_min=0.0,
                    fg_mask=fg_mask,
                )
                cap += r.capital_final
                gan += r.bruto_ganado
                per += r.bruto_perdido
                ntr += r.trades
                dd = max(dd, r.max_drawdown)
            n_sym = len(datos)
            first_data = list(datos.values())[0] if datos else []
            dias = (corte_ref if fase == "train" else
                    len(first_data) - corte_ref) if first_data else 1
            anual = ((cap / (capital0 * n_sym)) ** (365 / dias) - 1) * 100 if n_sym and dias > 0 else 0
            pf = gan / per if per > 0 else float("inf")
            resultado[fase] = {"pf": pf, "anual": anual, "n": ntr, "dd": dd}
        return resultado

    escenarios = [
        ("senal_v1", False),
        ("senal_v2", True),
    ]

    print(f"\n{'escenario':>10s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'n tr':>5s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'n te':>5s} {'DD te':>6s}")
    print("-" * 72)

    for nombre, usar_v2 in escenarios:
        res = _eval_agregado(usar_v2)
        tr, te = res["train"], res["test"]
        pf_tr = f"{tr['pf']:.2f}" if tr["pf"] != float("inf") else " inf"
        pf_te = f"{te['pf']:.2f}" if te["pf"] != float("inf") else " inf"
        print(f"{nombre:>10s} | "
              f"{pf_tr:>6s} {tr['anual']:+8.2f}% {tr['n']:5d} | "
              f"{pf_te:>6s} {te['anual']:+8.2f}% {te['n']:5d} {te['dd']:5.1f}%")

    # desglose por símbolo en test
    print("\n--- desglose por símbolo en TEST ---")
    print(f"{'symbol':>8s}  {'PF v1':>6s} {'anual v1':>9s} {'n v1':>5s}  "
          f"{'PF v2':>6s} {'anual v2':>9s} {'n v2':>5s}  {'delta':>7s}")
    print("-" * 76)
    for s, data in datos.items():
        n_s = len(data)
        corte = int(n_s * 0.7)
        dias = n_s - corte
        fg_mask = _fg_mask(data)
        r1 = backtest(
            data, s, capital0, 5.0,
            use_trailing=False, trend_filter=False,
            ind=indicadores[s], i0=corte, i1=n_s,
            sl_ratio=1.0, tp_ratio=3.0, vol_guard=True,
            riesgo=SHARPE_RIESGO.get(s, 0.03), senal_v2=False,
            btc_ind=btc_ind, be_atr=0.0, adx_min=0.0,
            fg_mask=fg_mask,
        )
        r2 = backtest(
            data, s, capital0, 5.0,
            use_trailing=False, trend_filter=False,
            ind=indicadores[s], i0=corte, i1=n_s,
            sl_ratio=1.0, tp_ratio=3.0, vol_guard=True,
            riesgo=SHARPE_RIESGO.get(s, 0.03), senal_v2=True,
            btc_ind=btc_ind, be_atr=0.0, adx_min=0.0,
            fg_mask=fg_mask,
        )
        anual1 = ((r1.capital_final / capital0) ** (365 / dias) - 1) * 100 if dias > 0 else 0.0
        anual2 = ((r2.capital_final / capital0) ** (365 / dias) - 1) * 100 if dias > 0 else 0.0
        pf1 = f"{r1.profit_factor:.2f}" if r1.profit_factor != float("inf") else " inf"
        pf2 = f"{r2.profit_factor:.2f}" if r2.profit_factor != float("inf") else " inf"
        delta = anual2 - anual1
        signo = "↑" if delta > 0.5 else ("↓" if delta < -0.5 else "≈")
        print(f"{s:>8s}  {pf1:>6s} {anual1:+8.2f}% {r1.trades:5d}  "
              f"{pf2:>6s} {anual2:+8.2f}% {r2.trades:5d}  {delta:+6.2f}% {signo}")

    n_sym = len(datos)
    n_test = int(len(list(datos.values())[0]) * 0.3) if datos else 0
    print(f"\nconfig: {n_sym} símbolos × 1d | test ~{n_test} días | "
          "Sharpe+FG_zona_neutral+vol_guard+BTC_macro")
    print("senal_v2: ruptura Donchian-20 (+3.0 score cuando close > max(high,20))")


def estudio_tp_por_simbolo(symbols: list[str], days: int, capital0: float) -> None:
    """Optimiza el TP ratio por símbolo de forma independiente (fuera de muestra).

    Para cada símbolo prueba TP en [2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0]
    usando la config completa validada (Sharpe + F&G zona_neutral + vol_guard + BTC_macro).

    Fase 1 — ranking por símbolo: muestra la curva TP vs (PF, anual) en TEST.
    Fase 2 — comparación agregada: TP uniforme 3.0 (actual) vs TP óptimo por símbolo.
    """
    SHARPE_RIESGO = {
        "ETHEUR": 0.08, "BTCEUR": 0.03,
        "SOLEUR": 0.03, "XRPEUR": 0.03, "AVAXEUR": 0.02,
    }
    TP_GRID = [2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0]

    print("  descargando Fear&Greed histórico…")
    fg_map = _descargar_fear_greed(days)
    if not fg_map:
        print("  [aviso] sin datos F&G — se omite el filtro zona_neutral")

    print("  descargando datos de mercado…")
    datos: dict[str, list] = {}
    indicadores: dict[str, list] = {}
    for s in symbols:
        try:
            datos[s] = descargar_klines(s, "1d", days)
            indicadores[s] = calcular_indicadores(datos[s])
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
    if not datos:
        return

    btc_ind = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    def _fg_mask(data: list) -> list[bool] | None:
        if not fg_map:
            return None
        mask = []
        for v in data:
            fg_val = _fg_valor(fg_map, v[0])
            ok = True
            if fg_val is not None:
                ok = 25 <= fg_val <= 75
            mask.append(ok)
        return mask

    # ── Fase 1: tabla TP × símbolo en TEST ──────────────────────────────────
    print("\n── Fase 1: ranking TP por símbolo (test, fuera de muestra) ──")

    # cabecera dinámica con los TPs
    tp_hdr = "  ".join(f"{tp:.1f}" for tp in TP_GRID)
    print(f"\n{'symbol':>8s}  {'TP':>4s}  {tp_hdr}")
    print(f"{'':>8s}  {'':>4s}  " + "  ".join("-" * 4 for _ in TP_GRID))

    mejores: dict[str, float] = {}  # symbol → TP óptimo (test anual)

    for s, data in datos.items():
        n_s = len(data)
        corte = int(n_s * 0.7)
        dias_test = n_s - corte
        fg_mask = _fg_mask(data)

        anuals_test: list[float] = []
        for tp in TP_GRID:
            r = backtest(
                data, s, capital0, 5.0,
                use_trailing=False, trend_filter=False,
                ind=indicadores[s], i0=corte, i1=n_s,
                sl_ratio=1.0, tp_ratio=tp, vol_guard=True,
                riesgo=SHARPE_RIESGO.get(s, 0.03), senal_v2=False,
                btc_ind=btc_ind, be_atr=0.0, adx_min=0.0,
                fg_mask=fg_mask,
            )
            anual = ((r.capital_final / capital0) ** (365 / dias_test) - 1) * 100 if dias_test > 0 else 0.0
            anuals_test.append(anual)

        mejor_idx = anuals_test.index(max(anuals_test))
        mejores[s] = TP_GRID[mejor_idx]

        valores = "  ".join(
            f"{'→' if i == mejor_idx else ' '}{anuals_test[i]:+5.1f}%"
            for i in range(len(TP_GRID))
        )
        print(f"{s:>8s}  anual  {valores}")

    # ── Fase 2: comparación agregada ────────────────────────────────────────
    print("\n── Fase 2: comparación agregada (train 70% / test 30%) ──")
    print(f"\n{'config':>14s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'n tr':>5s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'n te':>5s} {'DD te':>6s}")
    print("-" * 74)

    configs = [
        ("tp_uniforme_3",  {s: 3.0 for s in datos}),
        ("tp_por_simbolo", mejores),
    ]

    for nombre, tp_map in configs:
        for fase in ("train", "test"):
            cap = gan = per = 0.0
            ntr = 0
            dd = 0.0
            corte_ref = 0
            for s, data in datos.items():
                n_s = len(data)
                corte = int(n_s * 0.7)
                corte_ref = corte
                a, b = (0, corte) if fase == "train" else (corte, n_s)
                fg_mask = _fg_mask(data)
                r = backtest(
                    data, s, capital0, 5.0,
                    use_trailing=False, trend_filter=False,
                    ind=indicadores[s], i0=a, i1=b,
                    sl_ratio=1.0, tp_ratio=tp_map.get(s, 3.0), vol_guard=True,
                    riesgo=SHARPE_RIESGO.get(s, 0.03), senal_v2=False,
                    btc_ind=btc_ind, be_atr=0.0, adx_min=0.0,
                    fg_mask=fg_mask,
                )
                cap += r.capital_final
                gan += r.bruto_ganado
                per += r.bruto_perdido
                ntr += r.trades
                dd = max(dd, r.max_drawdown)

            first_data = list(datos.values())[0] if datos else []
            dias = (corte_ref if fase == "train" else len(first_data) - corte_ref) if first_data else 1
            n_sym = len(datos)
            anual = ((cap / (capital0 * n_sym)) ** (365 / dias) - 1) * 100 if n_sym and dias > 0 else 0
            pf = gan / per if per > 0 else float("inf")

            if fase == "train":
                tr = {"pf": pf, "anual": anual, "n": ntr, "dd": dd}
            else:
                te = {"pf": pf, "anual": anual, "n": ntr, "dd": dd}

        pf_tr = f"{tr['pf']:.2f}" if tr["pf"] != float("inf") else " inf"
        pf_te = f"{te['pf']:.2f}" if te["pf"] != float("inf") else " inf"
        print(f"{nombre:>14s} | "
              f"{pf_tr:>6s} {tr['anual']:+8.2f}% {tr['n']:5d} | "
              f"{pf_te:>6s} {te['anual']:+8.2f}% {te['n']:5d} {te['dd']:5.1f}%")

    print(f"\nTP óptimos (test): " + "  ".join(
        f"{s.replace('EUR','')}/{mejores[s]:.1f}" for s in datos))
    n_test = int(len(list(datos.values())[0]) * 0.3) if datos else 0
    print(f"config base: Sharpe+FG_zona_neutral+vol_guard+BTC_macro | test ~{n_test} días")


def estudio_equity_filter(symbols: list[str], days: int, capital0: float) -> None:
    """Evalúa el Equity Curve Filter: reducir riesgo o pausar entradas cuando
    el capital está en drawdown respecto a su máximo histórico.

    Compara 5 configuraciones vs baseline sin filtro:
      sin_filtro:    todas las entradas con riesgo 4% (baseline)
      reducir_10:    reducir riesgo a 2% cuando DD > 10%
      reducir_15:    reducir riesgo a 2% cuando DD > 15%
      pausar_15:     pausar entradas cuando DD > 15%
      pausar_20:     pausar entradas cuando DD > 20%

    El objetivo: el filtro reduce el DD máximo sin sacrificar demasiado
    retorno. Un buen filtro sube el Sharpe aunque baje el CAGR bruto.
    """
    print("  descargando datos…")
    datos, indicadores = {}, {}
    for s in symbols:
        try:
            datos[s] = descargar_klines(s, "1d", days)
            indicadores[s] = calcular_indicadores(datos[s])
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
    if not datos:
        return
    btc_ind = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))
    n_sym = len(datos)

    variantes = [
        ("sin_filtro",  0.0,  0.0),
        ("reducir_10%", 0.0,  0.10),
        ("reducir_15%", 0.0,  0.15),
        ("pausar_15%",  0.15, 0.0),
        ("pausar_20%",  0.20, 0.0),
    ]

    print(f"\n{'variante':>14s} | "
          f"{'PF tr':>6s} {'anual tr':>9s} {'DD tr':>6s} | "
          f"{'PF te':>6s} {'anual te':>9s} {'DD te':>6s}  resultado")
    print("-" * 82)

    for nombre, dd_pausa, dd_red in variantes:
        agg = {}
        for fase in ("train", "test"):
            cap = gan = per = 0.0
            ntr = 0
            dd = 0.0
            for s, data in datos.items():
                n_s = len(data)
                corte = int(n_s * 0.7)
                a, b = (0, corte) if fase == "train" else (corte, n_s)
                r = backtest(
                    data, s, capital0, 5.0,
                    use_trailing=False, trend_filter=False,
                    ind=indicadores[s], i0=a, i1=b,
                    sl_ratio=1.0, tp_ratio=3.0, vol_guard=True,
                    riesgo=0.04, senal_v2=False, btc_ind=btc_ind,
                    eq_dd_pausa=dd_pausa, eq_dd_reduccion=dd_red,
                )
                cap += r.capital_final
                gan += r.bruto_ganado
                per += r.bruto_perdido
                ntr += r.trades
                dd = max(dd, r.max_drawdown)
            dias_f = (corte if fase == "train" else (len(list(datos.values())[0]) - corte))
            anual = ((cap / (capital0 * n_sym)) ** (365 / dias_f) - 1) * 100 if dias_f > 0 else 0
            pf = gan / per if per > 0 else float("inf")
            agg[fase] = {"pf": pf, "anual": anual, "dd": dd, "n": ntr}

        tr, te = agg["train"], agg["test"]
        pf_tr = f"{tr['pf']:.2f}" if tr["pf"] != float("inf") else "inf"
        pf_te = f"{te['pf']:.2f}" if te["pf"] != float("inf") else "inf"
        mejora = "✓ DD↓" if te["dd"] < agg["test"]["dd"] else ""
        print(f"{nombre:>14s} | "
              f"{pf_tr:>6s} {tr['anual']:+8.2f}% {tr['dd']:5.1f}% | "
              f"{pf_te:>6s} {te['anual']:+8.2f}% {te['dd']:5.1f}%  {mejora}")

    print(f"\nconfig base: 1d umbral=5 SL=1×ATR TP=3×ATR riesgo=4% vol_guard+BTC_macro")


def estudio_walk_forward(symbols: list[str], days: int, capital0: float,
                         n_folds: int = 5) -> None:
    """Walk-Forward Validation: divide el histórico en N ventanas y evalúa
    la estrategia en cada período independientemente.

    A diferencia del split estático 70/30, esto responde a:
    '¿Es la estrategia consistente en TODOS los períodos de mercado o
    solo funciona en algunos?'

    Config fija (la óptima validada): 1d umbral=5 SL=1×ATR TP=3×ATR
    riesgo=4% vol_guard ON BTC_macro ON.

    Cada fold es un segmento consecutivo del histórico total.
    Se reporta el PF, retorno anual y DD de cada ventana temporal.
    Al final se muestra consistencia: cuántas ventanas son positivas.
    """
    print("  descargando datos…")
    datos, indicadores = {}, {}
    for s in symbols:
        try:
            datos[s] = descargar_klines(s, "1d", days)
            indicadores[s] = calcular_indicadores(datos[s])
        except Exception as e:
            print(f"  [ERROR] {s}: {e}")
    if not datos:
        print("  [walk-forward] sin datos")
        return

    btc_ind = indicadores.get("BTCEUR") or calcular_indicadores(
        descargar_klines("BTCEUR", "1d", days))

    # longitud mínima entre todos los símbolos
    n_min = min(len(v) for v in datos.values())
    tam_fold = n_min // n_folds

    print(f"\n  {n_min} velas × {len(datos)} símbolos → {n_folds} folds "
          f"de ~{tam_fold} días cada uno")

    print(f"\n{'fold':>5s} {'periodo':>22s} | "
          f"{'PF':>6s} {'anual':>9s} {'trades':>7s} {'DD':>6s}  resultado")
    print("-" * 75)

    fold_positivos = 0
    for k in range(n_folds):
        a = k * tam_fold
        b = (k + 1) * tam_fold if k < n_folds - 1 else n_min

        # fecha aproximada del período (usando timestamps del primer símbolo)
        primer_sym = next(iter(datos))
        ts_a = datos[primer_sym][a][0] / 1000
        ts_b = datos[primer_sym][min(b - 1, len(datos[primer_sym]) - 1)][0] / 1000
        import datetime as _dt
        fecha_a = _dt.datetime.utcfromtimestamp(ts_a).strftime("%Y-%m")
        fecha_b = _dt.datetime.utcfromtimestamp(ts_b).strftime("%Y-%m")

        cap = gan = per = 0.0
        ntr = 0
        dd = 0.0
        for s, data in datos.items():
            n_s = len(data)
            b_s = min(b, n_s)
            r = backtest(
                data, s, capital0, 5.0,
                use_trailing=False, trend_filter=False,
                ind=indicadores[s], i0=a, i1=b_s,
                sl_ratio=1.0, tp_ratio=3.0, vol_guard=True,
                riesgo=0.04, senal_v2=False,
                btc_ind=btc_ind,
                be_atr=0.0, adx_min=0.0,
            )
            cap += r.capital_final
            gan += r.bruto_ganado
            per += r.bruto_perdido
            ntr += r.trades
            dd = max(dd, r.max_drawdown)

        n_sym = len(datos)
        dias_fold = b - a
        ret_total = (cap / (capital0 * n_sym) - 1) * 100
        anual = ((cap / (capital0 * n_sym)) ** (365 / dias_fold) - 1) * 100
        pf = gan / per if per > 0 else float("inf")
        pf_str = f"{pf:.2f}" if pf != float("inf") else "inf"
        positivo = anual > 0 and pf > 1.0
        if positivo:
            fold_positivos += 1
        marca = "✓" if positivo else "✗"
        print(f"{k+1:>5d} {fecha_a}→{fecha_b:>7s} | "
              f"{pf_str:>6s} {anual:+8.2f}% {ntr:7d} {dd:5.1f}%  {marca}")

    print(f"\nConsistencia: {fold_positivos}/{n_folds} períodos positivos "
          f"({'ROBUSTO' if fold_positivos >= n_folds * 0.8 else 'INESTABLE — revisa la estrategia'})")
    print("config: 1d umbral=5 SL=1×ATR TP=3×ATR riesgo=4% vol_guard+BTC_macro")


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
    p.add_argument("--study_eth_riesgo", action="store_true",
                   help="grid de riesgo por trade para ETH: busca sweet-spot retorno/DD")
    p.add_argument("--study_fear_greed", action="store_true",
                   help="Fear & Greed Index como filtro de entradas (alternative.me)")
    p.add_argument("--study_wf", action="store_true",
                   help="walk-forward validation: N folds temporales, mide consistencia")
    p.add_argument("--wf_folds", type=int, default=5,
                   help="número de folds para walk-forward (default 5)")
    p.add_argument("--study_sharpe", action="store_true",
                   help="asignación de riesgo ponderada por Sharpe ratio por símbolo")
    p.add_argument("--study_equity_filter", action="store_true",
                   help="equity curve filter: reducir/pausar entradas en drawdown")
    p.add_argument("--study_completo", action="store_true",
                   help="rentabilidad actual: baseline vs Sharpe vs Sharpe+FG zona_neutral")
    p.add_argument("--study_senal_v2", action="store_true",
                   help="señal v1 vs v2 (Donchian-20 breakout) sobre config completa validada")
    p.add_argument("--study_tp_por_simbolo", action="store_true",
                   help="optimiza TP ratio por símbolo: grid 2.0-6.0 en train/test")
    p.add_argument("--study_sl_por_simbolo", action="store_true",
                   help="optimiza SL ratio por símbolo: grid 0.5-2.0 en train/test")
    p.add_argument("--study_trailing", action="store_true",
                   help="trailing stop diferido: compara activación 1.5-3.0×ATR vs TP fijo")
    p.add_argument("--study_funding_rate", action="store_true",
                   help="funding rate Binance Futures como filtro de entradas: grid 0.05-0.20%%")
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
        all_syms = args.symbol or (["BTCEUR", "ETHEUR", "SOLEUR", "XRPEUR", "AVAXEUR"] + CANDIDATOS)
        estudio_simbolos(all_syms, args.days, args.capital)
        print(f"\n[tiempo] estudio simbolos: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_eth_riesgo:
        t0 = time.perf_counter()
        estudio_eth_riesgo(args.days, args.capital)
        print(f"\n[tiempo] estudio ETH riesgo: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_fear_greed:
        t0 = time.perf_counter()
        estudio_fear_greed(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio fear&greed: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_wf:
        t0 = time.perf_counter()
        estudio_walk_forward(symbols, args.days, args.capital, n_folds=args.wf_folds)
        print(f"\n[tiempo] walk-forward: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_sharpe:
        t0 = time.perf_counter()
        estudio_sharpe_allocation(symbols, args.days, args.capital)
        print(f"\n[tiempo] sharpe allocation: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_equity_filter:
        t0 = time.perf_counter()
        estudio_equity_filter(symbols, args.days, args.capital)
        print(f"\n[tiempo] equity filter: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_completo:
        t0 = time.perf_counter()
        estudio_completo(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio completo: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_senal_v2:
        t0 = time.perf_counter()
        estudio_senal_v2(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio senal_v2: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_tp_por_simbolo:
        t0 = time.perf_counter()
        estudio_tp_por_simbolo(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio tp_por_simbolo: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_sl_por_simbolo:
        t0 = time.perf_counter()
        estudio_sl_por_simbolo(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio sl_por_simbolo: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_trailing:
        t0 = time.perf_counter()
        estudio_trailing(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio trailing: {time.perf_counter() - t0:.1f}s")
        return

    if args.study_funding_rate:
        t0 = time.perf_counter()
        estudio_funding_rate(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio funding rate: {time.perf_counter() - t0:.1f}s")
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
