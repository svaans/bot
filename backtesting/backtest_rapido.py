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
import time
import urllib.request
from dataclasses import dataclass, field

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
BINANCE = "https://api.binance.com/api/v3/klines"
MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}

# Parámetros alineados con config/configuraciones_optimas.json
SL_RATIO = 1.5          # sl_ratio: stop loss a 1.5 * ATR
TP_RATIO = 3.0          # tp_ratio: take profit a 3.0 * ATR
RIESGO_POR_TRADE = 0.02 # riesgo_por_trade
COOLDOWN_TRAS_PERDIDA = 3
FEE = 0.001             # comisión spot Binance por lado (0.1%)
SLIPPAGE = 0.0005       # deslizamiento estimado por lado


# ---------------------------------------------------------------- datos

def descargar_klines(symbol: str, interval: str, days: int) -> list[list[float]]:
    """Descarga velas (con caché en CSV). Devuelve [ts, o, h, l, c, v]."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    ruta = os.path.join(CACHE_DIR, f"{symbol}_{interval}_{days}d.csv")
    if os.path.exists(ruta):
        with open(ruta, newline="") as f:
            return [[float(x) for x in row] for row in csv.reader(f)]

    fin = int(time.time() * 1000)
    inicio = fin - days * 86_400_000
    velas: list[list[float]] = []
    cursor = inicio
    while cursor < fin:
        url = (f"{BINANCE}?symbol={symbol}&interval={interval}"
               f"&startTime={cursor}&limit=1000")
        with urllib.request.urlopen(url, timeout=30) as r:
            lote = json.load(r)
        if not lote:
            break
        for k in lote:
            velas.append([float(k[0]), float(k[1]), float(k[2]),
                          float(k[3]), float(k[4]), float(k[5])])
        cursor = int(lote[-1][0]) + MS[interval]
        time.sleep(0.15)  # respetar rate limit
    with open(ruta, "w", newline="") as f:
        csv.writer(f).writerows(velas)
    return velas


# ------------------------------------------------------------ indicadores
# Todos en una sola pasada O(n), listas pre-asignadas: es lo que lo hace rápido.

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

    return {"close": close, "ema9": ema9, "ema21": ema21, "ema200": ema200,
            "rsi": rsi, "macd": macd, "macd_signal": macd_signal, "atr": atr,
            "vol": vol, "vol_ma": vol_ma}


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
             i0: int = 0, i1: int | None = None) -> Resultado:
    if ind is None:
        ind = calcular_indicadores(velas)
    close = ind["close"]
    n = i1 if i1 is not None else len(velas)
    res = Resultado(symbol=symbol, capital_final=capital0)
    capital = capital0
    pico = capital
    en_pos = False
    qty = entrada = sl = tp = maximo = 0.0
    cooldown = 0

    for i in range(max(30, i0), n - 1):
        if cooldown > 0:
            cooldown -= 1

        if en_pos:
            h, l = velas[i][2], velas[i][3]
            maximo = max(maximo, h)
            if use_trailing:
                # trailing por ATR sobre el máximo alcanzado
                sl = max(sl, maximo - SL_RATIO * ind["atr"][i])
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
        if cooldown == 0 and score_entrada(ind, i) >= umbral and not math.isnan(ind["atr"][i]):
            precio = close[i] * (1 + FEE + SLIPPAGE)
            riesgo_unitario = SL_RATIO * ind["atr"][i]
            qty = (capital * RIESGO_POR_TRADE) / riesgo_unitario
            qty = min(qty, capital / precio)  # sin apalancamiento
            if qty * precio < 10:  # mínimo de orden ~10 EUR
                continue
            entrada = precio
            sl = close[i] - SL_RATIO * ind["atr"][i]
            tp = close[i] + TP_RATIO * ind["atr"][i]
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
    args = p.parse_args()

    symbols = args.symbol or ["BTCEUR", "ETHEUR", "SOLEUR", "ADAEUR", "BNBEUR"]

    if args.study:
        t0 = time.perf_counter()
        estudio_timeframes(symbols, args.days, args.capital)
        print(f"\n[tiempo] estudio completo: {time.perf_counter() - t0:.1f}s")
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
    main()
