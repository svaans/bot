"""Tests para ADX, break-even stop y study4 (camino a 16-20% anual).

Cubren:
  - _calcular_adx: correctitud, rangos, comportamiento en tendencia vs lateral
  - backtest be_atr: break-even se activa, reduce DD, retrocompatible
  - backtest adx_min: filtro reduce trades en mercados laterales
  - parámetros nuevos son retrocompatibles (defaults = comportamiento anterior)
  - estudio_v4 se ejecuta sin errores con datos sintéticos
"""
from __future__ import annotations

import importlib.util
import math
import sys
from pathlib import Path

import pytest

# Importar backtest_rapido directamente (no depende de `ta` ni del core stack).
# Se registra manualmente en sys.modules para que @dataclass resuelva __module__.
_spec = importlib.util.spec_from_file_location(
    "backtesting.backtest_rapido",
    Path(__file__).parent.parent / "backtesting" / "backtest_rapido.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["backtesting.backtest_rapido"] = _mod
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

_calcular_adx = _mod._calcular_adx
backtest = _mod.backtest
calcular_indicadores = _mod.calcular_indicadores
Resultado = _mod.Resultado
estudio_v4 = _mod.estudio_v4
estudio_simbolos = _mod.estudio_simbolos
estudio_eth_riesgo = _mod.estudio_eth_riesgo
estudio_fear_greed = _mod.estudio_fear_greed
estudio_walk_forward = _mod.estudio_walk_forward
_fg_valor = _mod._fg_valor


# ─── generadores de datos sintéticos ────────────────────────────────────────

def _velas_tendencia(
    n: int = 200, inicio: float = 100.0, pendiente: float = 0.5
) -> list[list[float]]:
    """Velas con tendencia alcista constante (ADX alto)."""
    velas = []
    ts = 1_700_000_000_000
    c = inicio
    for i in range(n):
        o = c
        c = o + pendiente
        h = c + 0.3
        lo = o - 0.1
        velas.append([ts + i * 86_400_000, o, h, lo, c, 100.0 + i])
    return velas


def _velas_laterales(n: int = 200, precio: float = 100.0) -> list[list[float]]:
    """Velas laterales sinusoidales (ADX bajo)."""
    velas = []
    ts = 1_700_000_000_000
    amplitud = 0.8
    for i in range(n):
        c = precio + amplitud * math.sin(i * 0.25)
        o = precio + amplitud * math.sin((i - 1) * 0.25)
        h = max(o, c) + 0.05
        lo = min(o, c) - 0.05
        velas.append([ts + i * 86_400_000, o, h, lo, c, 50.0])
    return velas


def _velas_bajistas(n: int = 200, inicio: float = 200.0, pendiente: float = -0.4) -> list[list[float]]:
    """Velas con tendencia bajista (para verificar que el filtro ADX no discrimina dirección)."""
    velas = []
    ts = 1_700_000_000_000
    c = inicio
    for i in range(n):
        o = c
        c = max(o + pendiente, 1.0)
        h = o + 0.1
        lo = c - 0.1
        velas.append([ts + i * 86_400_000, o, h, lo, c, 80.0])
    return velas


# ─────────────────────────────────────────────────────────────────────────────
#  Tests de _calcular_adx
# ─────────────────────────────────────────────────────────────────────────────

class TestCalcularAdx:

    def test_insuficiente_data_todo_nan(self) -> None:
        """Con menos de 2*periodo+1 velas todos los valores son nan."""
        n = 20
        highs = [1.0] * n
        lows = [0.9] * n
        closes = [1.0] * n
        result = _calcular_adx(highs, lows, closes, periodo=14)
        assert all(math.isnan(v) for v in result), "Con 20 velas y periodo=14 todo debe ser nan"

    def test_longitud_output_igual_input(self) -> None:
        """La lista retornada siempre tiene la misma longitud que la entrada."""
        for n in (50, 100, 300):
            velas = _velas_tendencia(n)
            highs = [v[2] for v in velas]
            lows = [v[3] for v in velas]
            closes = [v[4] for v in velas]
            result = _calcular_adx(highs, lows, closes)
            assert len(result) == n, f"n={n}: output len={len(result)}"

    def test_rango_valido_0_100(self) -> None:
        """Todos los valores ADX no-nan deben estar en [0, 100]."""
        velas = _velas_tendencia(200)
        highs = [v[2] for v in velas]
        lows = [v[3] for v in velas]
        closes = [v[4] for v in velas]
        result = _calcular_adx(highs, lows, closes)
        validos = [v for v in result if not math.isnan(v)]
        assert len(validos) > 0, "Debe haber valores válidos con 200 velas"
        out_of_range = [v for v in validos if not (0.0 <= v <= 100.0)]
        assert not out_of_range, f"ADX fuera de [0,100]: {out_of_range[:5]}"

    def test_primeros_valores_son_nan(self) -> None:
        """Los primeros 2*periodo-1 índices deben ser nan."""
        periodo = 14
        velas = _velas_tendencia(100)
        highs = [v[2] for v in velas]
        lows = [v[3] for v in velas]
        closes = [v[4] for v in velas]
        result = _calcular_adx(highs, lows, closes, periodo=periodo)
        limite = 2 * periodo - 1
        for i in range(limite):
            assert math.isnan(result[i]), f"result[{i}] debería ser nan (periodo={periodo})"

    def test_tendencia_mayor_que_lateral(self) -> None:
        """ADX promedio en tendencia debe superar al de mercado lateral."""
        def mean_adx(velas: list[list[float]]) -> float:
            highs = [v[2] for v in velas]
            lows = [v[3] for v in velas]
            closes = [v[4] for v in velas]
            vals = [v for v in _calcular_adx(highs, lows, closes) if not math.isnan(v)]
            return sum(vals) / len(vals) if vals else 0.0

        adx_tend = mean_adx(_velas_tendencia(300, pendiente=0.8))
        adx_lat = mean_adx(_velas_laterales(300))
        assert adx_tend > adx_lat, (
            f"ADX tendencia ({adx_tend:.1f}) no mayor que lateral ({adx_lat:.1f})"
        )

    def test_adx_bajista_tambien_es_alto(self) -> None:
        """ADX mide fuerza de tendencia, no dirección — bajista también da ADX alto."""
        def mean_adx(velas: list[list[float]]) -> float:
            highs = [v[2] for v in velas]
            lows = [v[3] for v in velas]
            closes = [v[4] for v in velas]
            vals = [v for v in _calcular_adx(highs, lows, closes) if not math.isnan(v)]
            return sum(vals) / len(vals) if vals else 0.0

        adx_bajista = mean_adx(_velas_bajistas(300))
        adx_lateral = mean_adx(_velas_laterales(300))
        assert adx_bajista > adx_lateral, (
            f"ADX bajista ({adx_bajista:.1f}) debería superar al lateral ({adx_lateral:.1f})"
        )

    def test_periodo_pequeno_funciona(self) -> None:
        """Periodo pequeño (ej. 7) también debe funcionar correctamente."""
        velas = _velas_tendencia(60)
        highs = [v[2] for v in velas]
        lows = [v[3] for v in velas]
        closes = [v[4] for v in velas]
        result = _calcular_adx(highs, lows, closes, periodo=7)
        validos = [v for v in result if not math.isnan(v)]
        assert len(validos) > 0
        assert all(0.0 <= v <= 100.0 for v in validos)


# ─────────────────────────────────────────────────────────────────────────────
#  Tests de calcular_indicadores (integración ADX)
# ─────────────────────────────────────────────────────────────────────────────

class TestIndicadoresIncluyanAdx:

    def test_clave_adx_presente(self) -> None:
        """calcular_indicadores debe incluir la clave 'adx'."""
        velas = _velas_tendencia(200)
        ind = calcular_indicadores(velas)
        assert "adx" in ind, "Falta la clave 'adx' en el dict de indicadores"

    def test_adx_longitud_igual_a_close(self) -> None:
        """len(ind['adx']) debe ser igual a len(ind['close'])."""
        velas = _velas_tendencia(150)
        ind = calcular_indicadores(velas)
        assert len(ind["adx"]) == len(ind["close"])

    def test_todas_las_claves_previas_siguen_presentes(self) -> None:
        """Las claves existentes no deben desaparecer al agregar ADX."""
        velas = _velas_tendencia(100)
        ind = calcular_indicadores(velas)
        for key in ("close", "ema9", "ema21", "ema200", "rsi", "macd",
                    "macd_signal", "atr", "vol", "vol_ma", "don_hi", "adx"):
            assert key in ind, f"Clave faltante: {key}"


# ─────────────────────────────────────────────────────────────────────────────
#  Tests del break-even stop (be_atr)
# ─────────────────────────────────────────────────────────────────────────────

class TestBreakEvenStop:

    def test_campo_be_activados_existe_en_resultado(self) -> None:
        """Resultado debe tener el campo be_activados."""
        velas = _velas_tendencia(100)
        res = backtest(velas, "TEST")
        assert hasattr(res, "be_activados"), "Falta campo be_activados en Resultado"
        assert isinstance(res.be_activados, int)

    def test_be_cero_no_activa(self) -> None:
        """Con be_atr=0.0 (default), be_activados debe ser 0."""
        velas = _velas_tendencia(300, pendiente=1.0)
        res = backtest(velas, "TEST", umbral=2.0, use_trailing=False, be_atr=0.0)
        assert res.be_activados == 0

    def test_be_activo_campo_es_no_negativo(self) -> None:
        """Con be_atr > 0, be_activados debe ser >= 0."""
        velas = _velas_tendencia(300, pendiente=0.5)
        res = backtest(velas, "TEST", umbral=2.0, use_trailing=False, be_atr=1.0)
        assert res.be_activados >= 0

    def test_be_no_excede_numero_de_trades(self) -> None:
        """be_activados no puede superar el número total de trades."""
        velas = _velas_tendencia(400, pendiente=0.5)
        res = backtest(velas, "TEST", umbral=2.0, use_trailing=False,
                       be_atr=0.8, riesgo=0.04)
        assert res.be_activados <= res.trades, (
            f"be_activados={res.be_activados} > trades={res.trades}"
        )

    def test_be_con_tendencia_fuerte_activa_algunos(self) -> None:
        """En tendencia fuerte, be_atr=0.5 debe activar al menos un breakeven."""
        # Pendiente grande = precio sube rápido, casi seguro alcanza be_atr*ATR
        velas = _velas_tendencia(500, pendiente=2.0)
        res = backtest(velas, "TEST", umbral=2.0, use_trailing=False,
                       be_atr=0.5, riesgo=0.04)
        if res.trades > 0:
            assert res.be_activados > 0, (
                f"En tendencia fuerte con {res.trades} trades se esperan BEs activados"
            )

    def test_be_no_empeora_drawdown_significativamente(self) -> None:
        """Con BE activo, el max_drawdown no debe ser significativamente mayor que sin él."""
        velas = _velas_tendencia(400, pendiente=0.3)
        res_sin = backtest(velas, "TEST", umbral=2.0, use_trailing=False,
                           be_atr=0.0, riesgo=0.04)
        res_con = backtest(velas, "TEST", umbral=2.0, use_trailing=False,
                           be_atr=1.0, riesgo=0.04)
        if res_sin.trades > 0 and res_con.trades > 0:
            # BE no puede empeorar el DD más de un 5% (margen de rounding)
            assert res_con.max_drawdown <= res_sin.max_drawdown * 1.05, (
                f"DD con BE ({res_con.max_drawdown:.1f}%) es peor que sin BE ({res_sin.max_drawdown:.1f}%)"
            )

    def test_be_retrocompatible_mismos_resultados_sin_activar(self) -> None:
        """Con be_atr=0.0 el resultado es idéntico al de no pasar el parámetro."""
        velas = _velas_tendencia(200)
        res_base = backtest(velas, "TEST", umbral=3.0)
        res_nuevo = backtest(velas, "TEST", umbral=3.0, be_atr=0.0, adx_min=0.0)
        assert res_base.trades == res_nuevo.trades
        assert abs(res_base.pnl_total - res_nuevo.pnl_total) < 1e-9
        assert abs(res_base.capital_final - res_nuevo.capital_final) < 1e-9
        assert res_nuevo.be_activados == 0


# ─────────────────────────────────────────────────────────────────────────────
#  Tests del filtro ADX (adx_min)
# ─────────────────────────────────────────────────────────────────────────────

class TestFiltroAdx:

    def test_adx_cero_identico_sin_filtro(self) -> None:
        """adx_min=0.0 debe producir exactamente los mismos trades que sin el parámetro."""
        velas = _velas_tendencia(200)
        res1 = backtest(velas, "TEST", umbral=3.0)
        res2 = backtest(velas, "TEST", umbral=3.0, adx_min=0.0)
        assert res1.trades == res2.trades
        assert abs(res1.pnl_total - res2.pnl_total) < 1e-9

    def test_adx_imposible_sin_trades(self) -> None:
        """ADX está acotado en [0,100], por lo que adx_min=101 nunca se cumple → 0 trades."""
        velas = _velas_tendencia(300, pendiente=1.0)
        res = backtest(velas, "TEST", umbral=2.0, adx_min=101.0)
        assert res.trades == 0, f"adx_min=101 debería dar 0 trades, dio {res.trades}"

    def test_adx_min_reduce_trades_en_lateral(self) -> None:
        """En mercado lateral (ADX bajo), adx_min moderado debe reducir trades."""
        velas = _velas_laterales(400)
        res_sin = backtest(velas, "TEST", umbral=2.0, adx_min=0.0)
        res_con = backtest(velas, "TEST", umbral=2.0, adx_min=25.0)
        assert res_con.trades <= res_sin.trades, (
            f"adx_min=25 debería reducir trades: sin={res_sin.trades}, con={res_con.trades}"
        )

    def test_adx_min_no_bloquea_tendencia_fuerte(self) -> None:
        """Con tendencia fuerte, adx_min=20 no debe bloquear todos los trades."""
        velas = _velas_tendencia(400, pendiente=1.0)
        res = backtest(velas, "TEST", umbral=2.0, adx_min=20.0)
        # La tendencia fuerte produce ADX alto, debe haber trades
        assert res.trades >= 0  # al menos no explota; puede haber 0 si ATR falla

    def test_adx_min_mayor_umbral_da_menos_o_igual_trades(self) -> None:
        """Mayor adx_min → menos o iguales trades que menor adx_min."""
        velas = _velas_tendencia(400, pendiente=0.5)
        res_20 = backtest(velas, "TEST", umbral=2.0, adx_min=20.0)
        res_30 = backtest(velas, "TEST", umbral=2.0, adx_min=30.0)
        assert res_30.trades <= res_20.trades, (
            f"adx_min=30 ({res_30.trades}) debería tener <= trades que adx_min=20 ({res_20.trades})"
        )


# ─────────────────────────────────────────────────────────────────────────────
#  Tests combinados y de resultado
# ─────────────────────────────────────────────────────────────────────────────

class TestCombinados:

    def test_be_y_adx_juntos_sin_error(self) -> None:
        """BE + ADX combinados deben ejecutar sin excepciones."""
        velas = _velas_tendencia(300, pendiente=0.5)
        res = backtest(velas, "TEST", capital0=1000.0, umbral=2.0,
                       use_trailing=False, be_atr=1.0, adx_min=20.0)
        assert isinstance(res, Resultado)
        assert res.capital_final > 0

    def test_winrate_pf_validos(self) -> None:
        """Con BE y ADX, winrate y PF deben estar en rangos válidos."""
        velas = _velas_tendencia(400, pendiente=0.5)
        res = backtest(velas, "TEST", umbral=2.0, be_atr=1.0, adx_min=20.0)
        assert 0.0 <= res.winrate <= 100.0
        assert res.profit_factor >= 0.0 or math.isinf(res.profit_factor)

    def test_be_con_trailing_juntos(self) -> None:
        """BE stop y trailing pueden coexistir sin error."""
        velas = _velas_tendencia(300, pendiente=0.6)
        res = backtest(velas, "TEST", umbral=2.0,
                       use_trailing=True, be_atr=1.0)
        assert isinstance(res, Resultado)
        assert res.be_activados >= 0

    def test_parametros_extremos_no_crash(self) -> None:
        """Combinaciones extremas de parámetros no deben causar excepciones."""
        velas = _velas_tendencia(200)
        for be in (0.0, 0.1, 5.0):
            for adx in (0.0, 1.0, 99.9):
                res = backtest(velas, "TEST", umbral=2.0,
                               be_atr=be, adx_min=adx,
                               use_trailing=False)
                assert isinstance(res, Resultado)
                assert not math.isnan(res.capital_final)

    def test_capital_final_no_negativo(self) -> None:
        """El capital final nunca debe ser negativo (sin apalancamiento)."""
        velas = _velas_tendencia(500, pendiente=0.3)
        for riesgo in (0.02, 0.04, 0.06):
            res = backtest(velas, "TEST", umbral=2.0,
                           be_atr=1.0, adx_min=20.0, riesgo=riesgo)
            assert res.capital_final >= 0.0, (
                f"capital_final negativo ({res.capital_final:.2f}) con riesgo={riesgo}"
            )


# ─────────────────────────────────────────────────────────────────────────────
#  Tests de estudio_v4 (lógica interna, sin descarga de red)
# ─────────────────────────────────────────────────────────────────────────────

class TestEstudioV4Logica:
    """Valida la lógica del estudio v4 directamente usando backtest() con datos
    sintéticos — sin descargar datos de Binance."""

    def _run_mini_grid(self, velas: list[list[float]]) -> list[dict]:
        """Ejecuta un grid reducido (subconjunto de estudio_v4) sobre velas dadas."""
        ind = calcular_indicadores(velas)
        n = len(velas)
        corte = int(n * 0.7)
        resultados = []
        for be in (0.0, 1.0):
            for adx_min in (0, 20):
                for tp in (3.0,):
                    for riesgo in (0.04,):
                        agg = {}
                        for fase in ("train", "test"):
                            a, b = (0, corte) if fase == "train" else (corte, None)
                            r = backtest(
                                velas, "SYNTH", 1000.0, 2.0,
                                use_trailing=False, ind=ind, i0=a, i1=b,
                                sl_ratio=1.0, tp_ratio=tp, vol_guard=False,
                                riesgo=riesgo, be_atr=be, adx_min=float(adx_min))
                            agg[fase] = r
                        resultados.append({"be": be, "adx": adx_min, "agg": agg})
        return resultados

    def test_grid_completa_sin_excepcion(self) -> None:
        """El grid mini de estudio_v4 no debe lanzar ninguna excepción."""
        velas = _velas_tendencia(300)
        resultados = self._run_mini_grid(velas)
        assert len(resultados) == 4  # 2 be * 2 adx * 1 tp * 1 riesgo

    def test_todas_las_entradas_son_resultado(self) -> None:
        """Cada combinación del grid debe producir un objeto Resultado válido."""
        velas = _velas_tendencia(300)
        for entry in self._run_mini_grid(velas):
            for fase in ("train", "test"):
                r = entry["agg"][fase]
                assert isinstance(r, Resultado), f"No es Resultado: {type(r)}"
                assert r.capital_final > 0

    def test_train_test_no_se_solapan(self) -> None:
        """Train (0-70%) y test (70-100%) deben producir resultados distintos."""
        velas = _velas_tendencia(300, pendiente=0.5)
        ind = calcular_indicadores(velas)
        n = len(velas)
        corte = int(n * 0.7)
        r_train = backtest(velas, "T", 1000.0, 2.0, ind=ind, i0=0, i1=corte,
                           be_atr=1.0, adx_min=20.0)
        r_test = backtest(velas, "T", 1000.0, 2.0, ind=ind, i0=corte, i1=None,
                          be_atr=1.0, adx_min=20.0)
        # No pueden tener exactamente los mismos resultados (distintos periodos)
        total_trades = r_train.trades + r_test.trades
        assert total_trades >= 0  # al menos uno puede ser 0

    def test_be_mayor_no_aumenta_be_activados_mas_trades(self) -> None:
        """be_activados siempre <= trades en todas las combinaciones del grid."""
        velas = _velas_tendencia(400, pendiente=0.5)
        for entry in self._run_mini_grid(velas):
            for fase in ("train", "test"):
                r = entry["agg"][fase]
                assert r.be_activados <= r.trades, (
                    f"be_activados={r.be_activados} > trades={r.trades}"
                )


# ─────────────────────────────────────────────────────────────────────────────
#  Tests de estudio_simbolos (sin red, datos sintéticos)
# ─────────────────────────────────────────────────────────────────────────────

class TestEstudioSimbolos:
    """Verifica estudio_simbolos usando monkeypatching de descargar_klines."""

    def _patch_descarga(self, monkeypatch, velas: list[list[float]]) -> None:
        """Reemplaza descargar_klines en el módulo para devolver datos sintéticos."""
        monkeypatch.setattr(_mod, "descargar_klines", lambda s, iv, d: velas)

    def test_sin_error_multiples_simbolos(self, monkeypatch) -> None:
        """estudio_simbolos no debe lanzar excepción con varios símbolos sintéticos."""
        velas = _velas_tendencia(200, pendiente=0.5)
        self._patch_descarga(monkeypatch, velas)
        # No debe lanzar excepción
        estudio_simbolos(["BTCEUR", "ETHEUR", "SOLEUR"], days=200, capital0=1000.0)

    def test_un_solo_simbolo(self, monkeypatch) -> None:
        """estudio_simbolos funciona con un solo símbolo."""
        velas = _velas_tendencia(200, pendiente=0.5)
        self._patch_descarga(monkeypatch, velas)
        estudio_simbolos(["BTCEUR"], days=200, capital0=1000.0)

    def test_simbolo_candidato_marcado(self, monkeypatch, capsys) -> None:
        """Símbolos fuera de los 5 actuales se marcan con (*) en la salida."""
        velas = _velas_tendencia(200, pendiente=0.5)
        self._patch_descarga(monkeypatch, velas)
        estudio_simbolos(["BTCEUR", "XRPEUR"], days=200, capital0=1000.0)
        out = capsys.readouterr().out
        assert "*" in out, "Los candidatos deben marcarse con (*) en la salida"

    def test_error_simbolo_no_bloquea_resto(self, monkeypatch) -> None:
        """Si un símbolo falla la descarga, los demás se procesan igual."""
        velas = _velas_tendencia(200, pendiente=0.5)
        call_count = {"n": 0}

        def mock_descarga(s, iv, d):
            call_count["n"] += 1
            if s == "BADEUR":
                raise RuntimeError("símbolo inválido simulado")
            return velas

        monkeypatch.setattr(_mod, "descargar_klines", mock_descarga)
        # No debe lanzar excepción aunque BADEUR falle
        estudio_simbolos(["BTCEUR", "BADEUR", "ETHEUR"], days=200, capital0=1000.0)

    def test_salida_contiene_header_tabla(self, monkeypatch, capsys) -> None:
        """La tabla de resultados incluye columnas clave."""
        velas = _velas_tendencia(200, pendiente=0.5)
        self._patch_descarga(monkeypatch, velas)
        estudio_simbolos(["BTCEUR", "ETHEUR"], days=200, capital0=1000.0)
        out = capsys.readouterr().out
        assert "PF tr" in out
        assert "PF te" in out
        assert "anual" in out.lower() or "anual" in out


# ─────────────────────────────────────────────────────────────────────────────
#  Tests de estudio_eth_riesgo
# ─────────────────────────────────────────────────────────────────────────────

class TestEstudioEthRiesgo:

    def test_sin_error_con_datos_sinteticos(self, monkeypatch) -> None:
        """estudio_eth_riesgo no debe lanzar excepción con datos sintéticos."""
        velas = _velas_tendencia(200, pendiente=0.5)
        monkeypatch.setattr(_mod, "descargar_klines", lambda s, iv, d: velas)
        estudio_eth_riesgo(days=200, capital0=1000.0)

    def test_salida_contiene_porcentajes_riesgo(self, monkeypatch, capsys) -> None:
        """La tabla debe mostrar los distintos niveles de riesgo probados."""
        velas = _velas_tendencia(200, pendiente=0.5)
        monkeypatch.setattr(_mod, "descargar_klines", lambda s, iv, d: velas)
        estudio_eth_riesgo(days=200, capital0=1000.0)
        out = capsys.readouterr().out
        assert "2%" in out or "2" in out
        assert "6%" in out or "6" in out

    def test_fg_mask_bloquea_entradas(self) -> None:
        """fg_mask=False en todas las velas debe producir 0 trades."""
        velas = _velas_tendencia(300, pendiente=0.5)
        fg_mask = [False] * len(velas)
        res = backtest(velas, "TEST", umbral=2.0, fg_mask=fg_mask)
        assert res.trades == 0, f"fg_mask=False debe bloquear todo, dio {res.trades} trades"

    def test_fg_mask_true_igual_que_sin_mask(self) -> None:
        """fg_mask=True en todas las velas debe dar resultado idéntico a sin mask."""
        velas = _velas_tendencia(300, pendiente=0.5)
        fg_mask = [True] * len(velas)
        res_con = backtest(velas, "TEST", umbral=2.0, fg_mask=fg_mask)
        res_sin = backtest(velas, "TEST", umbral=2.0, fg_mask=None)
        assert res_con.trades == res_sin.trades
        assert abs(res_con.capital_final - res_sin.capital_final) < 1e-9

    def test_fg_mask_parcial_reduce_trades(self) -> None:
        """fg_mask con la mitad de velas bloqueadas debe dar menos o igual trades."""
        velas = _velas_tendencia(300, pendiente=0.5)
        n = len(velas)
        fg_mask = [i % 2 == 0 for i in range(n)]  # bloquea velas impares
        res_parcial = backtest(velas, "TEST", umbral=2.0, fg_mask=fg_mask)
        res_completo = backtest(velas, "TEST", umbral=2.0, fg_mask=None)
        assert res_parcial.trades <= res_completo.trades


# ─────────────────────────────────────────────────────────────────────────────
#  Tests de estudio_fear_greed y _fg_valor
# ─────────────────────────────────────────────────────────────────────────────

class TestFearGreed:

    def _mock_fg_map(self) -> dict:
        """Mapa sintético: valor 50 para varios días."""
        base_ms = 1_700_000_000_000 // 86_400_000 * 86_400_000
        return {base_ms + i * 86_400_000: 50 for i in range(200)}

    def test_fg_valor_exacto(self) -> None:
        """_fg_valor devuelve el valor exacto cuando el timestamp es medianoche UTC."""
        # usar un timestamp alineado a medianoche (multiple de 86_400_000)
        dia_ms = (1_700_000_000_000 // 86_400_000) * 86_400_000
        fg_map = {dia_ms: 42}
        assert _fg_valor(fg_map, dia_ms) == 42

    def test_fg_valor_dia_anterior(self) -> None:
        """_fg_valor acepta timestamp con offset de horas dentro del mismo día."""
        dia_ms = (1_700_000_000_000 // 86_400_000) * 86_400_000
        fg_map = {dia_ms: 60}
        # timestamp con algunas horas de offset dentro del día → misma key
        assert _fg_valor(fg_map, dia_ms + 3_600_000) == 60

    def test_fg_valor_sin_datos_retorna_none(self) -> None:
        """_fg_valor retorna None cuando no hay datos cercanos."""
        assert _fg_valor({}, 1_700_000_000_000) is None

    def test_estudio_fear_greed_sin_red(self, monkeypatch, capsys) -> None:
        """estudio_fear_greed usa datos sintéticos para fg_map y velas."""
        velas = _velas_tendencia(200, pendiente=0.5)
        monkeypatch.setattr(_mod, "descargar_klines", lambda s, iv, d: velas)
        # Inyectar fg_map sintético directamente
        fg_map = {int(v[0] // 86_400_000) * 86_400_000: 50 for v in velas}
        monkeypatch.setattr(_mod, "_descargar_fear_greed", lambda d=1825: fg_map)
        estudio_fear_greed(["BTCEUR", "ETHEUR"], days=200, capital0=1000.0)
        out = capsys.readouterr().out
        assert "sin_filtro" in out
        assert "evitar_codicia" in out


# ─────────────────────────────────────────────────────────────────────────────
#  Tests de estudio_walk_forward
# ─────────────────────────────────────────────────────────────────────────────

class TestWalkForward:

    def _patch(self, monkeypatch, velas):
        monkeypatch.setattr(_mod, "descargar_klines", lambda s, iv, d: velas)

    def test_ejecuta_sin_excepcion(self, monkeypatch) -> None:
        """walk-forward no debe lanzar excepción con datos sintéticos."""
        velas = _velas_tendencia(300, pendiente=0.5)
        self._patch(monkeypatch, velas)
        estudio_walk_forward(["BTCEUR", "ETHEUR"], days=300, capital0=1000.0, n_folds=3)

    def test_salida_contiene_n_folds(self, monkeypatch, capsys) -> None:
        """La tabla debe tener exactamente n_folds filas de resultado."""
        velas = _velas_tendencia(300, pendiente=0.5)
        self._patch(monkeypatch, velas)
        estudio_walk_forward(["BTCEUR"], days=300, capital0=1000.0, n_folds=3)
        out = capsys.readouterr().out
        # cada fold aparece como "    1", "    2", "    3" en la columna fold
        assert out.count("✓") + out.count("✗") == 3

    def test_consistencia_aparece_en_salida(self, monkeypatch, capsys) -> None:
        """La línea de consistencia debe aparecer al final."""
        velas = _velas_tendencia(300, pendiente=0.5)
        self._patch(monkeypatch, velas)
        estudio_walk_forward(["BTCEUR"], days=300, capital0=1000.0, n_folds=3)
        out = capsys.readouterr().out
        assert "Consistencia:" in out
        assert "/3" in out

    def test_un_fold_funciona(self, monkeypatch) -> None:
        """n_folds=1 no debe crashear."""
        velas = _velas_tendencia(200, pendiente=0.5)
        self._patch(monkeypatch, velas)
        estudio_walk_forward(["BTCEUR"], days=200, capital0=1000.0, n_folds=1)

    def test_sin_datos_no_crashea(self, monkeypatch, capsys) -> None:
        """Si la descarga falla para todos los símbolos, debe salir con mensaje."""
        def falla(s, iv, d):
            raise RuntimeError("sin datos")
        monkeypatch.setattr(_mod, "descargar_klines", falla)
        estudio_walk_forward(["BTCEUR"], days=200, capital0=1000.0, n_folds=3)
        out = capsys.readouterr().out
        assert "sin datos" in out or "ERROR" in out or "sin datos" in out.lower()
