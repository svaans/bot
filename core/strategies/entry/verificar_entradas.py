from __future__ import annotations
from datetime import datetime, timezone
import asyncio
import pandas as pd
from core.utils import configurar_logger, safe_resample
from core.adaptador_dinamico import calcular_tp_sl_adaptativos
from core.adaptador_umbral import calcular_umbral_adaptativo
from core.config_manager.dinamica import adaptar_configuracion
from core.data import coincidencia_parcial
from core.estrategias import filtrar_por_direccion
from core.strategies.tendencia import obtener_tendencia
from core.strategies.evaluador_tecnico import (
    evaluar_puntaje_tecnico,
    cargar_pesos_tecnicos,
)
from indicators.helpers import get_rsi, get_momentum
from binance_api.cliente import fetch_ohlcv_async
from core.utils.utils import (
    distancia_minima_valida,
    verificar_integridad_datos,
    intervalo_a_segundos,
)
from core.utils.cooldown import calcular_cooldown
from core.contexto_externo import obtener_puntaje_contexto
from core.metricas_semanales import metricas_tracker
from core.scoring import DecisionTrace, DecisionReason
log = configurar_logger('verificar_entrada')
UTC = timezone.utc

def _reparar_huecos(
    df: pd.DataFrame,
    max_gap_multiplo: int = 3,
    max_huecos_tolerados: int = 2,
    max_velas_consecutivas: int = 2,
) -> tuple[pd.DataFrame, bool, dict]:
    """Repara huecos pequeños generando velas sintéticas.

    Acepta como máximo ``max_huecos_tolerados`` gaps donde el delta entre velas sea
    menor o igual a ``max_gap_multiplo`` veces el intervalo y el número de velas
    faltantes consecutivas no exceda ``max_velas_consecutivas``.

    Devuelve:
        ``df_reparado``: DataFrame con huecos rellenos.
        ``ok``: ``True`` si la reparación fue posible.
        ``stats``: información de diagnóstico.
    """
    if df.empty or 'timestamp' not in df.columns:
        return df, False, {"motivo": "df_vacio"}

    df2 = df.sort_values('timestamp').copy()
    # Asegurar tipos y monotonicidad
    df2['timestamp'] = pd.to_numeric(df2['timestamp'], errors='coerce')
    df2 = df2.dropna(subset=['timestamp'])
    df2 = df2.drop_duplicates(subset=['timestamp'])

    if len(df2) < 2:
        return df2, False, {"motivo": "muy_pocos_datos"}

    # Inferir intervalo por mediana de diferencias
    diffs = df2['timestamp'].diff().dropna()
    intervalo_ms = int(diffs.median())
    if intervalo_ms <= 0:
        return df2, False, {"motivo": "intervalo_invalido"}

    # Detectar huecos
    gaps = diffs[diffs > intervalo_ms].to_list()
    if not gaps:
        return df2, True, {"intervalo_ms": intervalo_ms, "gaps": 0}

    # Limitar tolerancias
    tolerable_ms = max_gap_multiplo * intervalo_ms
    tolerables = [g for g in gaps if g <= tolerable_ms]
    criticos = [g for g in gaps if g > tolerable_ms]

    if criticos:
        # Hueco(s) demasiado grande(s) -> no reparamos
        return df2, False, {"intervalo_ms": intervalo_ms, "gaps_criticos": len(criticos)}

    # Verificar cantidad de velas consecutivas perdidas en cada hueco
    velas_consecutivas = [(g // intervalo_ms) - 1 for g in tolerables]
    if any(vc > max_velas_consecutivas for vc in velas_consecutivas):
        return df2, False, {
            "intervalo_ms": intervalo_ms,
            "max_velas_consecutivas": max(velas_consecutivas),
        }

    if len(tolerables) > max_huecos_tolerados:
        # Demasiados huecos aunque pequeños
        return df2, False, {"intervalo_ms": intervalo_ms, "gaps_tolerables": len(tolerables)}

    # Rellenar rejilla completa y sintetizar faltantes
    idx = pd.to_datetime(df2['timestamp'], unit='ms')
    full_idx = pd.date_range(idx.iloc[0], idx.iloc[-1], freq=f'{intervalo_ms}ms')
    df2 = df2.set_index(idx).reindex(full_idx)

    # Para velas faltantes, generamos OHLC a partir del close previo y vol=0
    df2['close'] = df2['close'].ffill()
    for col in ('open', 'high', 'low'):
        df2[col] = df2[col].fillna(df2['close'])
    if 'volume' in df2.columns:
        df2['volume'] = df2['volume'].fillna(0)

    # Volver a timestamp ms
    df2 = df2.reset_index(drop=False).rename(columns={'index': 'dt'})
    df2['timestamp'] = (df2['dt'].astype('int64') // 10**6).astype('int64')
    df2 = df2.drop(columns=['dt'])

    return df2, True, {
        "intervalo_ms": intervalo_ms,
        "gaps_reparados": len(tolerables),
    }


def _tendencia_principal(tendencias: list[str | None]) -> tuple[str | None, float]:
    """Devuelve la tendencia predominante y su proporción."""
    valores = [t for t in tendencias if t]
    if not valores:
        return None, 0.0
    tendencia = max(set(valores), key=valores.count)
    proporcion = valores.count(tendencia) / len(tendencias)
    return tendencia, proporcion


def validar_marcos(symbol_state: dict) -> bool:
    """Valida coherencia entre marcos temporales.

    ``symbol_state`` debe incluir 'symbol', '1m', '5m', '1h', '1d' y opcionalmente
    un diccionario ``config`` con ``umbral_confirmacion_micro`` y
    ``umbral_confirmacion_macro``. Si las tendencias micro y macro superan
    los umbrales y se contradicen, retorna ``False``.
    """
    symbol = symbol_state.get('symbol', '')
    config = symbol_state.get('config', {})
    umbral_micro = config.get('umbral_confirmacion_micro', 0.6)
    umbral_macro = config.get('umbral_confirmacion_macro', 0.6)

    def extraer(tf: str):
        dato = symbol_state.get(tf)
        if isinstance(dato, pd.DataFrame):
            if dato.empty:
                return None
            return obtener_tendencia(symbol, dato)
        return dato

    micro = [extraer('1m'), extraer('5m')]
    macro = [extraer('1h'), extraer('1d')]

    micro_dir, micro_ratio = _tendencia_principal(micro)
    macro_dir, macro_ratio = _tendencia_principal(macro)

    if (
        micro_dir
        and macro_dir
        and micro_ratio >= umbral_micro
        and macro_ratio >= umbral_macro
        and micro_dir != macro_dir
    ):
        return False
    return True

async def verificar_entrada(trader, symbol: str, df: pd.DataFrame, estado) -> (
    dict | None):
    """
    Evalúa condiciones de entrada y devuelve info de operación
    si cumple todos los filtros, de lo contrario None.
    """

    # Normalización mínima previa
    if df is None or df.empty:
        log.warning(f'[{symbol}] DF vacío')
        metricas_tracker.registrar_filtro('datos_invalidos')
        return None
    df = df.sort_values('timestamp').copy()
    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp']).drop_duplicates(subset=['timestamp']).reset_index(drop=True)

    # Integridad básica; si falla intentamos reparación de huecos tolerables
    if not verificar_integridad_datos(df):
        reparado, ok, stats = _reparar_huecos(df)
        if ok and verificar_integridad_datos(reparado):
            log.info(f'[{symbol}] Datos incompletos reparados: {stats}')
            df = reparado
        else:
            if stats.get('gaps_criticos'):
                cliente = getattr(trader.data_feed, '_cliente', None) or getattr(trader, 'cliente', None)
                if cliente:
                    intervalo = trader.config.intervalo_velas
                    intervalo_ms = intervalo_a_segundos(intervalo) * 1000
                    diffs = df['timestamp'].diff()
                    gaps = diffs[diffs > intervalo_ms]
                    async def _backfill_critico() -> None:
                        dfs_nuevas: list[pd.DataFrame] = []
                        for idx in gaps.index:
                            if idx <= 0 or len(df) < 2:
                                log.warning(f'[{symbol}] Datos insuficientes para backfill crítico')
                                return
                            inicio_gap = int(df.loc[idx - 1, 'timestamp']) + intervalo_ms
                            fin_gap = int(df.loc[idx, 'timestamp']) - intervalo_ms
                            faltantes = int((fin_gap - inicio_gap) // intervalo_ms) + 1
                            try:
                                nuevas = await fetch_ohlcv_async(
                                    cliente,
                                    symbol,
                                    intervalo,
                                    since=inicio_gap,
                                    limit=faltantes,
                                )
                            except Exception as e:
                                log.warning(f'[{symbol}] Error backfill crítico: {e}')
                                return
                            dfs_nuevas.append(
                                pd.DataFrame(
                                    [
                                        {
                                            'timestamp': o[0],
                                            'open': float(o[1]),
                                            'high': float(o[2]),
                                            'low': float(o[3]),
                                            'close': float(o[4]),
                                            'volume': float(o[5]),
                                        }
                                        for o in nuevas
                                    ]
                                )
                            )
                        if not dfs_nuevas:
                            return
                        estado.df = (
                                pd.concat([estado.df, *dfs_nuevas])
                                .drop_duplicates(subset=['timestamp'])
                                .sort_values('timestamp')
                                .reset_index(drop=True)
                            )
                        if verificar_integridad_datos(estado.df):
                            log.info(f'[{symbol}] Hueco crítico reparado vía REST')
                        else:
                            log.warning(
                                f'[{symbol}] Datos corruptos irreparables tras backfill: {stats}'
                            )

                    asyncio.create_task(_backfill_critico())
                    metricas_tracker.registrar_filtro('datos_invalidos')
                    return None
            else:
                log.warning(f'[{symbol}] Datos corruptos irreparables: {stats}')
                metricas_tracker.registrar_filtro('datos_invalidos')
                return None

    config = adaptar_configuracion(symbol, df, trader.config_por_simbolo.get(symbol, {}))
    trader.config_por_simbolo[symbol] = config
    tendencia = obtener_tendencia(symbol, df)
    log.debug(f'[{symbol}] Tendencia: {tendencia}')
    df_sorted = df.sort_values('timestamp')
    df_idx = df_sorted.set_index(pd.to_datetime(df_sorted['timestamp'], unit='ms'))
    df_5m = safe_resample(df_idx, '5min').last().dropna() if len(df_idx) >= 5 else None
    df_1h = safe_resample(df_idx, '1h').last().dropna() if len(df_idx) >= 60 else None
    df_1d = safe_resample(df_idx, '1d').last().dropna() if len(df_idx) >= 1440 else None
    if not validar_marcos(
        {
            'symbol': symbol,
            '1m': df,
            '5m': df_5m,
            '1h': df_1h,
            '1d': df_1d,
            'config': config,
        }
    ):
        log.info(f'[{symbol}] Contradicción entre marcos temporales.')
        metricas_tracker.registrar_filtro('marcos_temporales')
        return None
    async def _engine_eval() -> dict:
        return await trader.engine.evaluar_entrada(
            symbol,
            df,
            tendencia=tendencia,
            config={
                **config,
                "contradicciones_bloquean_entrada": getattr(
                    trader, "contradicciones_bloquean_entrada", True
                ),
                "usar_score_tecnico": getattr(trader, "usar_score_tecnico", True),
            },
            pesos_symbol=trader.pesos_por_simbolo.get(symbol, {}),
        )

    engine_eval = await _engine_eval()
    estrategias = engine_eval.get('estrategias_activas', {})
    if not estrategias:
        log.warning(f'[{symbol}] Sin estrategias activas tras engine.')
        metricas_tracker.registrar_filtro('sin_estrategias')
        return None
    log.debug(f'[{symbol}] Estrategias activas: {list(estrategias.keys())}')
    if estado.estrategias_buffer:
        estado.estrategias_buffer[-1] = estrategias
    trader.persistencia.actualizar(symbol, estrategias)
    buffer_len = len(estado.buffer)
    historico_estrategias = list(estado.estrategias_buffer)[-100:]
    persistencia_score = coincidencia_parcial(
        historico_estrategias, trader.pesos_por_simbolo.get(symbol, {}), ventanas=5
    )
    if buffer_len < 30 and persistencia_score < 1:
        metricas_tracker.registrar_filtro('prebuffer')
        return None
    contexto_umbral = {
        "rsi": engine_eval.get("rsi"),
        "slope": engine_eval.get("slope"),
        "persistencia": persistencia_score,
    }
    umbral = calcular_umbral_adaptativo(symbol, df, contexto_umbral)
    estrategias_persistentes = {
        e: True
        for e, activo in estrategias.items()
        if activo and trader.persistencia.es_persistente(symbol, e)
    }
    direccion = 'short' if tendencia == 'bajista' else 'long'
    estrategias_persistentes, incoherentes = filtrar_por_direccion(
        estrategias_persistentes, direccion)
    penalizacion = 0.05 * len(incoherentes) if incoherentes else 0.0
    puntaje = sum(
        trader.pesos_por_simbolo.get(symbol, {}).get(e, 0)
        for e in estrategias_persistentes
    )
    puntaje += trader.persistencia.peso_extra * len(estrategias_persistentes)
    puntaje -= penalizacion
    cierre = trader.historial_cierres.get(symbol)
    if cierre:
        motivo = cierre.get('motivo')
        if motivo == 'stop loss':
            velas = cierre.get('velas', 0) + 1
            cierre['velas'] = velas
            perdidas = cierre.get('perdidas_consecutivas', 1)
            base_cd = int(config.get('cooldown_tras_perdida', 5))
            cooldown = calcular_cooldown(perdidas, base_cd)
            if velas < cooldown:
                log.info(
                    f'[{symbol}] Cooldown tras stop loss ({velas}/{cooldown}) activo.')
                metricas_tracker.registrar_filtro('cooldown')
                return None
            trader.historial_cierres.pop(symbol, None)
        elif motivo == 'cambio de tendencia':
            precio_actual = float(df['close'].iloc[-1])
            if not trader._validar_reentrada_tendencia(symbol, df, cierre,
                precio_actual):
                cierre['velas'] = cierre.get('velas', 0) + 1
                metricas_tracker.registrar_filtro('reentrada_tendencia')
                return None
            trader.historial_cierres.pop(symbol, None)
    hoy = datetime.now(UTC).date().isoformat()
    limite_base = getattr(trader.config, 'max_perdidas_diarias', 6)
    try:
        ultimo_ts = int(df['timestamp'].iloc[-1])
        inicio_ts = ultimo_ts - 24 * 60 * 60 * 1000
        df_dia = df[df['timestamp'] >= inicio_ts]
        volatilidad_dia = df_dia['close'].pct_change().std()
    except Exception:
        volatilidad_dia = df['close'].pct_change().tail(1440).std()
    if volatilidad_dia > 0.05:
        limite = max(3, int(limite_base * 0.5))
    elif volatilidad_dia < 0.02:
        limite = int(limite_base * 1.2)
    else:
        limite = limite_base
    if cierre and cierre.get('fecha_perdidas') == hoy and cierre.get(
        'perdidas_consecutivas', 0) >= limite:
        log.info(f'[{symbol}] Bloqueado por pérdidas consecutivas: {limite}')
        metricas_tracker.registrar_filtro('perdidas_consecutivas')
        return None
    peso_total = sum(
        trader.pesos_por_simbolo.get(symbol, {}).get(e, 0)
        for e in estrategias_persistentes
    )
    peso_min_total = config.get('peso_minimo_total', 0.5)
    diversidad_min = config.get('diversidad_minima', 2)
    rsi = engine_eval.get('rsi')
    if rsi is None:
        rsi = get_rsi(df)
    elif isinstance(rsi, pd.Series):
        rsi = rsi.iloc[-1]
    if rsi is None:
        log.warning(f'[{symbol}] RSI insuficiente, entrada descartada')
        metricas_tracker.registrar_filtro('datos_invalidos')
        return None
    momentum = engine_eval.get('momentum')
    if momentum is None:
        momentum = get_momentum(df)
    elif isinstance(momentum, pd.Series):
        momentum = momentum.iloc[-1]
    if momentum is None:
        log.warning(f'[{symbol}] Momentum insuficiente, entrada descartada')
        metricas_tracker.registrar_filtro('datos_invalidos')
        return None
    if trader.usar_score_tecnico:
        score_tecnico, puntos_tecnicos = trader._calcular_score_tecnico(
            df, rsi, momentum, tendencia, direccion
        )
    else:
        score_tecnico = None
        puntos_tecnicos = None
    ok_pers, valor_pers, minimo_pers = trader._evaluar_persistencia(symbol,
        estado, df, trader.pesos_por_simbolo.get(symbol, {}), tendencia,
        puntaje, umbral, estrategias)
    razones = []
    if not trader._validar_puntaje(symbol, puntaje, umbral, config.get(
        'modo_agresivo', False)):
        razones.append('puntaje')
    diversidad_ok = await trader._validar_diversidad(symbol, peso_total,
        peso_min_total, estrategias_persistentes, diversidad_min, df,
        config.get('modo_agresivo', False))
    if not diversidad_ok:
        umbral_peso_unico = config.get('umbral_peso_estrategia_unica',
            peso_min_total * 1.5) or (peso_min_total * 1.5)
        umbral_score_unico = config.get('umbral_score_estrategia_unica',
            trader.umbral_score_tecnico * 1.5) or (trader.umbral_score_tecnico * 1.5)
        high_weight = peso_total >= umbral_peso_unico
        high_score = (
            (score_tecnico if score_tecnico is not None else 0) >= umbral_score_unico
        )
        if not (high_weight or high_score or config.get('modo_agresivo', False)):
            razones.append('diversidad')
    if not trader._validar_estrategia(symbol, df, estrategias, config):
        razones.append('estrategia')
    if not ok_pers:
        razones.append('persistencia')
    if razones:
        agresivo = config.get('modo_agresivo', False)
        if not agresivo or len(razones) > 2:
            log.info(f'[{symbol}] Rechazo por: {razones}')
            for r in razones:
                metricas_tracker.registrar_filtro(r)
            return None
    if trader.usar_score_tecnico:
        log.debug(
            f'[{symbol}] Score técnico {score_tecnico:.2f} componentes: {puntos_tecnicos.to_dict()}'
        )
    else:
        score_tecnico = None
    precio = float(df['close'].iloc[-1])
    sl, tp = calcular_tp_sl_adaptativos(symbol, df, config,
        trader.capital_por_simbolo.get(symbol, 0), precio)
    try:
        df_htf = (
            df.sort_values('timestamp')
            .set_index(pd.to_datetime(df['timestamp'], unit='ms'))
        )
        df_htf = safe_resample(df_htf, '5min').last().dropna()
        if len(df_htf) >= 30:
            tendencia_htf = obtener_tendencia(symbol, df_htf)
            if tendencia_htf != tendencia:
                ajuste = 0.8
                if direccion == 'long':
                    tp *= ajuste
                else:
                    sl *= ajuste
        else:
            log.info(
                f'[{symbol}] ℹ️ Datos insuficientes para tendencia HTF '
                f'(necesarias 30 velas, obtenidas {len(df_htf)})'
            )
            metricas_tracker.registrar_filtro('tendencia_htf_insuficiente')
    except Exception as e:
        log.error(f'❌ Error evaluando tendencia HTF para {symbol}: {e}')
        metricas_tracker.registrar_filtro('tendencia_htf_error')
    if not distancia_minima_valida(precio, sl, tp):
        log.warning(
            f'[{symbol}] SL/TP distancia mínima no válida: SL {sl} TP {tp}')
        metricas_tracker.registrar_filtro('sl_tp')
        return None
    
    eval_tecnica = await evaluar_puntaje_tecnico(symbol, df, precio, sl, tp)
    score_total = eval_tecnica['score_total']
    score_normalizado = eval_tecnica.get('score_normalizado')
    pesos_simbolo = await cargar_pesos_tecnicos(symbol)
    score_max = sum(pesos_simbolo.values())
    if score_normalizado is None:
        score_normalizado = score_total / score_max if score_max else score_total
    umbral_normalizado = umbral / score_max if score_max else umbral
    if score_normalizado < umbral_normalizado:
        trace = DecisionTrace(
            score_normalizado, umbral_normalizado, DecisionReason.BELOW_THRESHOLD, puntos_tecnicos
        )
        log.info(f'[{symbol}] Trace: {trace.to_json()}')
        metricas_tracker.registrar_filtro('score_tecnico')
        return None
    abiertas = [s for s, o in trader.orders.ordenes.items() if o.cantidad_abierta > 0 and s != symbol]
    if abiertas:
        correlaciones = trader._calcular_correlaciones(symbols=[symbol, *abiertas])
        if not correlaciones.empty and symbol in correlaciones.columns:
            corr = correlaciones.loc[symbol, abiertas].abs().max()
            umbral_corr = getattr(trader.config, 'umbral_correlacion', 0.9)
            if corr >= umbral_corr:
                log.info(f'[{symbol}] Correlación {corr:.2f} supera umbral {umbral_corr}')
                metricas_tracker.registrar_filtro('correlacion')
                return None
    puntaje_macro = obtener_puntaje_contexto(symbol)
    if abs(puntaje_macro) > getattr(trader.config, 'umbral_puntaje_macro', 6):
        log.info(f'[{symbol}] Contexto macro desfavorable ({puntaje_macro:.2f})')
        metricas_tracker.registrar_filtro('contexto_macro')
        return None
    log.info(
        f'✅ [{symbol}] Señal de entrada generada con {len(estrategias_persistentes)} estrategias activas.'
        )
    candle_ts = int(df['timestamp'].iloc[-1])
    version = getattr(trader.config, 'version', 'v1')
    return {
        'symbol': symbol,
        'precio': precio,
        'sl': sl,
        'tp': tp,
        'estrategias': estrategias_persistentes,
        'puntaje': puntaje,
        'umbral': umbral,
        'tendencia': tendencia,
        'direccion': direccion,
        'candle_close_ts': candle_ts,
        'strategy_version': version,
        'score_tecnico': score_tecnico,
        'detalles_tecnicos': eval_tecnica.get('detalles', {}),
    }
