# core/trader.py

import asyncio
import json
import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from binance_api.websocket import escuchar_velas
from estrategias_entrada.gestor_entradas import evaluar_estrategias, entrada_permitida
from estrategias_salida.gestor_salidas import evaluar_salidas, verificar_filtro_tecnico
from estrategias_salida.salida_stoploss import salida_stoploss
from estrategias_salida.salida_trailing_stop import verificar_trailing_stop
from estrategias_salida.salida_por_tendencia import verificar_reversion_tendencia
from estrategias_salida.reajuste_tp_sl import calcular_promedios_sl_tp
from core.tendencia import detectar_tendencia, señales_repetidas
from core.adaptador_umbral import (
    calcular_umbral_adaptativo,
    cargar_umbral_optimo,
    calcular_tp_sl_adaptativos,
)
from core.logger import configurar_logger, log_resumen_operacion
from config.config import INTERVALO_VELAS, MODO_REAL
from binance_api.cliente import crear_cliente
from core.utils import respaldar_archivo, guardar_operacion_en_csv, validar_dataframe, segundos_transcurridos
from core.pesos import cargar_pesos_estrategias
from aprendizaje.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from core.configuracion import cargar_configuracion_simbolo
from core.adaptador_configuracion import configurar_parametros_dinamicos
from core.monitor_estado_bot import monitorear_estado_bot, monitorear_estado_periodicamente
from core import ordenes_reales
from core.riesgo import riesgo_superado, actualizar_perdida
from filtros.validador_entradas import evaluar_validez_estrategica
from filtros.filtro_salidas import validar_necesidad_de_salida
from aprendizaje.aprendizaje_en_linea import registrar_resultado_trade
from indicadores.rsi import calcular_rsi
from indicadores.momentum import calcular_momentum
from indicadores.slope import calcular_slope


log = configurar_logger("trading_bot")



class Trader:
    def __init__(self, symbols):
        self.symbols = symbols
        self.cliente = crear_cliente()
        self.config_por_simbolo = {
            symbol: cargar_configuracion_simbolo(symbol)
            for symbol in self.symbols
        }
        self.estado = {s: "esperando" for s in symbols}
        self.buffer = {s: [] for s in symbols}
        self.ordenes_abiertas = {}
        self.pesos_por_simbolo = cargar_pesos_estrategias()
        self.historial_cierres = {}
        self.ultimo_stop_loss = {s: None for s in symbols}
        self.ultimo_umbral = {s: None for s in symbols}
        self.ultimo_timestamp = {s: None for s in symbols}
        self.ultima_tendencia = {s: None for s in symbols}
        self.ultimas_estrategias = {s: None for s in symbols}
        self.locks = {s: asyncio.Lock() for s in symbols}



        if self.ordenes_abiertas:
            log.warning("⚠️ Órdenes abiertas encontradas. No se eliminarán automáticamente.")
            log.info(f"📌 Órdenes encontradas: {list(self.ordenes_abiertas.keys())}")
        else:
            log.info("📥 Precargando velas históricas...")
            for symbol in self.symbols:
                try:
                    ohlcv = self.cliente.fetch_ohlcv(symbol.replace("/", ""), timeframe="1m", limit=30)
                    self.buffer[symbol] = [
                        {
                            "symbol": symbol,
                            "timestamp": vela[0],
                            "open": vela[1],
                            "high": vela[2],
                            "low": vela[3],
                            "close": vela[4],
                            "volume": vela[5],
                        }
                        for vela in ohlcv
                    ]

                    # 🔍 Análisis del arranque
                    if len(self.buffer[symbol]) == 30:
                        ventana = pd.DataFrame(self.buffer[symbol])
                        tendencia, _ = detectar_tendencia(symbol, ventana)
                        resultado = evaluar_estrategias(symbol, ventana, tendencia)
                        log.info(f"🔍 [{symbol}] Estrategias activas iniciales: {resultado['estrategias_activas']}")
                        log.info(f"📊 [{symbol}] Puntaje técnico inicial: {resultado['puntaje']}")

                    log.info(f"✅ {symbol}: 30 velas precargadas.")
                except Exception as e:
                    log.error(f"❌ Error precargando {symbol}: {e}")
                time.sleep(0.5)

    async def abrir_operacion_real(self, symbol, precio_entrada, sl, tp, estrategias_activas, tendencia):
        config = self.config_por_simbolo.get(symbol, {})

        try:
            balance = self.cliente.fetch_balance()
            capital_total = balance['total'].get('EUR', 0)
            riesgo_maximo = config.get("riesgo_maximo_diario", 0.3)

            if riesgo_superado(umbral=riesgo_maximo, capital_total=capital_total):
                log.warning(f"🚫 Riesgo diario superado para {symbol}. Entrada bloqueada.")
                return

            euros_disponibles = balance['total'].get('EUR', 0)
            symbols_activos = list(self.symbols)
            euros_por_simbolo = euros_disponibles / len(symbols_activos)
            euros_seguro = euros_por_simbolo * 0.95

            cantidad_crypto = round(euros_seguro / precio_entrada, 6)
            if cantidad_crypto <= 0:
                log.warning(f"❌ Cantidad inválida al calcular compra para {symbol}")
                return

            valor_total = round(precio_entrada * cantidad_crypto, 2)
            if valor_total < 10:
                log.warning(f"❌ Valor total ({valor_total:.2f} EUR) por debajo del mínimo para {symbol}")
                return

            if euros_disponibles < valor_total:
                log.warning(f"❌ Saldo insuficiente. Disponible: {euros_disponibles:.2f}, Necesario: {valor_total:.2f}")
                return
            
            if symbol in self.ordenes_abiertas:
                log.warning(f"⚠️ Ya existe una orden abierta para {symbol}, se evita duplicación.")
                return


            log.info(f"🟢 Ejecutando ORDEN REAL en {symbol}: Comprar {cantidad_crypto} a {precio_entrada} EUR")
            orden = self.cliente.create_market_buy_order(symbol.replace("/", ""), cantidad_crypto)

            self.ordenes_abiertas[symbol] = {
                "symbol": symbol,
                "precio_entrada": precio_entrada,
                "stop_loss": sl,
                "take_profit": tp,
                "cantidad": cantidad_crypto,
                "timestamp": datetime.utcnow().isoformat(),
                "max_price": precio_entrada,
                "direccion": "long",
                "tendencia": tendencia,
                "estrategias_activas": estrategias_activas
            }
            self.guardar_orden_real(self.ordenes_abiertas[symbol].copy())
            self.ordenes_abiertas = ordenes_reales.obtener_todas_las_ordenes()

        except Exception as e:
            log.error(f"❌ Error al ejecutar orden real para {symbol}: {e}")



    async def procesar_vela(self, vela):
        symbol = vela["symbol"]
        async with self.locks[symbol]:
            config = self.config_por_simbolo.get(symbol, {})
            self.buffer[symbol].append(vela)
            if len(self.buffer[symbol]) > 50:
                self.buffer[symbol] = self.buffer[symbol][-50:]

            if len(self.buffer[symbol]) < 30:
                return

            df = pd.DataFrame(self.buffer[symbol])
            if not validar_dataframe(df, ["high", "low", "close"]):
                log.warning(f"⚠️ DataFrame inválido para {symbol}, omitiendo...")
                return
            
            # 🔄 Actualizar configuración con los últimos datos
            config = configurar_parametros_dinamicos(symbol, df, config)
            self.config_por_simbolo[symbol] = config

            if vela["timestamp"] == self.ultimo_timestamp.get(symbol):
                return
            self.ultimo_timestamp[symbol] = vela["timestamp"]

            tendencia, _ = detectar_tendencia(symbol, df)
            evaluacion = evaluar_estrategias(symbol, df, tendencia)

            if evaluacion is None or not isinstance(evaluacion.get("puntaje_total"), (int, float)):
                log.warning(f"⚠️ Evaluación fallida o puntaje inválido para {symbol}")
                return

            puntaje = evaluacion["puntaje_total"]
            estrategias_detectadas = evaluacion["estrategias_activas"]
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})

            condiciones_estables = (
                self.ultimo_umbral.get(symbol) is not None and
                self.ultima_tendencia.get(symbol) == tendencia and
                self.ultimas_estrategias.get(symbol) == estrategias_detectadas
            )

            if condiciones_estables:
                umbral = self.ultimo_umbral[symbol]
            else:
                umbral = cargar_umbral_optimo(symbol)
                if umbral == -1:
                    config_actual = self.config_por_simbolo.get(symbol, {})
                    umbral = calcular_umbral_adaptativo(symbol, df, estrategias_detectadas, pesos_symbol, config=config_actual)
                self.ultimo_umbral[symbol] = umbral
                self.ultima_tendencia[symbol] = tendencia
                self.ultimas_estrategias[symbol] = estrategias_detectadas

            if not isinstance(umbral, (int, float)):
                log.warning(f"⚠️ Umbral inválido: {umbral}")
                return

            log.info(f"📊 {symbol}: {tendencia.upper()} | Puntaje {puntaje:.2f}/{umbral:.2f} | Activas: {', '.join([k for k,v in estrategias_detectadas.items() if v]) or 'Ninguna'}")
            log.info(f"🔍 Estado → {symbol}: {self.estado.get(symbol)} | Orden abierta: {symbol in self.ordenes_abiertas}")

            if symbol in self.ordenes_abiertas:
                return

            cierre = self.historial_cierres.get(symbol)
            if cierre:
                cooldown = config.get("cooldown_tras_perdida", 5) * 60
                try:
                    ts = cierre["timestamp"]
                    if isinstance(ts, str):
                        ts = datetime.fromisoformat(ts)
                    elif isinstance(ts, (int, float)):
                        ts = datetime.utcfromtimestamp(ts)
                    tiempo_desde_cierre = (datetime.utcnow() - ts).total_seconds()
                except Exception as e:
                    log.warning(f"⚠️ No se pudo calcular cooldown para {symbol}: {e}")
                    tiempo_desde_cierre = float("inf")

                if cierre["motivo"].lower().strip() in ["stop loss", "estrategia: cambio de tendencia", "cambio de tendencia"] and tiempo_desde_cierre < cooldown:
                    log.info(f"🕒 Cooldown activo para {symbol}. Esperando tras pérdida anterior ({tiempo_desde_cierre:.0f}s)")
                    return

            estrategias_activas_list = [k for k, v in estrategias_detectadas.items() if v]
            peso_total = sum(pesos_symbol.get(k, 0) for k in estrategias_activas_list)
            diversidad = len(estrategias_activas_list)
            peso_min_total = config.get("peso_minimo_total", 0.5)
            diversidad_min = config.get("diversidad_minima", 2)

            if puntaje < umbral:
                log.debug(
                    f"🚫 Entrada no válida: Puntaje {puntaje:.2f} < Umbral {umbral:.2f}"
                )
                return
            if diversidad < diversidad_min or peso_total < peso_min_total:
                log.debug(
                    f"🚫 Entrada bloqueada por diversidad/peso insuficiente: {diversidad}/{diversidad_min}, {peso_total:.2f}/{peso_min_total:.2f}"
                )
                return
            if not evaluar_validez_estrategica(symbol, df, estrategias_detectadas):
                log.debug(
                    f"❌ Entrada rechazada por filtro estratégico en {symbol}."
                )
                return

            # Cálculo de volatilidad actual
            ventana_close = df["close"].tail(10)
            media_close = np.mean(ventana_close)
            volatilidad_actual = np.std(ventana_close) / media_close if media_close else 0

            # Evaluar persistencia dinámica
            repetidas = señales_repetidas(
                buffer=self.buffer[symbol],
                estrategias_func=pesos_symbol,
                tendencia_actual=tendencia,
                volatilidad_actual=volatilidad_actual,
                ventanas=5
            )

            # Condición inteligente de entrada
            if repetidas < 1 and puntaje < 1.2 * umbral:
                log.info(f"🚫 Entrada rechazada en {symbol}: {repetidas}/5 señales persistentes y puntaje débil ({puntaje:.2f})")
                return
            elif repetidas < 1:
                log.info(f"⚠️ Entrada débil en {symbol}: Sin persistencia pero puntaje alto ({puntaje}) > Umbral {umbral} — Permitida.")
            
            # 🎯 Validación técnica final
            rsi = calcular_rsi(df)
            momentum = calcular_momentum(df)
            slope = calcular_slope(df)

            if not entrada_permitida(symbol, puntaje, umbral, estrategias_detectadas, rsi, slope, momentum):
                return

            log.info(f"✅ Entrada confirmada en {symbol}. Puntaje {puntaje:.2f}, Peso {peso_total:.2f}, Diversidad {diversidad}, Persistencia {repetidas}/5")

            sl, tp = calcular_tp_sl_adaptativos(df, float(vela["close"]), config)
            log.info(f"📐 SL/TP para {symbol} → SL: {sl:.4f}, TP: {tp:.4f}")

            await self.abrir_operacion_real(symbol, float(vela["close"]), sl, tp, estrategias_detectadas, tendencia)
            


    async def verificar_cierres_reales(self, symbol, vela):
        if symbol not in self.ordenes_abiertas:
            return

        orden = ordenes_reales.obtener_orden(symbol)
        stop_loss = orden["stop_loss"]
        take_profit = orden["take_profit"]
        entrada = orden["precio_entrada"]
        cantidad = orden["cantidad"]
        timestamp_orden = orden.get("timestamp")

        df = pd.DataFrame(self.buffer[symbol])

        precio_max = float(vela["high"])
        precio_min = float(vela["low"])
        precio_cierre = float(vela["close"])
        max_price_actual = orden.get("max_price", entrada)

        # ⏱️ Tiempo máximo abierto (6h), pero solo cerrar si está en ganancia
        timestamp_orden = orden.get("timestamp")
        
        if timestamp_orden and segundos_transcurridos(timestamp_orden) > 6 * 3600:
            retorno_actual = (precio_cierre - orden["precio_entrada"]) / orden["precio_entrada"]
            if retorno_actual > 0:
                log.warning(f"⌛ [{symbol}] Orden expirada en ganancia — Retorno: {retorno_actual:.4f}")
                await self.cerrar_orden_real(symbol, precio_cierre, cantidad, exito=True, motivo="Expirada")
                return
            else:
                log.info(f"🕒 [{symbol}] Orden vencida pero aún en pérdida (retorno {retorno_actual:.4f}), se mantiene abierta")


        if df["low"].iloc[-1] <= orden["stop_loss"]:
            from estrategias_salida.salida_stoploss import salida_stoploss
            config_actual = self.config_por_simbolo.get(symbol, {})
            resultado = salida_stoploss(orden, df, config=config_actual)


            if resultado.get("cerrar", False):
                await self.cerrar_orden_real(symbol, orden["stop_loss"], cantidad, exito=False, motivo="Stop Loss")
            else:
                log.info(f"🛡️ SL evitado para {symbol} → {resultado['razon']}")
            return

        if precio_max >= take_profit:
            log.info(f"🎯 Take Profit alcanzado en {symbol}")
            await self.cerrar_orden_real(symbol, precio_max, cantidad, exito=True, motivo="Take Profit")
            return

        if precio_cierre > max_price_actual:
            self.ordenes_abiertas[symbol]["max_price"] = precio_cierre

        config_actual = configurar_parametros_dinamicos(symbol, df, self.config_por_simbolo.get(symbol, {}))
        self.config_por_simbolo[symbol] = config_actual
        cerrar, motivo_trailing = verificar_trailing_stop(orden, precio_cierre, config=config_actual)
        if cerrar:
            log.info(f"🔃 Trailing Stop activado para {symbol}")
            await self.cerrar_orden_real(symbol, precio_cierre, cantidad, exito=True, motivo=motivo_trailing)
            return

        
        if verificar_reversion_tendencia(symbol, df, self.ultima_tendencia.get(symbol)) and not verificar_filtro_tecnico(self, symbol):
            log.info(f"📉 Reversión de tendencia en {symbol}. Cerrando operación.")
            await self.cerrar_orden_real(symbol, precio_cierre, cantidad, exito=False, motivo="Cambio de tendencia")
            tendencia_str, _ = detectar_tendencia(symbol, df)
            self.ultima_tendencia[symbol] = tendencia_str
            return

        if not validar_dataframe(df, ["high", "low", "close"]):
            log.warning(f"⚠️ DataFrame inválido para salida personalizada en {symbol}.")
            return

        resultado_salida = evaluar_salidas(orden, df, config=config_actual)
        if resultado_salida.get("cerrar", False):
            razon = resultado_salida.get("razon", "Estrategia desconocida")
            tendencia_actual, _ = detectar_tendencia(symbol, df)
            evaluacion = evaluar_estrategias(symbol, df, tendencia_actual)
            estrategias_activas = evaluacion.get("estrategias_activas", {})
            puntaje = evaluacion.get("puntaje_total", 0)
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            config_actual = self.config_por_simbolo.get(symbol, {})
            umbral = calcular_umbral_adaptativo(symbol, df, estrategias_activas, pesos_symbol, config=config_actual)

            if not validar_necesidad_de_salida(df, orden, estrategias_activas, puntaje=puntaje, umbral=umbral, config=config_actual):
                log.info(f"❌ Cierre por '{razon}' evitado: condiciones técnicas aún válidas.")
                return
            log.info(f"🚪 Estrategia de salida activada en {symbol} → {razon}")
            await self.cerrar_orden_real(
                symbol,
                precio_cierre,
                cantidad,
                exito="profit" in razon.lower(),
                motivo=f"Estrategia: {razon}"
            )



    async def cerrar_orden_real(self, symbol, precio, cantidad, exito, motivo="Desconocido"):
        try:
            crypto = symbol.split("/")[0]
            balance = self.cliente.fetch_balance()
            disponible = balance['free'].get(crypto, 0)
            cantidad_real = min(disponible, cantidad)

            if cantidad_real <= 0:
                log.warning(f"⚠️ No hay saldo disponible para cerrar {symbol}. Se omite venta.")
                return

            log.info(f"📤 Vendiendo {cantidad_real} {crypto} en {symbol} a mercado ({precio:.2f}) [Motivo: {motivo}]")
            orden = self.cliente.create_market_sell_order(symbol.replace("/", ""), cantidad_real)

            if symbol not in self.ordenes_abiertas:
                log.warning(f"⚠️ Orden cerrada en {symbol} no registrada. No se guardará.")
                return

            info = self.ordenes_abiertas[symbol].copy()
            precio_entrada = info.get("precio_entrada", 0)
            retorno_total = round((precio - precio_entrada) / precio_entrada, 6) if precio_entrada else 0.0
            resultado_trade = "ganancia" if retorno_total > 0 else "pérdida"
            log.info(f"📈 Resultado de {symbol}: {resultado_trade.upper()} con retorno {retorno_total*100:.2f}%")


            if retorno_total < 0:
                actualizar_perdida(symbol, abs(retorno_total * precio_entrada))


            info.update({
                "precio_cierre": precio,
                "motivo_cierre": motivo,
                "fecha_cierre": datetime.utcnow().isoformat(),
                "retorno_total": retorno_total
            })
            self.guardar_orden_real(info.copy())
            registrar_resultado_trade(symbol, info, retorno_total)
            guardar_operacion_en_csv(symbol, info)

            motivo_normalizado = motivo.lower().strip()
            self.historial_cierres[symbol] = {
                "timestamp": datetime.utcnow().isoformat(),
                "motivo": motivo_normalizado
            }

            if motivo.lower() == "stop loss":
                self.ultimo_stop_loss[symbol] = datetime.utcnow()

            actualizar_pesos_estrategias_symbol(symbol)
            self.pesos_por_simbolo = cargar_pesos_estrategias()
            ordenes_reales.eliminar_orden(symbol)
            self.ordenes_abiertas = ordenes_reales.obtener_todas_las_ordenes()

            if motivo.lower() in ["cambio de tendencia", "estrategia: cambio de tendencia"]:
                tendencia_cierre, _ = detectar_tendencia(symbol, pd.DataFrame(self.buffer.get(symbol, [])))
                self.ultima_tendencia[symbol] = tendencia_cierre

            log.info(f"{'✔️' if exito else '⚠️'} ORDEN CERRADA para {symbol} a {precio:.2f} | Motivo: {motivo}")

        except Exception as e:
            log.error(f"❌ Error al cerrar orden para {symbol}: {e}")

    def guardar_orden_real(self, orden):
        symbol = orden.get("symbol", "desconocido")
        archivo = symbol.replace("/", "_").lower() + ".parquet"
        ruta = os.path.join("ordenes_reales", archivo)

        os.makedirs("ordenes_reales", exist_ok=True)

        if isinstance(orden.get("timestamp"), (pd.Timestamp, datetime)):
            orden["timestamp"] = orden["timestamp"].timestamp()

        try:
            ordenes_actuales = []
            if os.path.exists(ruta):
                try:
                    df = pd.read_parquet(ruta)
                    ordenes_actuales = df.to_dict("records")
                except Exception as carga_error:
                    backup_path = ruta.replace(".parquet", f"_corrupto_{int(datetime.now().timestamp())}.parquet")
                    os.rename(ruta, backup_path)
                    log.warning(f"⚠️ Archivo corrupto renombrado a: {backup_path}")
                    ordenes_actuales = []

            ordenes_actuales.append(orden)

            pd.DataFrame(ordenes_actuales).to_parquet(ruta, index=False)

            log.info(f"💾 Orden REAL registrada en {ruta}")

        except Exception as e:
            log.error(f"❌ Error guardando orden real: {e}")


    async def ejecutar(self):
        self.tarea_estado = asyncio.create_task(monitorear_estado_periodicamente(self))
        self.tareas_activas = []
        for symbol in self.symbols:
            async def tarea_simb(symbol=symbol):
                try:
                    await escuchar_velas(symbol, INTERVALO_VELAS, self.procesar_vela)
                except Exception as e:
                    log.error(f"❌ Error en escucha de {symbol}: {e}")
            self.tareas_activas.append(asyncio.create_task(tarea_simb()))
        log.info("🎧 Escuchando velas...")
        await asyncio.gather(*self.tareas_activas, self.tarea_estado)


    async def cerrar(self):
        log.info("🛑 Deteniendo tareas...")
        if hasattr(self, "tarea_estado"):
            self.tarea_estado.cancel()
            try:
                await self.tarea_estado
            except asyncio.CancelledError:
                log.info("🛑 Estado cancelado.")
        for tarea in getattr(self, "tareas_activas", []):
            tarea.cancel()
            try:
                await tarea
            except asyncio.CancelledError:
                log.info("🛑 Tarea cancelada.")



    









