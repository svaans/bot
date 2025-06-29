use pyo3::prelude::*;
use numpy::PyArray1;

fn atr_internal(high: &[f64], low: &[f64], close: &[f64], periodo: usize) -> Option<f64> {
    let n = high.len();
    if n != low.len() || n != close.len() || n < periodo + 1 { return None; }
    let mut tr = vec![0.0f64; n];
    tr[0] = high[0] - low[0];
    for i in 1..n {
        let tr1 = high[i] - low[i];
        let tr2 = (high[i] - close[i-1]).abs();
        let tr3 = (low[i] - close[i-1]).abs();
        tr[i] = tr1.max(tr2).max(tr3);
    }
    let alpha = 1.0f64 / periodo as f64;
    let mut ema = tr[0];
    for i in 1..n {
        ema = (1.0 - alpha) * ema + alpha * tr[i];
    }
    Some(ema)
}

fn rolling_min(values: &[f64], window: usize) -> Option<f64> {
    if values.len() < window { return None; }
    let slice = &values[values.len()-window..];
    let mut m = slice[0];
    for &v in slice.iter() {
        if v < m { m = v; }
    }
    Some(m)
}

fn rolling_max(values: &[f64], window: usize) -> Option<f64> {
    if values.len() < window { return None; }
    let slice = &values[values.len()-window..];
    let mut m = slice[0];
    for &v in slice.iter() {
        if v > m { m = v; }
    }
    Some(m)
}

fn get_f64(dict: Option<&PyDict>, key: &str, default: f64) -> f64 {
    dict.and_then(|d| d.get_item(key).and_then(|v| v.extract::<f64>().ok())).unwrap_or(default)
}

fn get_bool(dict: Option<&PyDict>, key: &str, default: bool) -> bool {
    dict.and_then(|d| d.get_item(key).and_then(|v| v.extract::<bool>().ok())).unwrap_or(default)
}

#[pyfunction]
#[pyo3(signature=(info, precio_actual, high, low, close, config=None))]
fn verificar_trailing_stop(
    info: &PyDict,
    precio_actual: f64,
    high: &PyArray1<f64>,
    low: &PyArray1<f64>,
    close: &PyArray1<f64>,
    config: Option<&PyDict>,
) -> PyResult<(bool, String)> {
    let entrada: f64 = info.get_item("precio_entrada").ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError,_>("precio_entrada missing"))?.extract()?;
    let mut max_price: f64 = info.get_item("max_price").map(|v| v.extract().unwrap_or(entrada)).unwrap_or(entrada);
    let buffer_pct = get_f64(config, "trailing_buffer", 0.0);
    if precio_actual > max_price * (1.0 + buffer_pct) {
        info.set_item("max_price", precio_actual)?;
        max_price = precio_actual;
    }
    let trailing_start_ratio = get_f64(config, "trailing_start_ratio", 1.015);
    let atr_mult = get_f64(config, "atr_multiplicador", 1.0);
    let usar_atr = get_bool(config, "trailing_por_atr", false);
    let atr = atr_internal(unsafe { high.as_slice()? }, unsafe { low.as_slice()? }, unsafe { close.as_slice()? }, 14);
    let atr = match atr { Some(v) => v, None => return Ok((false, "ATR no disponible".to_string())) };
    let trailing_trigger = entrada * trailing_start_ratio;
    if max_price >= trailing_trigger {
        let distancia_ratio = get_f64(config, "trailing_distance_ratio", 0.02);
        let mut trailing_dist = if usar_atr { atr * atr_mult } else { max_price * distancia_ratio };
        let atr_ratio = if precio_actual != 0.0 { atr / precio_actual } else { 0.0 };
        let low_vol = get_f64(config, "umbral_volatilidad_baja", 0.001);
        let dist_min_pct = get_f64(config, "trailing_dist_min_pct", 0.005);
        if atr_ratio < low_vol {
            if dist_min_pct <= 0.0 {
                return Ok((false, "Trailing desactivado por baja volatilidad".to_string()));
            }
            let dist_min = precio_actual * dist_min_pct;
            if trailing_dist < dist_min {
                trailing_dist = dist_min;
            }
        }
        let direccion: String = info.get_item("direccion").map(|v| v.extract().unwrap_or_else(|_| "long".to_string())).unwrap_or_else(|| "long".to_string());
        let mut trailing_stop = if matches!(direccion.as_str(), "long"|"compra") {
            max_price - trailing_dist
        } else {
            max_price + trailing_dist
        };
        if get_bool(config, "uso_trailing_technico", false) {
            let soporte = rolling_min(unsafe { low.as_slice()? }, 5);
            let resistencia = rolling_max(unsafe { high.as_slice()? }, 5);
            if matches!(direccion.as_str(), "long"|"compra") {
                if let Some(s) = soporte { if s > trailing_stop { trailing_stop = s; } }
            } else {
                if let Some(r) = resistencia { if r < trailing_stop { trailing_stop = r; } }
            }
        }
        if matches!(direccion.as_str(), "long"|"compra") {
            if precio_actual <= trailing_stop {
                return Ok((true, format!("Trailing Stop activado — Máximo: {:.2}, Límite: {:.2}, Precio actual: {:.2}", max_price, trailing_stop, precio_actual)));
            }
        } else {
            if precio_actual >= trailing_stop {
                return Ok((true, format!("Trailing Stop activado — Mínimo: {:.2}, Límite: {:.2}, Precio actual: {:.2}", max_price, trailing_stop, precio_actual)));
            }
        }
        return Ok((false, format!("Trailing supervisando — Máx {:.2}, Límite {:.2}", max_price, trailing_stop)));
    }
    Ok((false, "".to_string()))
}

#[pymodule]
fn trailing_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(verificar_trailing_stop, m)?)?;
    Ok(())
}