use pyo3::prelude::*;
use pyo3::types::{PyDict, PyAny};

fn rolling_mean_last(values: &[f64], window: usize) -> Option<f64> {
    if values.len() < window { return None; }
    let slice = &values[values.len() - window..];
    let sum: f64 = slice.iter().sum();
    Some(sum / window as f64)
}

fn simple_sma(values: &[f64], window: usize) -> Vec<f64> {
    if values.len() < window { return vec![]; }
    let mut res = Vec::with_capacity(values.len() - window + 1);
    let mut sum: f64 = values[..window].iter().sum();
    res.push(sum / window as f64);
    for i in window..values.len() {
        sum += values[i] - values[i - window];
        res.push(sum / window as f64);
    }
    res
}

fn slope(values: &[f64]) -> f64 {
    let n = values.len();
    if n < 2 { return 0.0; }
    let n_f = n as f64;
    let sum_x: f64 = (0..n).map(|x| x as f64).sum();
    let sum_y: f64 = values.iter().sum();
    let sum_xy: f64 = (0..n).map(|i| i as f64 * values[i]).sum();
    let sum_x2: f64 = (0..n).map(|i| (i as f64).powi(2)).sum();
    let denom = n_f * sum_x2 - sum_x.powi(2);
    if denom == 0.0 { return 0.0; }
    (n_f * sum_xy - sum_x * sum_y) / denom
}

fn detectar_regimen(high: &[f64], low: &[f64], close: &[f64]) -> String {
    let n = close.len();
    let mut tr = Vec::with_capacity(n);
    for i in 0..n {
        let hl = high[i] - low[i];
        let hc = if i>0 { (high[i] - close[i-1]).abs() } else { 0.0 };
        let lc = if i>0 { (low[i] - close[i-1]).abs() } else { 0.0 };
        tr.push(hl.max(hc).max(lc));
    }
    let atr14 = rolling_mean_last(&tr, 14).unwrap_or(0.0);
    let cierre = close[n-1];
    let vol = if cierre != 0.0 { atr14 / cierre } else { 0.0 };

    let sma = simple_sma(close, 30);
    let slope_norm = if sma.is_empty() { 0.0 } else {
        let s = slope(&sma);
        let base = *sma.last().unwrap_or(&0.0);
        if base == 0.0 { 0.0 } else { s / base }
    };

    if vol > 0.02 { "alta_volatilidad".to_string() }
    else if slope_norm.abs() > 0.001 { "tendencial".to_string() }
    else { "lateral".to_string() }
}

#[pyfunction]
#[pyo3(signature = (symbol, df, config=None, capital_actual=None, precio_actual=None))]
fn calcular_tp_sl_adaptativos(py: Python<'_>, symbol: &str, df: &PyAny, config: Option<&PyDict>, capital_actual: Option<f64>, precio_actual: Option<f64>) -> PyResult<(f64, f64)> {
    let dict_any = df.call_method1("to_dict", ("list",))?;
    let dict: &PyDict = dict_any.downcast::<PyDict>()?;
    let high: Vec<f64> = dict.get_item("high").ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("missing high"))?.extract()?;
    let low: Vec<f64> = dict.get_item("low").ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("missing low"))?.extract()?;
    let close: Vec<f64> = dict.get_item("close").ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("missing close"))?.extract()?;
    let n = close.len();
    if n == 0 { return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("empty dataframe")); }

    let precio_actual = precio_actual.unwrap_or(close[n-1]);

    let mut tr = Vec::with_capacity(n);
    for i in 0..n {
        let hl = high[i] - low[i];
        let hc = if i>0 { (high[i] - close[i-1]).abs() } else { 0.0 };
        let lc = if i>0 { (low[i] - close[i-1]).abs() } else { 0.0 };
        tr.push(hl.max(hc).max(lc));
    }

    let regimen = detectar_regimen(&high, &low, &close);
    let ventana_atr = if regimen == "lateral" { 7usize } else { 14usize };
    let mut atr = rolling_mean_last(&tr, ventana_atr).unwrap_or(f64::NAN);
    if atr.is_nan() { atr = precio_actual * 0.01; }

    let mut multiplicador_sl = config.and_then(|c| c.get_item("sl_ratio").and_then(|v| v.extract().ok())).unwrap_or(1.5);
    let mut multiplicador_tp = config.and_then(|c| c.get_item("tp_ratio").and_then(|v| v.extract().ok())).unwrap_or(2.5);
    let sl_atr_min = config.and_then(|c| c.get_item("sl_atr_min").and_then(|v| v.extract().ok())).unwrap_or(1.0);

    if regimen == "lateral" {
        multiplicador_sl *= 0.8;
        multiplicador_tp *= 0.8;
    } else {
        multiplicador_sl *= 1.2;
        multiplicador_tp *= 1.2;
    }

    if let (Some(true), Some(cap)) = (config.and_then(|c| c.get_item("modo_capital_bajo").and_then(|v| v.extract().ok())), capital_actual) {
        if cap < 500.0 {
            let factor = 1.0 + (1.0 - cap / 500.0) * 0.2;
            multiplicador_tp *= factor;
            let reduc = 1.0 - (1.0 - cap / 500.0) * 0.1;
            multiplicador_sl *= reduc.max(0.5);
        }
    }

    let dist_sl = (atr * multiplicador_sl).max(atr * sl_atr_min);
    let mut sl = (precio_actual - dist_sl).round_to(6);
    let mut tp = (precio_actual + atr * multiplicador_tp).round_to(6);

    let ratio_min = config.and_then(|c| c.get_item("ratio_minimo_beneficio").and_then(|v| v.extract().ok())).unwrap_or(1.5);
    let riesgo = precio_actual - sl;
    let beneficio = tp - precio_actual;
    if riesgo > 0.0 && beneficio / riesgo < ratio_min {
        tp = (precio_actual + ratio_min * riesgo).round_to(6);
    }

    Ok((sl, tp))
}

trait RoundTo {
    fn round_to(self, dec: i32) -> f64;
}
impl RoundTo for f64 {
    fn round_to(self, dec: i32) -> f64 {
        let factor = 10f64.powi(dec);
        (self * factor).round() / factor
    }
}

#[pymodule]
fn fast_tp_sl(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calcular_tp_sl_adaptativos, m)?)?;
    Ok(())
}