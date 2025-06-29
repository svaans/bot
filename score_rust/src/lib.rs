use pyo3::prelude::*;
use numpy::PyArray1;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Mutex;

static STATS: Lazy<Mutex<HashMap<String, (f64, f64)>>> = Lazy::new(|| Mutex::new(HashMap::new()));
const ALPHA: f64 = 0.1;

fn rolling_mean_last(values: &[f64], window: usize) -> Option<f64> {
    if values.len() < window { return None; }
    let slice = &values[values.len() - window..];
    Some(slice.iter().sum::<f64>() / window as f64)
}

fn simple_sma(values: &[f64], window: usize) -> Vec<f64> {
    if values.len() < window { return Vec::new(); }
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

fn detectar_regimen(high: &[f64], low: &[f64], close: &[f64]) -> &'static str {
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

    if vol > 0.02 { "alta_volatilidad" }
    else if slope_norm.abs() > 0.001 { "tendencial" }
    else { "lateral" }
}

fn norm(value: Option<f64>, min_val: f64, max_val: f64) -> f64 {
    let v = match value { Some(v) if !v.is_nan() => v, _ => return 0.5 };
    if max_val == min_val { return 0.5; }
    let mut n = (v - min_val) / (max_val - min_val);
    if n < 0.0 { n = 0.0; }
    if n > 1.0 { n = 1.0; }
    n
}

fn normalizar_por_symbol(symbol: &str, score: f64) -> f64 {
    let mut map = STATS.lock().unwrap();
    let entry = map.entry(symbol.to_string()).or_insert((score, 0.0));
    if (*entry).1 == 0.0 && entry.0 == score {
        return score;
    }
    let avg = entry.0 * (1.0 - ALPHA) + score * ALPHA;
    let std = entry.1 * (1.0 - ALPHA) + (score - avg).abs() * ALPHA;
    entry.0 = avg;
    entry.1 = std;
    if std == 0.0 { return 0.5; }
    let mut norm = (score - avg) / (2.0 * std) + 0.5;
    if norm < 0.0 { norm = 0.0; }
    if norm > 1.0 { norm = 1.0; }
    norm
}

#[pyfunction]
#[pyo3(
    signature = (
        high,
        low,
        close,
        rsi=None,
        momentum=None,
        slope_val=None,
        tendencia,
        symbol=None
    )
)]
fn calcular_score_tecnico(
    high: &PyArray1<f64>,
    low: &PyArray1<f64>,
    close: &PyArray1<f64>,
    rsi: Option<f64>,
    momentum: Option<f64>,
    slope_val: Option<f64>,
    tendencia: &str,
    symbol: Option<&str>,
) -> PyResult<f64> {
    let h = unsafe { high.as_slice()? };
    let l = unsafe { low.as_slice()? };
    let c = unsafe { close.as_slice()? };
    if h.len() != l.len() || h.len() != c.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("array length mismatch"));
    }
    let regimen = detectar_regimen(h, l, c);
    let (w_rsi, w_mom, w_slope, w_tend) = match regimen {
        "tendencial" => (1.0, 1.0, 1.2, 1.0),
        "lateral" => (1.2, 0.5, 0.8, 0.5),
        _ => (1.0, 0.5, 1.0, 1.0),
    };
    let total = w_rsi + w_mom + w_slope + w_tend;
    let mut score = 0.0;
    score += w_rsi * norm(rsi, 30.0, 70.0);
    score += w_mom * norm(momentum, -0.02, 0.02);
    score += w_slope * norm(slope_val, -0.001, 0.001);
    score += w_tend * if ["alcista", "bajista"].contains(&tendencia) { 1.0 } else { 0.5 };
    score /= total;
    if let Some(sym) = symbol {
        score = normalizar_por_symbol(sym, score);
    }
    let rounded = (score * 10000.0).round() / 10000.0;
    Ok(rounded)
}

#[pymodule]
fn score_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calcular_score_tecnico, m)?)?;
    Ok(())