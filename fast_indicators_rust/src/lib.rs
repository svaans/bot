use pyo3::prelude::*;
use numpy::{PyArray1, IntoPyArray};

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

fn rsi_internal(close: &[f64], periodo: usize) -> Vec<f64> {
    let n = close.len();
    if n < periodo + 1 { return Vec::new(); }
    let mut r = vec![f64::NAN; n];
    let alpha = 1.0f64 / periodo as f64;
    let mut prev_gain = f64::NAN;
    let mut prev_loss = f64::NAN;
    for i in 1..n {
        let change = close[i] - close[i-1];
        let gain = if change > 0.0 { change } else { 0.0 };
        let loss = if change < 0.0 { -change } else { 0.0 };
        if prev_gain.is_nan() {
            prev_gain = gain;
            prev_loss = loss;
        } else {
            prev_gain = (1.0 - alpha) * prev_gain + alpha * gain;
            prev_loss = (1.0 - alpha) * prev_loss + alpha * loss;
        }
        if i >= periodo {
            let rs = prev_gain / prev_loss;
            r[i] = 100.0 - 100.0 / (1.0 + rs);
        }
    }
    r
}

fn slope_internal(close: &[f64], periodo: usize) -> f64 {
    let n = close.len();
    if n < periodo || periodo < 2 { return 0.0; }
    let slice = &close[n - periodo..];
    let p = periodo as f64;
    let sum_x: f64 = (0..periodo).map(|i| i as f64).sum();
    let sum_y: f64 = slice.iter().sum();
    let sum_xy: f64 = (0..periodo).map(|i| i as f64 * slice[i]).sum();
    let sum_x2: f64 = (0..periodo).map(|i| (i as f64).powi(2)).sum();
    let denom = p * sum_x2 - sum_x.powi(2);
    if denom == 0.0 { return 0.0; }
    (p * sum_xy - sum_x * sum_y) / denom
}

#[pyfunction]
fn atr(py: Python<'_>, high: &PyArray1<f64>, low: &PyArray1<f64>, close: &PyArray1<f64>, periodo: usize) -> PyResult<f64> {
    let h = unsafe { high.as_slice()? };
    let l = unsafe { low.as_slice()? };
    let c = unsafe { close.as_slice()? };
    Ok(atr_internal(h, l, c, periodo).unwrap_or(f64::NAN))
}

#[pyfunction]
fn rsi(py: Python<'_>, close: &PyArray1<f64>, periodo: usize) -> PyResult<Py<PyArray1<f64>>> {
    let c = unsafe { close.as_slice()? };
    let result = rsi_internal(c, periodo);
    Ok(result.into_pyarray(py).to_owned())
}

#[pyfunction]
fn slope(_py: Python<'_>, close: &PyArray1<f64>, periodo: usize) -> PyResult<f64> {
    let c = unsafe { close.as_slice()? };
    Ok(slope_internal(c, periodo))
}

#[pymodule]
fn fast_indicators_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(atr, m)?)?;
    m.add_function(wrap_pyfunction!(rsi, m)?)?;
    m.add_function(wrap_pyfunction!(slope, m)?)?;
    Ok(())
}