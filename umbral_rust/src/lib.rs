use pyo3::prelude::*;
use numpy::PyArray1;
use numpy::ToPyArray;
use ndarray::ArrayView1;
use ndarray::s;

fn mean(slice: ArrayView1<'_, f64>) -> f64 {
    if slice.is_empty() { return 0.0; }
    slice.sum() / slice.len() as f64
}

fn std(slice: ArrayView1<'_, f64>, mean_val: f64) -> f64 {
    if slice.is_empty() { return 0.0; }
    let mut var = 0.0;
    for v in slice.iter() {
        var += (*v - mean_val).powi(2);
    }
    (var / slice.len() as f64).sqrt()
}

fn slope(slice: ArrayView1<'_, f64>) -> f64 {
    let n = slice.len();
    if n < 2 { return 0.0; }
    let n_f = n as f64;
    let sum_x: f64 = (0..n).map(|x| x as f64).sum();
    let sum_y: f64 = slice.sum();
    let sum_xy: f64 = (0..n).map(|i| i as f64 * slice[i]).sum();
    let sum_x2: f64 = (0..n).map(|i| (i as f64).powi(2)).sum();
    let denom = n_f * sum_x2 - sum_x.powi(2);
    if denom == 0.0 { return 0.0; }
    (n_f * sum_xy - sum_x * sum_y) / denom
}

fn rsi_last(close: ArrayView1<'_, f64>, periodo: usize) -> f64 {
    let n = close.len();
    if n < periodo + 1 { return 50.0; }
    let alpha = 1.0f64 / periodo as f64;
    let mut prev_gain = std::f64::NAN;
    let mut prev_loss = std::f64::NAN;
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
    }
    if prev_loss == 0.0 { return 50.0; }
    let rs = prev_gain / prev_loss;
    100.0 - 100.0 / (1.0 + rs)
}

#[pyfunction]
fn limites_adaptativos(contexto_score: f64) -> (f64, f64) {
    let base_max = 10.0;
    let base_min = 1.0;
    let mut umbral_max = 5.0_f64.max((base_max + contexto_score).min(30.0));
    let mut umbral_min = 0.5_f64.max(base_min + contexto_score * 0.5);
    if umbral_max < umbral_min {
        umbral_max = umbral_min + 1.0;
    }
    (umbral_max, umbral_min)
}

#[pyfunction]
#[pyo3(signature = (close, high, low, volume))]
fn calcular_metricas<'py>(
    py: Python<'py>,
    close: &PyArray1<f64>,
    high: &PyArray1<f64>,
    low: &PyArray1<f64>,
    volume: &PyArray1<f64>,
) -> PyResult<&'py PyDict> {
    let c = close.as_array();
    let h = high.as_array();
    let l = low.as_array();
    let v = volume.as_array();
    let n_close = c.len();
    if n_close < 10 { return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("close too short")); }
    if h.len() < 10 || l.len() < 10 || v.len() < 30 { return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("array too short")); }

    let close_tail = c.slice(s![n_close-10..]);
    let high_tail = h.slice(s![h.len()-10..]);
    let low_tail = l.slice(s![l.len()-10..]);
    let vol_tail = v.slice(s![v.len()-30..]);

    let media_close = mean(close_tail);
    let (volatilidad, rango_medio) = if media_close == 0.0 || media_close.is_nan() {
        (0.0, 0.0)
    } else {
        let std_close = std(close_tail, media_close);
        let rango: ArrayView1<'_, f64> = high_tail - &low_tail;
        let rango_med = mean(rango) / media_close;
        (std_close / media_close, rango_med)
    };

    let volumen_promedio = mean(vol_tail);
    let volumen_max = vol_tail.iter().fold(0.0/0.0, |acc, &x| if acc.is_nan() || x>acc {x} else {acc});
    let volumen_relativo = if volumen_max == 0.0 || volumen_max.is_nan() {
        0.5
    } else {
        volumen_promedio / volumen_max
    };

    let mut cambios = Vec::new();
    if n_close >= 6 {
        for i in n_close-5..n_close {
            let prev = c[i-1];
            if prev != 0.0 {
                cambios.push((c[i] - prev) / prev);
            }
        }
    }
    let momentum_std = if cambios.is_empty() {
        0.0
    } else {
        let mean_c: f64 = cambios.iter().sum::<f64>() / cambios.len() as f64;
        let var: f64 = cambios.iter().map(|v| (*v - mean_c).powi(2)).sum::<f64>() / cambios.len() as f64;
        var.sqrt()
    };

    let slope_val = slope(close_tail);
    let rsi_val = rsi_last(c, 14);

    let dict = PyDict::new(py);
    dict.set_item("volatilidad", volatilidad)?;
    dict.set_item("rango_medio", rango_medio)?;
    dict.set_item("volumen_relativo", volumen_relativo)?;
    dict.set_item("momentum_std", momentum_std)?;
    dict.set_item("slope", slope_val)?;
    dict.set_item("rsi", rsi_val)?;
    Ok(dict)
}

#[pymodule]
fn umbral_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calcular_metricas, m)?)?;
    m.add_function(wrap_pyfunction!(limites_adaptativos, m)?)?;
    Ok(())
}