use pyo3::prelude::*;
use numpy::PyArray1;

mod indicators;
mod batch;
use indicators::*;
use batch::{Candle, TechnicalSummary, process_batch};

#[pyfunction]
fn rsi_py(close: &PyArray1<f64>, period: usize) -> PyResult<f64> {
    let c = unsafe { close.as_slice()? };
    Ok(rsi(c, period))
}

#[pyfunction]
fn process_batch_py(candles: &PyArray1<Candle>) -> PyResult<TechnicalSummary> {
    let slice = unsafe { candles.as_slice()? };
    Ok(process_batch(slice))
}

#[pymodule]
fn technical_indicators_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rsi_py, m)?)?;
    m.add_class::<Candle>()?;
    m.add_class::<TechnicalSummary>()?;
    m.add_function(wrap_pyfunction!(process_batch_py, m)?)?;
    Ok(())
}