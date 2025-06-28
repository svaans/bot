use pyo3::prelude::*;
use pyo3::types::PyDict;

const BUFFER_INICIAL: usize = 120;

#[pyfunction]
#[pyo3(signature = (symbols, dataset_path))]
fn run_backtest(py: Python<'_>, symbols: Vec<String>, dataset_path: &str) -> PyResult<Py<PyDict>> {
    let pandas = py.import("pandas")?;
    let asyncio = py.import("asyncio")?;
    let config_mod = py.import("config.config_manager")?;
    let backtest_mod = py.import("backtesting.backtest")?;

    let cfg_cls = config_mod.getattr("Config")?;
    let trader_cls = backtest_mod.getattr("BacktestTrader")?;

    // Cargar datos
    let mut data = Vec::new();
    for sym in &symbols {
        let file = format!("{}/{}_1m.parquet", dataset_path, sym.replace("/", "_").to_lowercase());
        if let Ok(df) = pandas.call_method1("read_parquet", (file,)) {
            let df = df.call_method0("dropna")?.call_method1("sort_values", ("timestamp",))?;
            let df = df.call_method0("reset_index")?.call_method1("drop", (true,))?;
            let len: usize = df.getattr("__len__")?.extract()?;
            data.push((sym.clone(), df.into_py(py), len));
        }
    }
    if data.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("no data"));
    }

    let max_len = data.iter().map(|(_,_,l)| *l).max().unwrap_or(0);

    let cfg_kwargs = PyDict::new(py);
    cfg_kwargs.set_item("api_key", "")?;
    cfg_kwargs.set_item("api_secret", "")?;
    cfg_kwargs.set_item("modo_real", false)?;
    cfg_kwargs.set_item("intervalo_velas", "1m")?;
    cfg_kwargs.set_item("symbols", symbols.clone())?;
    cfg_kwargs.set_item("umbral_riesgo_diario", 0.03)?;
    cfg_kwargs.set_item("min_order_eur", 10.0)?;
    cfg_kwargs.set_item("persistencia_minima", 1)?;
    let config = cfg_cls.call((), Some(cfg_kwargs))?;
    let trader = trader_cls.call1((config,))?;
    trader.call_method1("_precargar_historico", (BUFFER_INICIAL,))?;

    let loop_obj = asyncio.call_method0("new_event_loop")?;
    asyncio.call_method1("set_event_loop", (loop_obj.clone_ref(py),))?;

    for i in BUFFER_INICIAL..max_len {
        for (sym, df, len) in &data {
            if i >= *len { continue; }
            let row = df.as_ref(py).call_method1("__getitem__", (i,))?;
            let vela = PyDict::new(py);
            vela.set_item("symbol", sym)?;
            vela.set_item("timestamp", row.get_item("timestamp")?)?;
            vela.set_item("open", row.get_item("open")?)?;
            vela.set_item("high", row.get_item("high")?)?;
            vela.set_item("low", row.get_item("low")?)?;
            vela.set_item("close", row.get_item("close")?)?;
            vela.set_item("volume", row.get_item("volume")?)?;
            let coro = trader.call_method1("_procesar_vela", (vela,))?;
            loop_obj.call_method1("run_until_complete", (coro,))?;
        }
    }

    let coro = trader.call_method0("cerrar")?;
    loop_obj.call_method1("run_until_complete", (coro,))?;
    loop_obj.call_method0("close")?;

    let resultados = trader.getattr("resultados")?;
    let resumen = PyDict::new(py);
    resumen.set_item("resultados", resultados)?;
    Ok(resumen.into())
}

#[pymodule]
fn rust_backtesting(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_backtest, m)?)?;
    Ok(())
}