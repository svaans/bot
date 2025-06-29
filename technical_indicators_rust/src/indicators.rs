pub fn sma(values: &[f64], period: usize) -> f64 {
    if period == 0 || values.len() < period {
        return f64::NAN;
    }
    let slice = &values[values.len()-period..];
    let sum: f64 = slice.iter().sum();
    sum / period as f64
}

pub fn ema(values: &[f64], period: usize) -> f64 {
    if values.is_empty() { return f64::NAN; }
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut ema = values[0];
    for &v in &values[1..] {
        ema = (1.0 - alpha) * ema + alpha * v;
    }
    ema
}

pub fn rsi(close: &[f64], period: usize) -> f64 {
    if close.len() < period + 1 || period == 0 {
        return f64::NAN;
    }
    let start = close.len() - period;
    let mut gain = 0.0;
    let mut loss = 0.0;
    for i in start..close.len() {
        let diff = close[i] - close[i-1];
        if diff > 0.0 {
            gain += diff;
        } else {
            loss -= diff;
        }
    }
    let avg_gain = gain / period as f64;
    let avg_loss = loss / period as f64;
    if avg_loss == 0.0 { return 100.0; }
    let rs = avg_gain / avg_loss;
    100.0 - 100.0 / (1.0 + rs)
}

pub fn momentum(close: &[f64], period: usize) -> f64 {
    if close.len() < period + 1 || period == 0 { return f64::NAN; }
    let last = close[close.len()-1];
    let prev = close[close.len()-period-1];
    last - prev
}

pub fn volatility(close: &[f64], period: usize) -> f64 {
    if period == 0 || close.len() < period { return f64::NAN; }
    let slice = &close[close.len()-period..];
    let mean: f64 = slice.iter().sum::<f64>() / period as f64;
    let var: f64 = slice.iter().map(|v| (*v - mean).powi(2)).sum::<f64>() / period as f64;
    var.sqrt()
}

pub fn atr(high: &[f64], low: &[f64], close: &[f64], period: usize) -> f64 {
    let n = high.len();
    if period == 0 || n != low.len() || n != close.len() || n < period + 1 {
        return f64::NAN;
    }
    let mut prev_close = close[0];
    let mut atr = 0.0;
    for i in 1..=period {
        let tr = (high[i] - low[i])
            .max((high[i] - prev_close).abs())
            .max((low[i] - prev_close).abs());
        atr += tr;
        prev_close = close[i];
    }
    atr /= period as f64;
    for i in period+1..n {
        let tr = (high[i] - low[i])
            .max((high[i] - prev_close).abs())
            .max((low[i] - prev_close).abs());
        atr = (atr * (period as f64 - 1.0) + tr) / period as f64;
        prev_close = close[i];
    }
    atr
}