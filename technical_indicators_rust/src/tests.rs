use crate::indicators::*;
use crate::batch::{Candle, process_batch};

#[test]
fn test_sma() {
    let data = [1.0,2.0,3.0,4.0,5.0];
    assert!((sma(&data, 3) - 4.0).abs() < 1e-6);
}

#[test]
fn test_ema() {
    let data = [1.0,2.0,3.0,4.0,5.0];
    let v = ema(&data, 3);
    assert!(v > 0.0);
}

#[test]
fn test_rsi() {
    let close = [1.0,2.0,3.0,4.0,5.0,6.0];
    let r = rsi(&close, 5);
    assert!(r > 50.0);
}

#[test]
fn test_atr() {
    let high = [2.0,3.0,4.0,5.0,6.0];
    let low = [1.0,2.0,3.0,4.0,5.0];
    let close = [1.5,2.5,3.5,4.5,5.5];
    let a = atr(&high, &low, &close, 3);
    assert!(a > 0.0);
}

#[test]
fn test_process_batch() {
    let candles = [
        Candle { open:1.0, high:2.0, low:0.5, close:1.5, volume:100.0 },
        Candle { open:1.5, high:2.5, low:1.0, close:2.0, volume:120.0 },
        Candle { open:2.0, high:3.0, low:1.5, close:2.5, volume:110.0 },
        Candle { open:2.5, high:3.5, low:2.0, close:3.0, volume:130.0 },
        Candle { open:3.0, high:4.0, low:2.5, close:3.5, volume:140.0 },
    ];
    let summary = process_batch(&candles);
    assert!(summary.rsi >= 0.0 && summary.rsi <= 100.0);
}