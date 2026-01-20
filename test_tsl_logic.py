import numpy as np
import pandas as pd
import polars as pl
from backtest_framework import process_single_pair_polars

class MockStrategy:
    def __init__(self, tsl=0.02, side="LONG"):
        self.tsl = tsl
        self.side = side
        self.tp = 1.0 # Very high
        self.sl = 1.0 # Very high

def test_tsl_math():
    print("=== TSL MATHEMATICAL VERIFICATION ===")
    
    # 1. LONG TEST: Entry at 100. Price goes up to 110, then drops.
    # TSL at 2% means if it drops 2% from 110 (110 * 0.98 = 107.8), it should exit.
    data_long = {
        "ts_1s": [1, 2, 3, 4, 5],
        "open":  [100, 105, 110, 109, 107],
        "high":  [100, 106, 111, 109, 107],
        "low":   [99,  105, 109, 108, 106],
        "close": [100, 106, 110, 108, 106],
        "entry_signal": [True, False, False, False, False]
    }
    df_long = pl.DataFrame(data_long)
    
    # 2. SHORT TEST: 
    # Candle 1: 100 -> 101 (Pump up, triggers signal for SHORT)
    # Candle 2: 101 -> 95 (Price drops)
    # Candle 3: 95 -> 89 (Price drops further, Best Low = 89)
    # Candle 4: 89 -> 92 (Price rises, hits TSL at 89 * 1.02 = 90.78)
    data_short = {
        "ts_1s": [1, 2, 3, 4, 5],
        "open":  [100, 101, 95, 89, 93],
        "high":  [101.5, 101, 95, 92, 94],
        "low":   [100, 95, 89, 89, 92],
        "close": [101, 95, 89, 92, 93],
        "entry_signal": [True, False, False, False, False]
    }
    df_short = pl.DataFrame(data_short)

    def run_mock_bt(df, side, tsl):
        df.write_parquet("tsl_test_temp.parquet")
        from conditions.vectorized_strategy import VectorizedStrategy
        kwargs = {
            "side": side,
            "tsl": tsl,
            "tp": 0.5,
            "sl": 0.5,
            "pump_threshold": 0.001,  # Relaxed
            "marubozu_threshold": 0.1 # Relaxed
        }
        from backtest_framework import process_single_pair_polars
        trades = process_single_pair_polars(("tsl_test_temp.parquet", VectorizedStrategy, False, kwargs))
        return trades

    print("\n--- Testing LONG TSL (Trigger expected at 108.78) ---")
    trades_l = run_mock_bt(df_long, "LONG", 0.02)
    if not trades_l:
        print("❌ No trades found in LONG test!")
    for t in trades_l:
        print(f"Exit Type: {t.type}, Exit Price: {t.exit_price}")
        # best high so far at candle 3 was 111. 
        # tsl_trigger = 111 * (1 - 0.02) = 111 * 0.98 = 108.78
        expected = 111 * 0.98
        print(f"Mathematical Expected: {expected}")
        if abs(t.exit_price - expected) < 0.001:
            print("✅ LONG TSL Accuracy Correct!")
        else:
            print(f"❌ LONG TSL Accuracy Mismatch! Diff: {abs(t.exit_price - expected)}")

    print("\n--- Testing SHORT TSL (Trigger expected at 90.78) ---")
    trades_s = run_mock_bt(df_short, "SHORT", 0.02)
    if not trades_s:
        print("❌ No trades found in SHORT test!")
    for t in trades_s:
        print(f"Exit Type: {t.type}, Exit Price: {t.exit_price}")
        # best low so far at candle 3 was 89. 
        # tsl_trigger = 89 * (1 + 0.02) = 89 * 1.02 = 90.78
        expected = 89 * 1.02
        print(f"Mathematical Expected: {expected}")
        if abs(t.exit_price - expected) < 0.001:
            print("✅ SHORT TSL Accuracy Correct!")
        else:
            print(f"❌ SHORT TSL Accuracy Mismatch! Diff: {abs(t.exit_price - expected)}")

if __name__ == "__main__":
    test_tsl_math()
