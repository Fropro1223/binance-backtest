import pandas as pd
import os

def check_tsl_stats():
    # Load all trades from the last run if possible, 
    # but our main.py doesn't save to CSV by default, it logs to Sheets.
    # However, I can run a small backtest on one pair and print types.
    print("Verifying TSL exit types on a single pair...")
    
    from backtest_framework import BacktestEngine
    from conditions.vectorized_strategy import VectorizedStrategy
    
    engine = BacktestEngine(data_dir="data/processed")
    
    # Run on one pair manually
    results = engine.run(
        VectorizedStrategy,
        side="SHORT",
        tp=0.1,
        sl=0.1,
        tsl=0.015,
        pump_threshold=0.02,
        marubozu_threshold=0.8,
        ema="none",
        workers=1,
        parallel=False,
        tf_filter="30s" # use a common TF
    )
    
    if not results.empty:
        counts = results['type'].value_counts()
        print("\nExit Type Counts:")
        print(counts)
        if "TSL" in counts:
            print("\n✅ SUCCESS: Trailing Stop Loss (TSL) triggered!")
        else:
            print("\n❌ FAILURE: No TSL triggers found. Check logic or data.")
    else:
        print("No trades found for verification.")

if __name__ == "__main__":
    check_tsl_stats()
