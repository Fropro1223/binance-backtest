import sys
import os
import pandas as pd
import time

# Add project root to path
sys.path.append(os.getcwd())

from conditions.vectorized_strategy import VectorizedStrategy

def test_strategy():
    # Use a real file
    filepath = "data/processed/BTCUSDT_5s.parquet"
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return

    print(f"Loading {filepath}...")
    df = pd.read_parquet(filepath)
    print(f"Loaded {len(df)} rows.")

    # Init Strategy
    strategy = VectorizedStrategy(
        side="SHORT",
        ema="none",
        pump_threshold=0.001,
        marubozu_threshold=0.8,
        tp=0.05,
        sl=0.04
    )

    print("Running process_data...")
    start_time = time.time()
    
    # Run
    pl_df = strategy.process_data(df)
    
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.4f}s")

    if pl_df is None:
        print("Result: None (0 signals)")
    else:
        # Check signal count
        signal_count = pl_df['entry_signal'].sum()
        print(f"Signals Generated: {signal_count}")
        print(f"Signal %: {signal_count / len(df) * 100:.4f}%")
        
        # Determine if excessive
        if signal_count > 1000:
            print("⚠️ WARNING: Excessive signals! This will slow down backtester.")

if __name__ == "__main__":
    test_strategy()
