import sys
import os
import pandas as pd
from backtest_framework import BacktestEngine
from conditions.vectorized_strategy import VectorizedStrategy

# Fake main args
class Args:
    pass

def main():
    data_dir = "data/processed"
    # Create engine
    engine = BacktestEngine(data_dir=data_dir)
    
    # We want to run on a subset. 
    # BacktestEngine.run finds files automatically.
    # Let's monkeypatch engine.run or just let it find files and we'll see if it's fast.
    # Actually, let's just run it. 3000 files in serial might be slow, but parallel should be fast.
    # We will filter files manually if needed, but BacktestEngine doesn't accept file list.
    # Let's verify if "BTCUSDT_5s" is processed.
    
    print("Running Debug Backtest on subset (via hacking run method or just standard run)...")
    
    # We can pass `tf_filter` to limit files?
    # Let's allow it to run normally but only for ONE timeframe to speed up?
    
    strategy_settings = {
        'side': 'SHORT',
        'ema': 'none',
        'pump_threshold': 0.02,
        'marubozu_threshold': 0.8,
        'tp': 0.05,
        'sl': 0.04
    }
    
    # Run in Serial Mode to see errors
    try:
        results = engine.run(
            VectorizedStrategy,
            max_positions=1,
            avg_threshold=0,
            parallel=False, # Serial for debugging
            check_current_candle=True,
            tf_filter="5s", # Only 5s files to speed up
            workers=None,
            **strategy_settings
        )
        
        print(f"Backtest Done. Trades: {len(results)}")
        if not results.empty:
            print(results.head())
        else:
            print("No trades found.")
            
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
