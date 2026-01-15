
import pandas as pd
import numpy as np
import os
import sys

# Add path
sys.path.append(os.getcwd())

from conditions.ema_chain import EmaChainConditions
from conditions.marubozu_pump import MarubozuConditions

def debug_file(filepath):
    print(f"🔍 Debugging file: {filepath}")
    if not os.path.exists(filepath):
        print("❌ File not found.")
        return

    df = pd.read_parquet(filepath)
    print(f"📊 Loaded {len(df)} rows.")

    # 1. Initialize Strategies
    ema = EmaChainConditions()
    maru = MarubozuConditions(marubozu_threshold=0.80, pump_threshold=0.02)
    
    # 2. Prep Data (EMA)
    ema.prep_data(df)
    
    # 3. Counters
    counts = {
        'all_bull': 0,
        'is_pump': 0,
        'is_marubozu': 0,
        'COMBINED': 0
    }
    
    opens = df['open'].values
    highs = df['high'].values
    lows = df['low'].values
    closes = df['close'].values
    times = df.get('open_time', df.get('ts_1s')).values
    
    # 4. Iterate
    for i in range(len(df)):
        # Run EMA Condition
        ema.on_candle(None, opens[i], highs[i], lows[i], closes[i])
        
        # Run Marubozu Condition
        maru.on_candle(None, opens[i], highs[i], lows[i], closes[i])
        
        c_ema = ema.conditions
        c_maru = maru.conditions
        
        if c_ema.get('all_bull'): counts['all_bull'] += 1
        if c_maru.get('is_pump'): counts['is_pump'] += 1
        if c_maru.get('is_marubozu'): counts['is_marubozu'] += 1
        
        if c_ema.get('all_bull') and c_maru.get('is_pump') and c_maru.get('is_marubozu'):
            counts['COMBINED'] += 1
            # Print first hit details
            if counts['COMBINED'] == 1:
                 print(f"✅ First HIT at index {i} time {times[i]}")

    print("\n🧐 ANALYSIS REPORT:")
    print(f"Total Rows: {len(df)}")
    print(f"All Bull (EMA): {counts['all_bull']} ({counts['all_bull']/len(df)*100:.2f}%)")
    print(f"Pump > 2%:      {counts['is_pump']} ({counts['is_pump']/len(df)*100:.2f}%)")
    print(f"Marubozu > 0.8: {counts['is_marubozu']} ({counts['is_marubozu']/len(df)*100:.2f}%)")
    print(f"---------------------------")
    print(f"🔥 FINAL SIGNAL: {counts['COMBINED']}")

if __name__ == "__main__":
    # Pick a file that definitely exists
    # Listing directory first to find one
    data_dir = "data/processed"
    files = [f for f in os.listdir(data_dir) if f.endswith('.parquet')]
    if files:
        target = os.path.join(data_dir, files[0])
        debug_file(target)
    else:
        print("No files found in data/processed")
