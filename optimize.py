import sys
import os
import pandas as pd
from backtest_framework import BacktestEngine
from strategies.pump_short import PumpShortStrategy
from sheets import log_strategy_summary

# Add current directory to path
sys.path.append(os.getcwd())

DATA_ROOT = os.path.join(os.getcwd(), "data", "processed")

# Define the 10 Combinations (TP, SL)
# Aiming for a mix of 1:1, 1.5:1, and 2:1 Risk/Reward ratios
COMBINATIONS = [
    (0.03, 0.03), # Scalp 1:1
    (0.04, 0.04), # Standard 1:1
    (0.05, 0.05), # Mid 1:1
    (0.05, 0.03), # 1.6:1
    (0.06, 0.04), # 1.5:1
    (0.07, 0.03), # 2.3:1 (Aggressive Win)
    (0.08, 0.04), # 2:1
    (0.10, 0.05), # 2:1 Wide
    (0.03, 0.06), # Inverse 0.5:1 (High Win Rate attempt)
    (0.04, 0.08), # Inverse 0.5:1
]

def run_batch():
    if not os.path.exists(DATA_ROOT):
        print(f"❌ Data path not found: {DATA_ROOT}")
        return

    print("🚀 Initializing Engine (Loading Data)...")
    engine = BacktestEngine(data_dir=DATA_ROOT)
    
    print(f"📋 Starting Batch Test for {len(COMBINATIONS)} combinations...")
    
    for i, (tp, sl) in enumerate(COMBINATIONS, 1):
        print(f"\n[{i}/{len(COMBINATIONS)}] Testing TP: {tp*100}% | SL: {sl*100}%")
        
        try:
            # Run Strategy
            results = engine.run(
                PumpShortStrategy,
                max_positions=1,
                avg_threshold=0.0,
                pump_threshold=0.02, 
                tp=tp, 
                sl=sl, 
                bet_size=7.0
            )
            
            # Calculate Metrics
            total_trades = len(results)
            if total_trades == 0:
                print("   ⚠️ No trades found.")
                continue
                
            wins = len(results[results['pnl_usd'] > 0])
            losses = len(results[results['pnl_usd'] <= 0])
            win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
            total_pnl = results['pnl_usd'].sum()
            avg_pnl = results['pnl_usd'].mean()
            
            print(f"   => PnL: ${total_pnl:.2f} | WR: {win_rate:.2f}%")
            
            # Log to Sheets
            summary_data = {
                'strategy_name': f'Batch_Test_{i}',
                'tp_pct': tp,
                'sl_pct': sl,
                'max_pos': 1,
                'avg_thresh': 0.0,
                'bet_size': 7.0,
                'total_trades': total_trades,
                'win_rate': win_rate,
                'total_pnl': total_pnl,
                'avg_pnl': avg_pnl,
                'best_trade': results['pnl_usd'].max() if not results.empty else 0,
                'worst_trade': results['pnl_usd'].min() if not results.empty else 0
            }
            log_strategy_summary(summary_data)
            
        except Exception as e:
            print(f"   ❌ Error in run {i}: {e}")

    print("\n✅ Batch Optimization Complete!")

if __name__ == "__main__":
    run_batch()
