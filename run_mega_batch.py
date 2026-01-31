import sys
import os
import pandas as pd
import json
import time
from datetime import datetime
import itertools

import argparse

# Add project root to path
sys.path.append(os.getcwd())

from backtest_framework import BacktestEngine
from conditions.vectorized_strategy import VectorizedStrategy
from sheets import log_analysis_to_sheet

DATA_ROOT = os.path.join(os.getcwd(), "data", "processed")
PROGRESS_FILE = "mega_batch_progress.json"

def generate_combinations():
    """Generates all 36,000 unique combinations, prioritized."""
    sides = ["SHORT", "LONG"]
    # We map specific thresholds to conditions to avoid redundant work
    pos_thresholds = [1.0, 1.5, 2.0, 2.5, 3.0]
    neg_thresholds = [-1.0, -1.5, -2.0, -2.5, -3.0]
    
    emas = [
        "none",
        "all_bear", "all_bull", 
        "big_bear", "big_bull", 
        "small_bear", "small_bull", 
        "big_bear_small_bull", "big_bull_small_bear"
    ]
    tps = list(range(1, 11))
    sls = list(range(1, 11))
    tsls = [0, 1.0]

    # Prioritized Order: 
    # 1. SHORT - PUMP
    # 2. SHORT - DUMP
    # 3. LONG - PUMP
    # 4. LONG - DUMP
    
    prioritized_grid = []
    
    # Sequence: (Side, Cond, Thresholds)
    order = [
        ("SHORT", "pump", pos_thresholds),
        ("SHORT", "dump", neg_thresholds),
        ("LONG", "pump", pos_thresholds),
        ("LONG", "dump", neg_thresholds)
    ]
    
    for side, cond, thresh_list in order:
        sub_grid = list(itertools.product([side], [cond], thresh_list, emas, tps, sls, tsls))
        prioritized_grid.extend(sub_grid)
        
    # Total: 4 (Side/Cond pairs) * 5 (Thresholds) * 9 (EMA) * 10 (TP) * 10 (SL) * 2 (TSL) = 36,000
    return prioritized_grid

def get_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"completed_index": -1}

def save_progress(index):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({"completed_index": index, "last_updated": str(datetime.now())}, f)


def run_mega_batch():
    if not os.path.exists(DATA_ROOT):
        print(f"❌ Data path not found: {DATA_ROOT}")
        return

    engine = BacktestEngine(data_dir=DATA_ROOT)
    combinations = generate_combinations()
    total = len(combinations)
    progress = get_progress()
    start_idx = progress["completed_index"] + 1

    print(f"🚀 Starting Mega Batch: {total} combinations.")
    print(f"🔄 Resuming from index: {start_idx}")

    for i in range(start_idx, total):
        side, cond, thresh, ema, tp, sl, tsl = combinations[i]
        
        # Build Strategy Name for Log (Must match the regex in sheets.py)
        tsl_str = f"TSL:{tsl}%" if tsl > 0 else "TSL:OFF"
        # Ensure threshold label matches sheets.py expected format (positive for pump, negative for dump)
        cond_val_str = f"{cond.capitalize()}:{abs(thresh)}%"
        
        strat_name = f"[{side}] {cond.upper()} EMA:{ema.title()} {cond_val_str} TP:{float(tp)}% SL:{float(sl)}% {tsl_str} M:0.8"
        
        print(f"\n👉 [{i+1}/{total}] Running: {strat_name}")
        
        try:
            # Map parameters for engine.run
            # cond argument in engine.run handles the pump/dump logic in VectorizedStrategy
            results = engine.run(
                VectorizedStrategy,
                max_positions=1,
                avg_threshold=0.0,
                pump_threshold=abs(thresh)/100.0 if cond == "pump" else 0.02,
                dump_threshold=abs(thresh)/100.0 if cond == "dump" else 0.02,
                tp=tp/100.0,
                sl=sl/100.0,
                tsl=tsl/100.0,
                bet_size=7.0,
                side=side,
                cond=cond,
                ema=ema,
                parallel=True,
                workers=2
            )

            if results is None or results.empty:
                print("   ⚠️ No trades.")
                save_progress(i)
                continue

            # Calculate Stats
            total_trades = len(results)
            wins = len(results[results['pnl_usd'] > 0])
            win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
            total_pnl = results['pnl_usd'].sum()
            avg_pnl = results['pnl_usd'].mean()
            
            # Skip weekly stats
            date_range = f"({(pd.Timestamp.now() - pd.Timedelta(days=90)).strftime('%d.%m')}-{pd.Timestamp.now().strftime('%d.%m')})"
            weekly_stats = []
            
            # Timeframe Breakdown
            results['tf'] = results['symbol'].apply(lambda x: x.split('_')[-1] if '_' in x else 'Unknown')
            tf_groups = results.groupby('tf')
            tf_breakdown = {}
            for tf, group in tf_groups:
                tf_breakdown[tf] = {
                    'trades': len(group),
                    'pnl': group['pnl_usd'].sum()
                }

            # Prepare for Sheets
            summary_data = {
                'strategy_name': strat_name,
                'tp_pct': tp/100.0,
                'sl_pct': sl/100.0,
                'max_pos': 1,
                'avg_thresh': 0.0,
                'bet_size': 7.0,
                'total_trades': total_trades,
                'win_rate': win_rate,
                'total_pnl': total_pnl,
                'avg_pnl': avg_pnl,
                'best_trade': results['pnl_usd'].max(),
                'worst_trade': results['pnl_usd'].min(),
                'date_range': date_range,
                'weekly_stats': weekly_stats,
                'tf_breakdown': tf_breakdown,
                'total_days': 90
            }

            print(f"   ✅ Trades: {total_trades}, PnL: ${total_pnl:.2f}, WR: {win_rate:.1f}%")
            
            log_analysis_to_sheet(summary_data)
            save_progress(i)
            
            # API Limit protection
            time.sleep(1)

        except Exception as e:
            print(f"❌ Error on combination {i}: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run only 3 combinations for testing")
    args = parser.parse_args()
    
    if args.test:
        print("🧪 TEST MODE: Limited to 3 combinations.")
        # Monkey patch generate_combinations for test mode
        original_gen = generate_combinations
        generate_combinations = lambda: original_gen()[:3]
        
    run_mega_batch()
