import sys
import os
import pandas as pd
import time

# Add current directory
sys.path.append(os.getcwd())

from backtest_framework import BacktestEngine
from conditions.ema_chain import EmaChainConditions
from actions import evaluate_action
from sheets import log_analysis_to_sheet

DATA_ROOT = os.path.join(os.getcwd(), "data", "processed")

VALID_CONFIGS = [
    # 1. 4% TP / 4% SL (Symmetric)
    {"tp": 0.04, "sl": 0.04, "desc": "Symmetric"},
    
    # 2. 4% TP / 8% SL (Wide Stop - Survive the Wick)
    {"tp": 0.04, "sl": 0.08, "desc": "Wide Stop"},
    
    # 3. 7% TP / 3% SL (Snipe the Crash - High R:R) (Requested as "3-7 stp -tp")
    {"tp": 0.07, "sl": 0.03, "desc": "High R:R"},
]

def run_batch():
    if not os.path.exists(DATA_ROOT):
        print(f"❌ Data path not found: {DATA_ROOT}")
        return

    engine = BacktestEngine(data_dir=DATA_ROOT)
    
    print(f"🚀 Starting Custom Batch (AllBull + Pump + Marubozu) - {len(VALID_CONFIGS)} Variations...")
    print("-------------------------------------------------------------")
    
    for i, cfg in enumerate(VALID_CONFIGS):
        tp = cfg['tp']
        sl = cfg['sl']
        desc = cfg['desc']
        
        # Strategy Name for Sheet
        strat_name = f"[AllBull+Pump+Maru] SHORT ({desc}) TP:{tp*100:.1f}% SL:{sl*100:.1f}%"
        
        print(f"\n👉 [{i+1}/{len(VALID_CONFIGS)}] Running: {strat_name} ...")
        
        try:
            results = engine.run(
                EmaChainConditions,           # Condition Class
                action_func=evaluate_action,  # Action Function
                max_positions=1,
                avg_threshold=0.0,
                tp=tp,
                sl=sl,
                bet_size=7.0,
                side="SHORT",
                parallel=True,
                check_current_candle=False,
                workers=8
            )
            
            if results.empty:
                print("   ⚠️ No trades.")
                # We still might want to log '0' to sheet to keep track?
                # For now just skip.
                continue
                
            # Stats
            total_trades = len(results)
            wins = len(results[results['pnl_usd'] > 0])
            total_pnl = results['pnl_usd'].sum()
            win_rate = (wins/total_trades)*100 if total_trades > 0 else 0
            
            print(f"   ✅ Done. Trades: {total_trades}, PnL: ${total_pnl:.2f}, WR: {win_rate:.1f}%")
            
            # Prepare for Sheet
            # Weekly Stats
            results['entry_time'] = pd.to_datetime(results['entry_time'])
            
            # Use same standardized logic
            weekly_groups = results.set_index('entry_time').resample('W-MON')
            weekly_stats = []
            for w_end, group in weekly_groups:
                w_start_ts = w_end - pd.Timedelta(days=6)
                label = f"{w_start_ts.strftime('%d.%m')}-{w_end.strftime('%d.%m')}"
                
                if group.empty:
                    continue
                else:
                   weekly_stats.append({
                        'label': label,
                        'trades': len(group),
                        'pnl': group['pnl_usd'].sum()
                    })
                
            # Log
            summary = {
                'strategy_name': strat_name,
                'win_rate': win_rate,
                'total_trades': total_trades,
                'total_pnl': total_pnl,
                'date_range': date_range,
                'weekly_stats': weekly_stats
            }
            
            log_analysis_to_sheet(summary)
            
        except Exception as e:
            print(f"❌ Failed run {i+1}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    run_batch()
