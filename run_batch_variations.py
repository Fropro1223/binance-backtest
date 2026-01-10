import sys
import os
import pandas as pd
import time

# Add current directory
sys.path.append(os.getcwd())

from backtest_framework import BacktestEngine
from strategies.marubozu_pump import MarubozuPumpStrategy
from sheets import log_analysis_to_sheet

DATA_ROOT = os.path.join(os.getcwd(), "data", "processed")

VALID_CONFIGS = [
    # --- REGIME A: MOMENTUM IGNITION (Pump > 3%) ---
    {"regime": "A", "pump": 0.03, "side": "LONG", "tp": 0.03, "sl": 0.02, "desc": "Scalp"},
    {"regime": "A", "pump": 0.03, "side": "LONG", "tp": 0.05, "sl": 0.025, "desc": "Day Trade"},
    {"regime": "A", "pump": 0.03, "side": "LONG", "tp": 0.09, "sl": 0.03, "desc": "Swing"},
    {"regime": "A", "pump": 0.03, "side": "LONG", "tp": 0.15, "sl": 0.04, "desc": "Runner"},
    {"regime": "A", "pump": 0.03, "side": "LONG", "tp": 0.04, "sl": 0.04, "desc": "Safe"},
    {"regime": "A", "pump": 0.03, "side": "SHORT", "tp": 0.02, "sl": 0.02, "desc": "Scalp"},
    {"regime": "A", "pump": 0.03, "side": "SHORT", "tp": 0.04, "sl": 0.02, "desc": "Fade"},
    {"regime": "A", "pump": 0.03, "side": "SHORT", "tp": 0.06, "sl": 0.03, "desc": "Deep Fade"},

    # --- REGIME B: EXPANSION PHASE (Pump > 6%) ---
    {"regime": "B", "pump": 0.06, "side": "LONG", "tp": 0.06, "sl": 0.03, "desc": "Momentum"},
    {"regime": "B", "pump": 0.06, "side": "LONG", "tp": 0.12, "sl": 0.04, "desc": "Aggressive"},
    {"regime": "B", "pump": 0.06, "side": "LONG", "tp": 0.18, "sl": 0.05, "desc": "Moonshot"},
    {"regime": "B", "pump": 0.06, "side": "LONG", "tp": 0.05, "sl": 0.05, "desc": "Conservative"},
    {"regime": "B", "pump": 0.06, "side": "LONG", "tp": 0.10, "sl": 0.06, "desc": "Wide Stop"},
    {"regime": "B", "pump": 0.06, "side": "SHORT", "tp": 0.03, "sl": 0.015, "desc": "Quick Reversal"},
    {"regime": "B", "pump": 0.06, "side": "SHORT", "tp": 0.06, "sl": 0.03, "desc": "Correction"},
    {"regime": "B", "pump": 0.06, "side": "SHORT", "tp": 0.12, "sl": 0.04, "desc": "Crash"},
    {"regime": "B", "pump": 0.06, "side": "SHORT", "tp": 0.05, "sl": 0.05, "desc": "Safety"},

    # --- REGIME C: CLIMAX / EXTREMES (Pump > 10%) ---
    {"regime": "C", "pump": 0.10, "side": "LONG", "tp": 0.05, "sl": 0.03, "desc": "Scalp Context"},
    {"regime": "C", "pump": 0.10, "side": "LONG", "tp": 0.20, "sl": 0.10, "desc": "Gamble"},
    {"regime": "C", "pump": 0.10, "side": "LONG", "tp": 0.50, "sl": 0.15, "desc": "YOLO"},
    {"regime": "C", "pump": 0.10, "side": "SHORT", "tp": 0.05, "sl": 0.02, "desc": "Sniper"},
    {"regime": "C", "pump": 0.10, "side": "SHORT", "tp": 0.10, "sl": 0.03, "desc": "Structural"},
    {"regime": "C", "pump": 0.10, "side": "SHORT", "tp": 0.20, "sl": 0.05, "desc": "Collapse"},
    {"regime": "C", "pump": 0.10, "side": "SHORT", "tp": 0.08, "sl": 0.04, "desc": "Balanced"},
    {"regime": "C", "pump": 0.10, "side": "SHORT", "tp": 0.10, "sl": 0.10, "desc": "Safe"},
]

def run_batch():
    if not os.path.exists(DATA_ROOT):
        print(f"❌ Data path not found: {DATA_ROOT}")
        return

    engine = BacktestEngine(data_dir=DATA_ROOT)
    
    print(f"🚀 Starting Grand Batch Matrix ({len(VALID_CONFIGS)} Configs)...")
    print("-------------------------------------------------------------")
    
    for i, cfg in enumerate(VALID_CONFIGS):
        regime = cfg['regime']
        pump_thresh = cfg['pump']
        side = cfg['side']
        tp = cfg['tp']
        sl = cfg['sl']
        desc = cfg['desc']
        
        # Strategy Name for Sheet
        # e.g. "Regime A [Pump 3%] LONG (Scalp) TP:3% SL:2%"
        strat_name = f"[{regime}] {side} ({desc}) Pump:{pump_thresh*100:.0f}% TP:{tp*100:.1f}% SL:{sl*100:.1f}%"
        
        print(f"\n👉 [{i+1}/{len(VALID_CONFIGS)}] Running: {strat_name} ...")
        
        try:
            results = engine.run(
                MarubozuPumpStrategy,
                max_positions=1,
                avg_threshold=0.0,
                pump_threshold=pump_thresh,     # Dynamic Pump
                marubozu_threshold=0.80,
                tp=tp,
                sl=sl,
                bet_size=7.0,
                side=side,
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
