import sys
import os
import pandas as pd
import time

# Add current directory
sys.path.append(os.getcwd())

from backtest_framework import BacktestEngine
# from conditions.ema_chain import EmaChainConditions
# from actions import evaluate_action
from strategies.polars_ema_chain import PolarsEmaChain
from sheets import log_analysis_to_sheet

DATA_ROOT = os.path.join(os.getcwd(), "data", "processed")


def generate_configs():
    configs = []
    
    sides = ["SHORT", "LONG"]
    trends = ["BULLISH", "BEARISH"] # "NONE" can be added if needed
    pumps = [0.02, 0.05] # 2% and 5% pump thresholds
    
    tp_sl_pairs = [
        (0.04, 0.04, "Symmetric"),
        (0.04, 0.08, "WideStop"),
        (0.07, 0.03, "HighRR"),
        (0.02, 0.02, "Scalp"),
        (0.10, 0.05, "Yolo"),
    ]
    
    for side in sides:
        for trend in trends:
            for pump in pumps:
                for tp, sl, desc in tp_sl_pairs:
                    # Strategy Name Construction
                    # e.g. [SHORT] [BULLISH] [Pump:2%] Symmetric
                    name = f"[{side}] [{trend}] [Pump:{int(pump*100)}%] {desc} (TP:{int(tp*100)}/SL:{int(sl*100)})"
                    
                    cfg = {
                        "side": side,
                        "trend": trend,
                        "pump": pump,
                        "tp": tp,
                        "sl": sl,
                        "name": name
                    }
                    configs.append(cfg)
    
    return configs

def run_batch():
    if not os.path.exists(DATA_ROOT):
        print(f"❌ Data path not found: {DATA_ROOT}")
        return

    engine = BacktestEngine(data_dir=DATA_ROOT)
    
    configs = generate_configs()
    # Optional: Shuffle or sort? Sequential is better for comparison.
    # Total: 2 * 2 * 2 * 5 = 40 variations. 
    # Let's add NONE trend to get more? 
    # If we add NONE: 2 * 3 * 2 * 5 = 60 variations.
    # Let's stick to 40 for now, or add NONE for specific cases.
    # User asked for "50 tane". Let's add 'NONE' trend for just one Pump setting (2%)?
    
    # Adding extra variations for 'NONE' trend (Pure Pump)
    for side in ["SHORT", "LONG"]:
        for tp, sl, desc in [(0.04, 0.04, "Symmetric"), (0.07, 0.03, "HighRR")]:
             name = f"[{side}] [NO_EMA] [Pump:2%] {desc} (TP:{int(tp*100)}/SL:{int(sl*100)})"
             configs.append({
                "side": side,
                "trend": "NONE",
                "pump": 0.02,
                "tp": tp,
                "sl": sl,
                "name": name
             })
             
    print(f"🚀 Starting Grand Batch Backtest - {len(configs)} Variations...")
    print("-------------------------------------------------------------")
    
    for i, cfg in enumerate(configs):
        print(f"\n👉 [{i+1}/{len(configs)}] Running: {cfg['name']} ...")
        
        try:
            # PolarsEmaChain logic includes the pump threshold hardcoded?
            # Wait, I need to check if PolarsEmaChain supports variable pump threshold.
            # I forgot to refactor Pump Threshold in PolarsEmaChain!
            # It was hardcoded at 0.02.
            # I will pass 'pump_threshold' to kwargs.
            
            results = engine.run(
                PolarsEmaChain,
                max_positions=1,
                avg_threshold=0.0,
                tp=cfg['tp'],
                sl=cfg['sl'],
                bet_size=10.0,
                side=cfg['side'],
                parallel=True,
                check_current_candle=False,
                workers=8,
                # kwargs for Strategy
                trend=cfg['trend'],
                pump_threshold=cfg['pump']
            )
            
            if results.empty:
                print("   ⚠️ No trades.")
                continue
                
            # Stats
            total_trades = len(results)
            wins = len(results[results['pnl_usd'] > 0])
            total_pnl = results['pnl_usd'].sum()
            win_rate = (wins/total_trades)*100 if total_trades > 0 else 0
            
            print(f"   ✅ Trades: {total_trades}, PnL: ${total_pnl:.2f}, WR: {win_rate:.1f}%")
            
            # Prepare Sheet Data
            results['entry_time'] = pd.to_datetime(results['entry_time'])
            start_date = results['entry_time'].min().strftime('%d.%m')
            end_date = results['entry_time'].max().strftime('%d.%m')
            date_range = f"({start_date}-{end_date})"
            
            weekly_stats = []
            if not results.empty:
                weekly_groups = results.set_index('entry_time').resample('W-MON')
                for w_end, group in weekly_groups:
                    w_start_ts = w_end - pd.Timedelta(days=6)
                    label = f"{w_start_ts.strftime('%d.%m')}-{w_end.strftime('%d.%m')}"
                    if not group.empty:
                        weekly_stats.append({
                            'label': label,
                            'trades': len(group),
                            'pnl': group['pnl_usd'].sum()
                        })

            summary = {
                'strategy_name': cfg['name'],
                'win_rate': win_rate,
                'total_trades': total_trades,
                'total_pnl': total_pnl,
                'date_range': date_range,
                'weekly_stats': weekly_stats
            }
            
            log_analysis_to_sheet(summary)
            
            # Quota Safety Sleep
            time.sleep(10)
            
        except Exception as e:
            print(f"❌ Failed run {i+1}: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5) # Wait on error too

if __name__ == "__main__":
    run_batch()
