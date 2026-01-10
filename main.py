import sys
import os

# Add current directory to path so we can import modules
sys.path.append(os.getcwd())

from backtest_framework import BacktestEngine
from strategies.marubozu_pump import MarubozuPumpStrategy
from sheets import upload_to_sheets, log_strategy_summary
import pandas as pd

# Use local processed data
DATA_ROOT = os.path.join(os.getcwd(), "data", "processed")


def main():
    if not os.path.exists(DATA_ROOT):
        print(f"❌ Data path not found: {DATA_ROOT}")
        return

    # Initialize Engine
    engine = BacktestEngine(data_dir=DATA_ROOT)
    
    # Run Strategy
    # === CONFIGURATION (Edit here) ===
    MAX_POSITIONS = 1       # Single position per base pair (Pyramid disabled)
    AVG_THRESHOLD = 0.0     # Ignored when MAX_POSITIONS = 1
    SL_PCT = 0.04           # 4% Stop Loss
    TP_PCT = 0.04           # 4% Take Profit
    BET_SIZE = 7.0          # USDT per position

    # if AVG_THRESHOLD > SL_PCT:
    #     raise ValueError(f"⚠️ AVG_THRESHOLD ({AVG_THRESHOLD}) cannot be > SL_PCT ({SL_PCT})!")

    print("------------------------------------------------")
    print("🐇 STARTING MODULAR BACKTEST")
    print("Strategy: Marubozu Pump (Short on >2% Pump & 80% Body)")
    print(f"TP: {TP_PCT*100}% | SL: {SL_PCT*100}% | Bet: ${BET_SIZE}")
    print(f"Pyramid: Max {MAX_POSITIONS} | Gap {AVG_THRESHOLD*100}%")
    print("------------------------------------------------")
    
    # Pass strategy parameters here
    results = engine.run(
        MarubozuPumpStrategy,
        max_positions=MAX_POSITIONS,
        avg_threshold=AVG_THRESHOLD,
        pump_threshold=0.02,
        marubozu_threshold=0.80,  # 80% Body
        tp=TP_PCT, 
        sl=SL_PCT, 
        bet_size=BET_SIZE,
        side="SHORT", # <--- SHORT STRATEGY
        parallel=True,
        check_current_candle=False 
    )
    
    if results.empty:
        print("No trades generated.")
        return

    # Analysis
    print("\n📊 RESULTS")
    print("------------------------------------------------")
    
    total_trades = len(results)
    wins = len(results[results['pnl_usd'] > 0])
    losses = len(results[results['pnl_usd'] <= 0])
    win_rate = (wins / total_trades) * 100
    
    total_pnl = results['pnl_usd'].sum()
    avg_pnl = results['pnl_usd'].mean()
    
    print(f"Total Trades: {total_trades}")
    print(f"Win Rate:     {win_rate:.2f}% ({wins} W / {losses} L)")
    print(f"Total PnL:    ${total_pnl:.2f}")
    print(f"Avg PnL:      ${avg_pnl:.2f}")
    
    print("\n🏆 Top Winners:")
    print(results.groupby('symbol')['pnl_usd'].sum().sort_values(ascending=False).head(5))
    
    print("\n💀 Top Losers:")
    print(results.groupby('symbol')['pnl_usd'].sum().sort_values(ascending=False).tail(5))
    print("------------------------------------------------")

    # Save Results
    results_csv = "backtest_results_pump.csv"
    print(f"\n💾 Saving results to {results_csv}...")
    results.to_csv(results_csv, index=False)
    
    # --- WEEKLY BREAKDOWN LOGIC ---
    print("📅 Calculating Weekly Stats...")
    try:
        # Ensure entry_time is datetime
        results['entry_time'] = pd.to_datetime(results['entry_time'])
        
        # Sort by entry time
        results = results.sort_values('entry_time')
        
        # Calculate Overall Date Range
        start_date = results['entry_time'].min().strftime('%d.%m')
        end_date = results['entry_time'].max().strftime('%d.%m')
        overall_date_range = f"({start_date}-{end_date})"
        
        # Resample by Week (Starting Monday)
        # Using W-MON frequency
        weekly_groups = results.set_index('entry_time').resample('W-MON')
        
        weekly_stats = []
        for week_end, group in weekly_groups:
            # Even if group is empty, we might want to know? 
            # But sheet logic appends based on label.
            # CRITICAL FIX: Use the BIN edges for the label, not the data min/max.
            # W-MON means the index is the END of the week (Monday).
            # So Start is Week_End - 6 days.
            
            w_start_ts = week_end - pd.Timedelta(days=6)
            w_start = w_start_ts.strftime('%d.%m')
            w_end_str = week_end.strftime('%d.%m')
            
            label = f"{w_start}-{w_end_str}"
            
            if group.empty:
                trades_count = 0
                week_pnl = 0
                # Skip empty weeks to avoid clutter? 
                # Or keep for consistency? 
                # If we skip, we might miss alignment. 
                # Let's skip empty for now but ensure label is standard.
                continue
            else:
                trades_count = len(group)
                week_pnl = group['pnl_usd'].sum()
            
            weekly_stats.append({
                'label': label,
                'trades': trades_count,
                'pnl': week_pnl
            })
            
    except Exception as e:
        print(f"⚠️ Error calculating weekly stats: {e}")
        overall_date_range = ""
        weekly_stats = []

    # Log Summary to Analysis Sheet
    print("☁️  Logging Analysis to Google Sheets...")
    
    summary_data = {
        'strategy_name': f'Marubozu Pump [SHORT] TP:{TP_PCT*100:.1f}% SL:{SL_PCT*100:.1f}%',
        'tp_pct': TP_PCT,
        'sl_pct': SL_PCT,
        'max_pos': MAX_POSITIONS,
        'avg_thresh': AVG_THRESHOLD,
        'bet_size': BET_SIZE,
        'total_trades': total_trades,
        'win_rate': win_rate,
        'total_pnl': total_pnl,
        'avg_pnl': avg_pnl,
        'best_trade': results['pnl_usd'].max(),
        'worst_trade': results['pnl_usd'].min(),
        'date_range': overall_date_range, # NEW
        'weekly_stats': weekly_stats       # NEW
    }
    
    from sheets import log_analysis_to_sheet
    log_analysis_to_sheet(summary_data)


if __name__ == "__main__":
    main()
