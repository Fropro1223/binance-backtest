import sys
import os

# Add current directory to path so we can import modules
sys.path.append(os.getcwd())

from backtest_framework import BacktestEngine
from strategies.pump_short import PumpShortStrategy
from sheets import upload_to_sheets
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
    SL_PCT = 0.06           # 6% Stop Loss
    TP_PCT = 0.06           # 6% Take Profit
    BET_SIZE = 7.0          # USDT per position

    # if AVG_THRESHOLD > SL_PCT:
    #     print(f"⚠️  WARNING: Avg Threshold ({AVG_THRESHOLD*100}%) > SL ({SL_PCT*100}%). 2nd position will likely never open!")

    print("------------------------------------------------")
    print("🐇 STARTING MODULAR BACKTEST")
    print("Strategy: PumpShort (Short on 2% Pump)")
    print(f"TP: {TP_PCT*100}% | SL: {SL_PCT*100}% | Bet: ${BET_SIZE}")
    print(f"Pyramid: Max {MAX_POSITIONS} | Gap {AVG_THRESHOLD*100}%")
    print("------------------------------------------------")
    
    # Pass strategy parameters here
    results = engine.run(
        PumpShortStrategy,
        max_positions=MAX_POSITIONS,
        avg_threshold=AVG_THRESHOLD,
        pump_threshold=0.02, 
        tp=TP_PCT, 
        sl=SL_PCT, 
        bet_size=BET_SIZE
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
    
    # Log Summary to Google Sheets (Comparison)
    print("☁️  Logging summary to Google Sheets...")
    
    summary_data = {
        'strategy_name': 'PumpShort',
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
        'worst_trade': results['pnl_usd'].min()
    }
    
    # Import here to avoid early import issues if sheets.py has setup code
    from sheets import log_strategy_summary
    log_strategy_summary(summary_data)


if __name__ == "__main__":
    main()
