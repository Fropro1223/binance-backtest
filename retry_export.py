import sys
import os
import pandas as pd
import warnings

# Add current directory to path
sys.path.append(os.getcwd())

from sheets import upload_to_sheets, log_strategy_summary

warnings.simplefilter(action='ignore')

def main():
    print("🔄 Retry Export: Loading results...")
    
    results_file = "backtest_results_pump.csv"
    if not os.path.exists(results_file):
        print(f"❌ File not found: {results_file}")
        sys.exit(1)
        
    df = pd.read_csv(results_file)
    print(f"Loaded {len(df)} rows.")
    
    if 'pump_percent' in df.columns:
        print(f"✅ 'pump_percent' column found. Avg Pump: {df['pump_percent'].mean()*100:.2f}%")
    else:
        print("⚠️ 'pump_percent' column MISSING!")

    # 1. Calculate Stats
    total_trades = len(df)
    wins = len(df[df['pnl_usd'] > 0])
    losses = len(df[df['pnl_usd'] <= 0])
    win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
    total_pnl = df['pnl_usd'].sum()
    avg_pnl = df['pnl_usd'].mean()
    best_trade = df['pnl_usd'].max()
    worst_trade = df['pnl_usd'].min()
    
    print("------------------------------------------------")
    print(f"Total Trades: {total_trades}")
    print(f"Win Rate:     {win_rate:.2f}%")
    print(f"Total PnL:    ${total_pnl:.2f}")
    print(f"Avg PnL:      ${avg_pnl:.2f}")
    print("------------------------------------------------")
    
    # 2. Log Summary
    summary_data = {
        'strategy_name': 'Marubozu Pump (Short >2% 80%Body)',
        'tp_pct': 0.04,
        'sl_pct': 0.04,
        'max_pos': 1,
        'avg_thresh': 0.0,
        'bet_size': 7.0,
        'total_trades': total_trades,
        'win_rate': win_rate,
        'total_pnl': total_pnl,
        'avg_pnl': avg_pnl,
        'best_trade': best_trade,
        'worst_trade': worst_trade
    }
    
    print("\n☁️  Logging summary to Google Sheets...")
    try:
        log_strategy_summary(summary_data)
    except Exception as e:
        print(f"❌ Failed Summary Log: {e}")
        # import traceback
        # traceback.print_exc()

    # 3. Upload Full Data
    print("\n☁️  Uploading full dataset to Google Sheets...")
    try:
        upload_to_sheets(results_file)
    except Exception as e:
        print(f"❌ Failed Data Upload: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
