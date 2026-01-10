import pandas as pd
import os

def investigate_week():
    csv_path = "/Users/firat/Algo/binance-backtest/backtest_results_pump.csv"
    if not os.path.exists(csv_path):
        print("❌ CSV not found.")
        return

    print("📊 Loading Backtest Results...")
    df = pd.read_csv(csv_path)
    
    # Convert dates
    df['entry_time'] = pd.to_datetime(df['entry_time'])
    
    # Filter for 09.10 - 13.10 (assuming month-day format)
    # We need to guess the Year. Probably the latest year in the data.
    # Let's inspect the date range of the whole df first.
    print(f"Full Date Range: {df['entry_time'].min()} to {df['entry_time'].max()}")
    
    # Filter specifically for Month 10, Days 9-13
    # Use generic filtering to catch any year present
    target_df = df[
        (df['entry_time'].dt.month == 10) & 
        (df['entry_time'].dt.day >= 9) & 
        (df['entry_time'].dt.day <= 13)
    ]
    
    print(f"\nExample filtered row dates: {target_df['entry_time'].head().dt.date.unique()}")
    
    if target_df.empty:
        print("⚠️ No trades found for Oct 09-13.")
        return

    print(f"\n🔍 Analyzing Period: 09.10 - 13.10")
    print(f"Total Trades: {len(target_df)}")
    
    # 1. Check Duplicates
    # Duplicate defined as same Symbol AND same Entry Time
    duplicates = target_df[target_df.duplicated(subset=['symbol', 'entry_time'], keep=False)]
    if not duplicates.empty:
        print(f"⚠️  FOUND {len(duplicates)} DUPLICATE TRADES!")
        print(duplicates[['symbol', 'entry_time']].head())
    else:
        print("✅ No exact duplicates found (Symbol + Entry Time).")

    # 2. Daily Breakdown
    print("\n📅 Daily Breakdown:")
    daily_counts = target_df.groupby(target_df['entry_time'].dt.date).size()
    print(daily_counts)
    
    # 3. Symbol Breakdown (Top 10)
    print("\n🏆 Top 10 Most Active Symbols:")
    print(target_df['symbol'].value_counts().head(10))
    
    # 4. PnL Check
    print(f"\n💰 Total PnL for period: ${target_df['pnl_usd'].sum():.2f}")
    print(f"   Win Rate: {(target_df['pnl_usd'] > 0).mean()*100:.2f}%")

if __name__ == "__main__":
    investigate_week()
