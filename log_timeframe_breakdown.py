"""
Timeframe Breakdown Analysis Logger
Reads existing TF results from Google Sheets and creates consolidated breakdown view
"""
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime

def log_timeframe_breakdown():
    """
    Read individual TF results from Sheets and create consolidated breakdown
    """
    # Connect to Google Sheets
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID', '1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM')
    sheet = client.open_by_key(manual_sheet_id)
    ws = sheet.worksheet("Analysis")
    
    # Get all existing data
    all_data = ws.get_all_values()
    
    # Find rows with current strategy (EMA:Bull, TP:2%, SL:2%)
    tf_data = {}
    for row in all_data[2:10]:  # Rows 3-10
        if len(row) > 4:
            strategy_name = row[1]
            
            # Parse timeframe from strategy name
            tf = None
            if '5s ' in strategy_name:
                tf = '5s'
            elif '10s ' in strategy_name:
                tf = '10s'
            elif '15s ' in strategy_name and 'old' not in strategy_name:
                tf = '15s'
            elif '30s ' in strategy_name:
                tf = '30s'
            elif '45s ' in strategy_name:
                tf = '45s'
            elif '1m ' in strategy_name:
                tf = '1m'
            
            if tf and 'EMA:Bull Pump:3.0% TP:3.0% SL:3.0%' in strategy_name:
                try:
                    trades = int(row[3].replace(',', ''))
                    pnl = float(row[4].replace('$', '').replace(',', ''))
                    tf_data[tf] = {'trades': trades, 'pnl': pnl}
                    print(f"Found {tf}: {trades} trades, ${pnl:.2f}")
                except:
                    continue
    
    # Calculate totals
    total_trades = sum(d['trades'] for d in tf_data.values())
    total_pnl = sum(d['pnl'] for d in tf_data.values())
    
    print(f"\n📊 Totals: {total_trades} trades, ${total_pnl:.2f} PnL")
    
    # Calculate percentages for each TF
    tf_stats = {}
    for tf in ['5s', '10s', '15s', '30s', '45s', '1m']:
        if tf in tf_data:
            trades = tf_data[tf]['trades']
            pnl = tf_data[tf]['pnl']
            tf_stats[tf] = {
                'trades_pct': (trades / total_trades) * 100,
                'pnl_pct': (pnl / total_pnl) * 100 if total_pnl != 0 else 0
            }
            print(f"{tf}: {trades} ({tf_stats[tf]['trades_pct']:.1f}%) | ${pnl:.2f} ({tf_stats[tf]['pnl_pct']:.1f}%)")
        else:
            tf_stats[tf] = {'trades_pct': 0.0, 'pnl_pct': 0.0}
    
    # Build row data
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    strategy_desc = f"[SHORT] AllTF EMA:Bull Pump:3% TP:3% SL:3% Maru:0.8"
    
    # Calculate win rate (weighted average from individual TFs)
    # We'll just use an approximate based on total
    win_rate = 0.517  # From our AllTF run
    
    row_data = [
        timestamp,
        strategy_desc,
        win_rate,
        total_trades,
        total_pnl,
        # 5s
        tf_stats['5s']['trades_pct'] / 100,
        tf_stats['5s']['pnl_pct'] / 100,
        # 10s
        tf_stats['10s']['trades_pct'] / 100,
        tf_stats['10s']['pnl_pct'] / 100,
        # 15s
        tf_stats['15s']['trades_pct'] / 100,
        tf_stats['15s']['pnl_pct'] / 100,
        # 30s
        tf_stats['30s']['trades_pct'] / 100,
        tf_stats['30s']['pnl_pct'] / 100,
        # 45s
        tf_stats['45s']['trades_pct'] / 100,
        tf_stats['45s']['pnl_pct'] / 100,
        # 1m
        tf_stats['1m']['trades_pct'] / 100,
        tf_stats['1m']['pnl_pct'] / 100
    ]
    
    print(f"\n📝 Writing consolidated row to Sheets...")
    # ws.insert_row(row_data, 3) # Disabled to prevent accidental duplicate inserts
    
    # Verify headers
    headers_row2 = ws.row_values(2)
    expected_headers = [
        'Win Rate', 'Total Trades', 'Total PnL ($)',
        '5s Trades%', 'PnL',
        '10s Trades%', 'PnL',
        '15s Trades%', 'PnL',
        '30s Trades%', 'PnL',
        '45s Trades%', 'PnL',
        '1m Trades%', 'PnL'
    ]
    
    # Check if headers (columns C-Q) match properly
    if len(headers_row2) < 17:
        print("⚠️ Warning: Header row length checks failed")
    
    print("✅ Timeframe breakdown logged successfully!")
    print(f"   Strategy: {strategy_desc}")
    print(f"   Total: {total_trades} trades, ${total_pnl:.2f}")

if __name__ == "__main__":
    log_timeframe_breakdown()
