import sys
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
import re

# System Rule 1 & 2: Credentials Location
DEFAULT_CREDS_PATH = os.path.expanduser("~/Algo/credentials/google_service_account.json")

# Master Column Structure for Parameters (C-L)
PARAM_COLS = {
    2: {"header": "Side", "options": ["SHORT", "LONG"]},
    3: {"header": "Cond", "options": ["pump", "dump"]},
    4: {"header": "Threshold%", "options": ["3.0", "2.5", "2.0", "1.5", "1.0", "-1.0", "-1.5", "-2.0", "-2.5", "-3.0"]},
    5: {"header": "EMA", "options": ["⚪ none", "🔴🔴🔴 all_bear", "🟢🟢🟢 all_bull", "🔴🔴 big_bear", "🟢🟢 big_bull", "🔴 small_bear", "🟢 small_bull", "🔴🔴🟢 big_bear_small_bull", "🟢🟢🔴 big_bull_small_bear"]},
    6: {"header": "TP%", "options": [str(x) for x in range(10, 0, -1)]},
    7: {"header": "SL%", "options": [str(x) for x in range(10, 0, -1)]},
    8: {"header": "TSL%", "options": [str(x) for x in range(10, 0, -1)] + ["OFF"]},
    9: {"header": "Maru", "options": ["0.9", "0.8", "0.7", "0.6", "0.5"]},
    10: {"header": "Days", "options": []}
}

METRICS_START_COL = 11 # L (WinRate), M (Trades), N (PnL)

def get_credentials_path():
    return Path(DEFAULT_CREDS_PATH)

def log_analysis_to_sheet(data, json_path=None):
    try:
        creds_path = DEFAULT_CREDS_PATH
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
        client = gspread.authorize(creds)
        
        # Open Sheet
        manual_sheet_id = os.environ.get('MANUAL_SHEET_ID')
        MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
        sheet_id = manual_sheet_id if manual_sheet_id else MASTER_SHEET_ID
        
        sheet = client.open_by_key(sheet_id)
        ws = sheet.worksheet("backtest1")
        
        # --- DYNAMIC ROW DATA POPULATION ---
        row_data = {i: "" for i in range(1, 150)} # Pre-initialize
        row_data[1] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row_data[2] = data.get('strategy_name', 'Unknown')
        
        strategy_str = data.get('strategy_name', '')
        ema_emoji_map = {
            "big_bull": "🟢🟢", "big_bear": "🔴🔴", "all_bull": "🟢🟢🟢", "all_bear": "🔴🔴🔴",
            "small_bull": "🟢", "small_bear": "🔴", "none": "⚪",
            "big_bull_small_bear": "🟢🟢🔴", "big_bear_small_bull": "🔴🔴🟢"
        }
        
        # Extract Params (Indices 3-11 for C-K)
        if "[SHORT]" in strategy_str: row_data[3] = "SHORT"
        elif "[LONG]" in strategy_str: row_data[3] = "LONG"
        
        if "PUMP" in strategy_str.upper(): row_data[4] = "pump"
        elif "DUMP" in strategy_str.upper(): row_data[4] = "dump"
        
        ema_match = re.search(r'EMA:(\S+)', strategy_str)
        ema_raw = (ema_match.group(1).lower() if ema_match else "none").replace("big_bull_small_bull", "all_bull").replace("big_bear_small_bear", "all_bear").replace("small_bull_big_bull", "all_bull").replace("small_bear_big_bear", "all_bear").replace("small_bull_big_bear", "big_bear_small_bull").replace("small_bear_big_bull", "big_bull_small_bear")
        
        emoji = ema_emoji_map.get(ema_raw, "")
        row_data[6] = f"{emoji} {ema_raw}" if emoji else ema_raw

        pump_match = re.search(r'Pump:(\d+\.?\d*)%', strategy_str)
        dump_match = re.search(r'Dump:(\d+\.?\d*)%', strategy_str)
        if pump_match: row_data[5] = f"{float(pump_match.group(1)):.1f}"
        elif dump_match: row_data[5] = f"{-float(dump_match.group(1)):.1f}"

        row_data[7] = (re.search(r'TP:(\d+\.?\d*)%', strategy_str) or re.search(r'TP:(\d+)', strategy_str)).group(1) if re.search(r'TP:', strategy_str) else ""
        row_data[8] = (re.search(r'SL:(\d+\.?\d*)%', strategy_str) or re.search(r'SL:(\d+)', strategy_str)).group(1) if re.search(r'SL:', strategy_str) else ""
        row_data[9] = (re.search(r'TSL:(\d+\.?\d*|OFF)', strategy_str)).group(1) if re.search(r'TSL:', strategy_str) else "OFF"
        row_data[10] = (re.search(r'M:(\d+\.?\d*)', strategy_str)).group(1) if re.search(r'M:', strategy_str) else ""
        row_data[11] = str(data.get('total_days', 90))

        # Metrics (L-N: 12, 13, 14)
        row_data[12] = float(data.get('win_rate', 0))/100.0
        row_data[13] = int(data.get('total_trades', 0))
        row_data[14] = float(data.get('total_pnl', 0))

        # Timeframes (O-Z, col 15+)
        tf_breakdown = data.get('tf_breakdown', {})
        tf_cols = {'5s': 15, '10s': 17, '15s': 19, '30s': 21, '45s': 23, '1m': 25}
        for tf, col in tf_cols.items():
            stats = tf_breakdown.get(tf, {})
            # row_data is 1-indexed. col 15 is index 15.
            row_data[col] = int(stats.get('trades', 0))
            row_data[col+1] = float(stats.get('pnl', 0.0))

        weekly_stats = data.get('weekly_stats', [])
        new_cols_group = []
        headers_r1 = ws.row_values(1)
        # Strip trailing empty strings
        while headers_r1 and not str(headers_r1[-1]).strip():
            headers_r1.pop()
            
        next_col_idx = len(headers_r1) + 1
        if next_col_idx < 27: next_col_idx = 27 # Start at AA
        
        # Weekly Stats
        for week in weekly_stats:
            label = week['label']
            week_num = week.get('week_num')
            trades = week['trades']
            pnl = week['pnl']
            
            found_col = -1
            # headers_r1 is 0-indexed list, so enumerate gives 0-based index
            # We need 1-based column number for row_data
            for i, val in enumerate(headers_r1):
                if val == label:
                    found_col = i + 1  # Convert to 1-based column number
                    break
            
            if found_col != -1:
                # Label is at found_col, so Trades goes there, PnL at found_col+1
                row_data[found_col] = int(trades)
                row_data[found_col + 1] = pnl
            else:
                # New week - add at next available position
                current_start_col = next_col_idx + (len(new_cols_group) * 2)
                row_data[current_start_col] = int(trades)
                row_data[current_start_col + 1] = pnl
                new_cols_group.append({'label': label, 'week_num': week_num, 'start_col': current_start_col})


        # Create New Weekly Headers
        if new_cols_group:
            updates = []
            for group in new_cols_group:
                c = group['start_col']
                lbl = group['label']
                wn = f"W{group['week_num']:02d}" if group['week_num'] else ""
                updates.append({'range': gspread.utils.rowcol_to_a1(1, c), 'values': [[lbl]]})
                updates.append({'range': gspread.utils.rowcol_to_a1(1, c+1), 'values': [[wn]]})
                updates.append({'range': f"{gspread.utils.rowcol_to_a1(2, c)}:{gspread.utils.rowcol_to_a1(2, c+1)}", 'values': [["Trades", "PnL"]]})
            ws.batch_update(updates, value_input_option='USER_ENTERED')

        # Insert Data (At Row 3)
        max_idx = max(row_data.keys())
        final_values = [""] * max_idx
        for k, v in row_data.items():
            final_values[k-1] = v
            
        ws.insert_row(final_values, index=3, value_input_option='USER_ENTERED')
        
        # Format
        try:
            apply_sheet_formatting(ws)
        except:
             pass
        
        print(f"✅ Logged row to Row 3.")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Failed to log: {e}")

def apply_sheet_formatting(ws):
    pass # Disable complex formatting for now to be safe
