import sys
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from pathlib import Path
from datetime import datetime

# System Rule 1 & 2: Credentials Location
DEFAULT_CREDS_PATH = os.path.expanduser("~/Algo/credentials/google_service_account.json")

def get_credentials_path():
    """
    Determines the credentials path based on env var or default system location.
    """
    env_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if env_path:
        return Path(env_path)
    return Path(DEFAULT_CREDS_PATH)

def upload_to_sheets(csv_path_str, total_days=None):
    try:
        csv_path = Path(csv_path_str)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        creds_path = get_credentials_path()
        if not creds_path.exists():
            raise FileNotFoundError(f"Credentials file not found at: {creds_path}\n"
                                    f"Please ensure GOOGLE_APPLICATION_CREDENTIALS is set or file exists at default location.")

        print(f"Using credentials from: {creds_path}")
        
        # Auth
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
        client = gspread.authorize(creds)

        # Read Data
        print(f"Reading data from: {csv_path}")
        df = pd.read_csv(csv_path)

         # Fix Dates and Duration
        if 'entry_time' in df.columns and 'exit_time' in df.columns:
            try:
                # Convert to datetime objects (CSV times are in UTC)
                df['entry_time'] = pd.to_datetime(df['entry_time']).dt.tz_localize('UTC')
                df['exit_time'] = pd.to_datetime(df['exit_time']).dt.tz_localize('UTC')
                
                # Recalculate Duration (in minutes) because source was 0
                df['duration_min'] = (df['exit_time'] - df['entry_time']).dt.total_seconds() / 60
                
                # Round duration to 2 decimals for readability
                df['duration_min'] = df['duration_min'].round(2)
                
                # Convert to Istanbul timezone for display (UTC+3)
                entry_istanbul = df['entry_time'].dt.tz_convert('Europe/Istanbul')
                exit_istanbul = df['exit_time'].dt.tz_convert('Europe/Istanbul')

                # Format Dates as Strings for Sheets (Istanbul time for display)
                # Save purely formatted date for exit_time
                df['exit_time'] = exit_istanbul.dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Create HYPERLINK for entry_time
                # Entry time string needed for the label (Istanbul time)
                entry_time_str = entry_istanbul.dt.strftime('%Y-%m-%d %H:%M:%S')
                # Add timestamp column explicitly (keep as UTC for TradingView)
                df['entry_time_ts'] = df['entry_time'].astype('int64') // 10**9

                # Parse Symbol for Hyperlink
                def create_tv_link(row, date_label):
                    try:
                        timestamp = row['entry_time_ts']
                        parts = row['symbol'].split('_')
                        ticker = parts[0]
                        interval_raw = parts[1] if len(parts) > 1 else '1d'
                        
                        # Map to TV interval
                        tv_interval = '1D' # default
                        if 's' in interval_raw:
                            tv_interval = interval_raw.upper().replace('S', 'S')
                        elif 'm' in interval_raw:
                            tv_interval = interval_raw.replace('m', '')
                        elif 'h' in interval_raw:
                            tv_interval = str(int(interval_raw.replace('h', '')) * 60)
                        elif 'd' in interval_raw:
                            tv_interval = interval_raw.upper()
                        
                        # Simple TradingView chart link (no timestamp)
                        url = f"https://www.tradingview.com/chart/ldW33glX/?symbol=BINANCE:{ticker}.P&interval={tv_interval}"
                        return f'=HYPERLINK("{url}", "{date_label}")'
                    except Exception:
                        return date_label

                # Apply to entry_time using row access
                # First row debug
                if not df.empty:
                     sample_row = df.iloc[0]
                     print(f"DEBUG Sample URL: {create_tv_link(sample_row, entry_time_str[sample_row.name])}")

                df['entry_time'] = df.apply(lambda row: create_tv_link(row, entry_time_str[row.name]), axis=1)
                
                print("✅ Recalculated 'duration_min' and added TradingView hyperlinks with timestamps.")
            except Exception as e:
                print(f"Warning: Could not process dates/duration: {e}")

        df = df.fillna("")
        
        # Prepare Sheet Name (files are usually like "BTCUSDT_results.csv")
        # specific sheet name logic wasn't requested, using filename as sheet title or fallback
        sheet_title = "Backtest Results"
        
        # Prepare Sheet Name with Timestamp
        import time
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        
        # CHECK FOR MANUALLY PROVIDED SHEET ID/URL via input or env
        # This is the fix for Storage Quota Exceeded on Service Account
        MANUAL_SHEET_ID = os.environ.get('MANUAL_SHEET_ID')
        
        if MANUAL_SHEET_ID:
             print(f"Using Manual Sheet ID: {MANUAL_SHEET_ID}")
             try:
                 sheet = client.open_by_key(MANUAL_SHEET_ID)
                 print(f"✅ Opened Manual Sheet: {sheet.title}")
                 
                 # CLEANUP: Delete old tabs to avoid 10M cell limit
                 # Keep the first tab if it's not a "Run_" one, or just keep the newest?
                 # Strategy: Delete all tabs that start with "Run_" except maybe the last one? 
                 # Or just delete all "Run_" tabs since we are re-uploading the same data effectively.
                 try:
                     worksheets = sheet.worksheets()
                     for const_ws in worksheets:
                         if const_ws.title.startswith("Run_"):
                             print(f"  - Deleting old tab: {const_ws.title} to free up space...")
                             try:
                                 sheet.del_worksheet(const_ws)
                             except Exception as del_err:
                                 print(f"    Failed to delete tab {const_ws.title}: {del_err}")
                 except Exception as e:
                     print(f"Warning during tab cleanup: {e}")
                     
             except Exception as e:
                 print(f"Failed to open manual sheet: {e}")
                 raise e
        else:
            # Fallback to creation (which might fail if quota exceeded)
            sheet_title = f"Backtest_Results_{timestamp}"
            print(f"Creating NEW Spreadsheet: {sheet_title}")
            try:
                sheet = client.create(sheet_title)
                sheet.share(None, perm_type='anyone', role='writer')
                print(f"✅ Created and Shared Spreadsheet: {sheet.url}")
            except Exception as e:
                print(f"Error creating sheet: {e}")
                # Re-raise to alert loop
                raise e

        # Prepare new worksheet name with timestamp
        import time
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        worksheet_title = f"Run_{timestamp}"
        
        print(f"Creating new worksheet: {worksheet_title}")
        try:
            # Create new worksheet
            worksheet = sheet.add_worksheet(title=worksheet_title, rows=len(df)+20, cols=len(df.columns)+5)
        except Exception as e:
            # Fallback in case of weird errors, try to modify name slightly
            import random
            worksheet_title = f"Run_{timestamp}_{random.randint(10,99)}"
            print(f"Retry creating worksheet as: {worksheet_title}")
            worksheet = sheet.add_worksheet(title=worksheet_title, rows=len(df)+20, cols=len(df.columns)+5)

        # Convert to list of lists for gspread
        data = [df.columns.tolist()] + df.values.tolist()
        
        print(f"Uploading data to new tab '{worksheet_title}'...")
        
        # Chunked Upload Logic to avoid API 500 Errors with large datasets + formulas
        chunk_size = 5000
        total_rows = len(data)
        
        # Upload headers first (row 1) separately or as part of first chunk? 
        # data includes headers at index 0.
        
        for i in range(0, total_rows, chunk_size):
            chunk = data[i : i + chunk_size]
            start_row = i + 1
            end_row = i + len(chunk)
            
            # Construct A1 notation range, e.g. A1:H5000
            # Just specifying start cell like 'A1' is usually enough for gspread to figure it out, 
            # but to be precise for chunks we need the start cell of each chunk.
            start_cell = f"A{start_row}"
            
            print(f"  - Uploading chunk rows {start_row} to {end_row}...")
            
            try:
                # Retry logic for chunks
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        worksheet.update(start_cell, chunk, value_input_option='USER_ENTERED')
                        break # Success
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise e
                        print(f"    Warning: Chunk upload failed (attempt {attempt+1}), retrying... {e}")
                        import time
                        time.sleep(5) # Wait before retry
                        
            except Exception as e:
                 print(f"ERROR: Failed to upload chunk starting at {start_cell}: {e}")
                 # Don't break immediately? Or do? 
                 # If one chunk fails, the data is partial. Better to fail hard or try to continue?
                 # Failing hard is safer for data integrity.
                 raise e
        
        print(f"SUCCESS: Uploaded {len(data)-1} data rows to Google Sheet '{sheet_title}'")

    except Exception:
        # User Rule: Print full traceback, do not silently fail
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python sheets.py <path_to_csv>")
        sys.exit(1)
    
    # FIRST: Clean up Drive storage to avoid Quota Exceeded
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds_path = get_credentials_path()
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
        client = gspread.authorize(creds)
        
        print("🧹 Checking Drive storage usage...")
        all_files = client.list_spreadsheet_files()
        print(f"Found {len(all_files)} existing spreadsheets.")
        
        # Delete ALL existing sheets to free up space (User asked to "delete old")
        for f in all_files:
            try:
                print(f"Deleting older sheet: {f['name']} (ID: {f['id']})")
                client.del_spreadsheet(f['id'])
            except Exception as e:
                print(f"Failed to delete {f['name']}: {e}")
                
    except Exception as e:
        print(f"Warning: Failed to cleanup Drive: {e}")

    upload_to_sheets(sys.argv[1])

def log_strategy_summary(data: dict):
    """
    Appends a single row of strategy results to the 'Strategy Comparison' sheet.
    Cols: Timestamp, Strategy, TP, SL, Max Pos, Gap, Bet, Trades, Win Rate, Total PnL, Avg PnL
    """
    try:
        creds_path = get_credentials_path()
        if not creds_path.exists():
            print(f"⚠️ Credentials not found at {creds_path}")
            return

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
        client = gspread.authorize(creds)
        
        # Print Service Account Email for the user
        print(f"🤖 Bot Email: {creds.service_account_email}")
        print("ℹ️  To use your own sheet, share it with this email.")

        # TARGET MASTER SPREADSHEET
        # User provided backtestmini sheet
        MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
        
        sheet = None
        
        # 1. Try to open by ID (Priority)
        try:
             sheet = client.open_by_key(MASTER_SHEET_ID)
             print(f"✅ Opened Master Sheet by ID: {sheet.title}")
        except Exception as e:
             print(f"❌ Could not open sheet by ID ({MASTER_SHEET_ID}): {e}")
             print("   Please ensure you have shared the sheet with the bot email.")
             return # Stop if we can't open the specific requested sheet


        # Get or create 'Strategy Comparison' tab
        try:
            ws = sheet.worksheet("Strategy Comparison")
        except gspread.WorksheetNotFound:
            print("Creating 'Strategy Comparison' tab...")
            ws = sheet.add_worksheet("Strategy Comparison", rows=1000, cols=20)
            headers = [
                "Timestamp", "Strategy", "TP %", "SL %", "Max Pos", 
                "Gap %", "Bet ($)", "Total Trades", "Win Rate %", 
                "Total PnL ($)", "Avg PnL ($)", "Best Trade", "Worst Trade"
            ]
            ws.append_row(headers)

        # Prepare Row
        import time
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        
        row = [
            timestamp,
            data.get('strategy_name', 'Unknown'),
            data.get('tp_pct', 0) * 100,
            data.get('sl_pct', 0) * 100,
            data.get('max_pos', 0),
            data.get('avg_thresh', 0) * 100,
            data.get('bet_size', 0),
            data.get('total_trades', 0),
            f"{data.get('win_rate', 0):.2f}",
            f"{data.get('total_pnl', 0):.2f}",
            f"{data.get('avg_pnl', 0):.4f}",
            f"{data.get('best_trade', 0):.2f}",
            f"{data.get('worst_trade', 0):.2f}"
        ]
        
        print(f"writing to {sheet.title} -> Strategy Comparison...")
        ws.append_row(row)
        print("✅ Strategy result logged successfully.")
        print(f"🔗 Link: {sheet.url}")

    except Exception as e:
        print(f"❌ Failed to log strategy summary: {e}")

def log_analysis_to_sheet(data: dict):
    """
    Logs high-level analysis stats to the 'Analysis' tab of the MANUAL_SHEET_ID.
    Supports dynamic weekly columns with 2-Row Merged Headers.
    """
    try:
        creds_path = get_credentials_path()
        if not creds_path.exists():
            print(f"❌ Credentials not found at {creds_path}")
            return

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
        client = gspread.authorize(creds)
        
        # Use MANUAL_SHEET_ID env var if set, otherwise fallback to MASTER_SHEET_ID
        manual_sheet_id = os.environ.get('MANUAL_SHEET_ID')
        MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
        sheet_id = manual_sheet_id if manual_sheet_id else MASTER_SHEET_ID

        try:
            sheet = client.open_by_key(sheet_id)
            print(f"✅ Opened Sheet: {sheet.title}")
        except Exception as e:
            print(f"❌ Could not open sheet: {e}")
            return

        # Check/Create 'Analysis' worksheet
        try:
            ws = sheet.worksheet("Analysis")
        except gspread.exceptions.WorksheetNotFound:
            print("✨ Creating 'Analysis' worksheet...")
            ws = sheet.add_worksheet(title="Analysis", rows="1000", cols="100")
            ws.freeze(rows=2)
        
        # --- NEW STRUCTURE: 10 Parameter Columns After Strategy ---
        # Columns: A=Timestamp, B=Strategy, C-L=Parameters (10), M=WinRate, N=Trades, O=PnL
        
        # Define parameter columns (C through L, indices 3-12)
        PARAM_COLS = {
            3: {"header": "Side", "options": ["SHORT", "LONG"]},
            4: {"header": "Cond", "options": ["pump", "dump"]},
            5: {"header": "EMA", "options": ["none", "all_bull", "all_bear", "small_bull", "small_bear", "big_bull", "big_bear", "small_bull_big_bull", "small_bear_big_bear", "small_bull_big_bear", "small_bear_big_bull"]},
            6: {"header": "Pump%", "options": ["1", "2", "3", "4", "5"]},
            7: {"header": "Dump%", "options": ["1", "2", "3", "4", "5"]},
            8: {"header": "TP%", "options": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]},
            9: {"header": "SL%", "options": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]},
            10: {"header": "TSL%", "options": ["OFF", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]},
            11: {"header": "Maru", "options": ["0.5", "0.6", "0.7", "0.8", "0.9"]}
            # Note: Days (column 12) has no dropdown - it's purely informational
        }
        
        # Metrics start at column 13 (M)
        METRICS_START_COL = 13
        
        # Check if headers need to be set up
        headers_r2 = ws.row_values(2)
        if len(headers_r2) < 12 or headers_r2[2] != "Side":
            print("✨ Setting up 10 parameter columns with dropdowns...")
            
            # Set Row 2 headers for parameters
            param_headers = [PARAM_COLS[i]["header"] for i in range(3, 13)]
            ws.update(range_name="C2:L2", values=[param_headers])
            
            # Set Row 2 headers for metrics (M, N, O)
            ws.update(range_name="M2:O2", values=[["Win Rate", "Trades", "PnL ($)"]])
            
            # Add Data Validation (Dropdowns) for each parameter column
            dv_requests = []
            for col_idx, config in PARAM_COLS.items():
                options_str = ",".join(config["options"])
                dv_requests.append({
                    "setDataValidation": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": 2,  # Row 3 onwards
                            "endRowIndex": 1000,
                            "startColumnIndex": col_idx - 1,  # 0-indexed
                            "endColumnIndex": col_idx
                        },
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [{"userEnteredValue": opt} for opt in config["options"]]
                            },
                            "showCustomUi": True,
                            "strict": False
                        }
                    }
                })
            
            # Apply data validation
            if dv_requests:
                sheet.batch_update({"requests": dv_requests})
                print(f"✅ Added dropdowns to {len(dv_requests)} columns")
        
        # --- DYNAMIC HEADER LOGIC ---
        total_days = data.get('total_days', 90)
        headers_r1 = ws.row_values(1)
        
        # Row Data Map (1-based indices, shifted for new structure)
        # A=1, B=2, C-L=3-12 (params), M=13 (WinRate), N=14 (Trades), O=15 (PnL)
        row_data = {}
        row_data[1] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row_data[2] = data.get('strategy_name', 'Unknown')
        
        # Parse strategy_name to extract parameters for dropdown columns
        # Format: "[SHORT] PUMP EMA:None Pump:2.0% TP:8.0% SL:8.0% TSL:1% M:0.8"
        strategy_str = data.get('strategy_name', '')
        
        # Extract Side
        if "[SHORT]" in strategy_str:
            row_data[3] = "SHORT"
        elif "[LONG]" in strategy_str:
            row_data[3] = "LONG"
        else:
            row_data[3] = ""
        
        # Extract Cond
        if "PUMP" in strategy_str.upper():
            row_data[4] = "pump"
        elif "DUMP" in strategy_str.upper():
            row_data[4] = "dump"
        else:
            row_data[4] = ""
        
        # Extract EMA
        import re
        ema_match = re.search(r'EMA:(\S+)', strategy_str)
        row_data[5] = ema_match.group(1).lower() if ema_match else "none"
        
        # Extract Pump%
        pump_match = re.search(r'Pump:(\d+\.?\d*)%', strategy_str)
        row_data[6] = pump_match.group(1) if pump_match else ""
        
        # Extract Dump%
        dump_match = re.search(r'Dump:(\d+\.?\d*)%', strategy_str)
        row_data[7] = dump_match.group(1) if dump_match else ""
        
        # Extract TP%
        tp_match = re.search(r'TP:(\d+\.?\d*)%', strategy_str)
        row_data[8] = tp_match.group(1) if tp_match else ""
        
        # Extract SL%
        sl_match = re.search(r'SL:(\d+\.?\d*)%', strategy_str)
        row_data[9] = sl_match.group(1) if sl_match else ""
        
        # Extract TSL%
        tsl_match = re.search(r'TSL:(\d+\.?\d*|OFF)%?', strategy_str)
        row_data[10] = tsl_match.group(1) if tsl_match else "OFF"
        
        # Extract Marubozu
        maru_match = re.search(r'M:(\d+\.?\d*)', strategy_str)
        row_data[11] = maru_match.group(1) if maru_match else ""
        
        # Days
        row_data[12] = str(total_days)
        
        # Metrics (shifted to columns 13, 14, 15)
        row_data[METRICS_START_COL] = float(data.get('win_rate', 0))/100.0
        row_data[METRICS_START_COL + 1] = int(data.get('total_trades', 0))
        row_data[METRICS_START_COL + 2] = float(data.get('total_pnl', 0))
        
        # 3. Timeframe Breakdown (P onwards = column 16+)
        # Shifted by 10 for new parameter columns
        tf_breakdown = data.get('tf_breakdown', {})
        if tf_breakdown:
            print(f"📊 Processing Timeframe Breakdown for Sheets...")
            # Map TF to Start Column Index (1-based, shifted by +10)
            # Old: 6, 8, 10, 12, 14, 16 → New: 16, 18, 20, 22, 24, 26
            tf_map = {'5s': 16, '10s': 18, '15s': 20, '30s': 22, '45s': 24, '1m': 26}
            
            # Initialize TF Headers if Row 2 is empty/incomplete
            headers_r2_current = ws.row_values(2)
            tf_header_updates = []
            
            for tf, col_start in tf_map.items():
                # Check if Trades/PnL labels exist in Row 2
                if len(headers_r2_current) < col_start or headers_r2_current[col_start-1] != "Trades":
                    tf_header_updates.append({'range': gspread.utils.rowcol_to_a1(1, col_start), 'values': [[tf]]})
                    tf_header_updates.append({'range': f"{gspread.utils.rowcol_to_a1(2, col_start)}:{gspread.utils.rowcol_to_a1(2, col_start+1)}", 'values': [["Trades", "PnL"]]})
                
                stats = tf_breakdown.get(tf, {})
                row_data[col_start] = int(stats.get('trades', 0))
                row_data[col_start+1] = stats.get('pnl', 0.0)
                
            if tf_header_updates:
                try:
                    ws.batch_update(tf_header_updates)
                except Exception as e:
                    print(f"⚠️ TF Header Update Warning: {e}")

        weekly_stats = data.get('weekly_stats', [])
        new_cols_group = []
        next_col_idx = len(headers_r1) + 1
        # Ensure we don't overwrite TF columns (now ending at 27)
        if next_col_idx < 28:
             next_col_idx = 28
        
        # Initialize final_requests early (used in weekly stats section)
        final_requests = []
        
        for week in weekly_stats:
            label = week['label']
            week_num = week.get('week_num')
            trades = week['trades']
            pnl = week['pnl']
            
            # Find Label in Row 1
            found_col = -1
            for i, val in enumerate(headers_r1):
                if val == label:
                    found_col = i + 1
                    break
            
            # Formatted Trades (Absolute Count)
            trade_val = int(trades)
            # DEBUG for user verification
            print(f"   📊 week {label}: {trades} trades")
                
            # Formatted PnL (Absolute USD)
            pnl_val = pnl
            
            if found_col != -1:
                # Found existing week: Trades at found_col, PnL at found_col+1
                row_data[found_col] = trade_val
                row_data[found_col+1] = pnl_val
                # Update week num in Row 1 just in case
                if week_num:
                    ws.update_cell(1, found_col+1, f"W{week_num:02d}")
            else:
                # New Week
                current_start = next_col_idx + (len(new_cols_group) * 2)
                row_data[current_start] = trade_val
                row_data[current_start+1] = pnl_val
                
                new_cols_group.append({
                    'label': label,
                    'week_num': week_num,
                    'start_col': current_start,
                    'is_date': True # For font size styling later
                })
        
        # Create Headers for New Weeks
        if new_cols_group:
            print(f"✨ Adding {len(new_cols_group)} new weekly header groups...")
            header_batch_data = []
            for group in new_cols_group:
                c_idx = group['start_col']
                lbl = group['label']
                wn = group.get('week_num')
                wn_str = f"W{wn:02d}" if wn else ""
                
                # Prepare header values for this group (Row 1 & 2)
                header_range = f"{gspread.utils.rowcol_to_a1(1, c_idx)}:{gspread.utils.rowcol_to_a1(2, c_idx+1)}"
                values = [
                    [lbl, wn_str],
                    ["Trades", "PnL"]
                ]
                header_batch_data.append({'range': header_range, 'values': values})
                
                # Apply font size 8 to the date label in Row 1
                final_requests.append({
                    "repeatCell": {
                        "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": c_idx-1, "endColumnIndex": c_idx},
                        "cell": {"userEnteredFormat": {"textFormat": {"fontSize": 8}, "horizontalAlignment": "CENTER"}},
                        "fields": "userEnteredFormat.textFormat.fontSize,userEnteredFormat.horizontalAlignment"
                    }
                })
            
            if header_batch_data:
                try:
                    ws.batch_update(header_batch_data)
                except Exception as e:
                    print(f"⚠️ Warning: Could not update headers: {e}")



        # Insert Data (At Row 3)
        max_idx = max(row_data.keys())
        final_values = [""] * max_idx
        for k, v in row_data.items():
            final_values[k-1] = v
            
        print(f"DEBUG: Inserting Row Data: {final_values}")
            
        ws.insert_row(final_values, index=3, value_input_option='USER_ENTERED')
        
        # --- APPLY FORMATTING ---
        try:
            apply_sheet_formatting(ws, ws.row_values(2))
        except Exception as fmt_err:
             print(f"⚠️ Formatting update warning: {fmt_err}")
        
        print(f"✅ Analysis logged to '{sheet.title}' -> 'Analysis' (Row 3, Formatting Applied)")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Failed to log analysis summary: {e}")

def apply_sheet_formatting(ws, headers_r2_final):
    """
    Applies all structural formatting, styling, and conditional formatting to the Analysis sheet.
    Can be called standalone to update layout without re-running data logic.
    """
    
    # Collect ALL formatting and CF requests into a single batch_update
    final_requests = []
    
    # 1. Hide Timestamp Column
    final_requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
            "properties": {"hiddenByUser": True},
            "fields": "hiddenByUser"
        }
    })

    # 1a. Global Row 1 Formatting (Font Size 8, Bold)
    final_requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 100},
            "cell": {"userEnteredFormat": {"textFormat": {"fontSize": 8, "bold": True}, "horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat(textFormat.fontSize,textFormat.bold,horizontalAlignment)"
        }
    })

    # 1. Formatting for Column A (Timestamp) - Font Size 7
    final_requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {"textFormat": {"fontSize": 7}}},
            "fields": "userEnteredFormat.textFormat.fontSize"
        }
    })
    
    # 2. Formatting for Column B (Strategy) - Font Size 8
    final_requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": 1, "endColumnIndex": 2},
            "cell": {"userEnteredFormat": {"textFormat": {"fontSize": 8}}},
            "fields": "userEnteredFormat.textFormat.fontSize"
        }
    })
    
    # 3. Static Formats (M=WinRate, N=Trades, O=PnL)
    def get_repeat_cell_req(range_a1, cell_format, fields):
        start, end = range_a1.split(":")
        sr, sc = gspread.utils.a1_to_rowcol(start)
        er, ec = gspread.utils.a1_to_rowcol(end)
        return {
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": sr - 1,
                    "endRowIndex": er,
                    "startColumnIndex": sc - 1,
                    "endColumnIndex": ec
                },
                "cell": {"userEnteredFormat": cell_format},
                "fields": fields
            }
        }

    # Win Rate (M = Column 13)
    final_requests.append(get_repeat_cell_req("M3:M1000", 
        {"numberFormat": {"type": "PERCENT", "pattern": "0%"}, "horizontalAlignment": "CENTER", "backgroundColor": {"red": 0, "green": 0, "blue": 0}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9}},
        "userEnteredFormat(numberFormat,horizontalAlignment,backgroundColor,textFormat)"))
    
    # Trades (N = Column 14)
    final_requests.append(get_repeat_cell_req("N3:N1000", 
        {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}, "horizontalAlignment": "RIGHT", "backgroundColor": {"red": 0, "green": 0, "blue": 0}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9}},
        "userEnteredFormat(numberFormat,horizontalAlignment,backgroundColor,textFormat)"))
        
    # PnL (O = Column 15)
    final_requests.append(get_repeat_cell_req("O3:O1000", 
        {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0"}, "horizontalAlignment": "RIGHT", "backgroundColor": {"red": 0, "green": 0, "blue": 0}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9}},
        "userEnteredFormat(numberFormat,horizontalAlignment,backgroundColor,textFormat)"))

    # Format Parameter Columns C-L (3-12) with centered text, light gray background
    final_requests.append(get_repeat_cell_req("C3:L1000", 
        {"horizontalAlignment": "CENTER", "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}, "textFormat": {"fontSize": 8}},
        "userEnteredFormat(horizontalAlignment,backgroundColor,textFormat)"))

    # 3. Conditional Formatting Rules
    current_cf_index = 0
    # Side Conditional Formatting (C = Column 3, index 2)
    c_range = {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": 2, "endColumnIndex": 3}
    # SHORT -> Red text
    final_requests.append({"addConditionalFormatRule": {"rule": {"ranges": [c_range], "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "SHORT"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}}}, "index": current_cf_index}}); current_cf_index += 1
    # LONG -> Green text
    final_requests.append({"addConditionalFormatRule": {"rule": {"ranges": [c_range], "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "LONG"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}}}, "index": current_cf_index}}); current_cf_index += 1

    # Cond Conditional Formatting (D = Column 4, index 3)
    d_range = {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": 3, "endColumnIndex": 4}
    # pump -> Green text
    final_requests.append({"addConditionalFormatRule": {"rule": {"ranges": [d_range], "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "pump"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}}}, "index": current_cf_index}}); current_cf_index += 1
    # dump -> Red text
    final_requests.append({"addConditionalFormatRule": {"rule": {"ranges": [d_range], "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "dump"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}}}, "index": current_cf_index}}); current_cf_index += 1

    # PnL Conditional Formatting (O = Column 15, index 14)
    o_range = {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": 14, "endColumnIndex": 15}
    final_requests.append({"addConditionalFormatRule": {"rule": {"ranges": [o_range], "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}}}, "index": current_cf_index}}); current_cf_index += 1
    final_requests.append({"addConditionalFormatRule": {"rule": {"ranges": [o_range], "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}}}, "index": current_cf_index}}); current_cf_index += 1

    # 3. Weekly Columns Formats & CF (start at column 28+)
    week_pair_index = 0
    color_a = {"red": 1.0, "green": 1.0, "blue": 1.0}
    color_b = {"red": 0.93, "green": 0.93, "blue": 0.93}
    color_tf = {"red": 0.95, "green": 0.97, "blue": 1.0} # Alice Blue
    
    for i, val in enumerate(headers_r2_final):
        c_idx = i + 1
        if c_idx <= 15: continue  # Skip A-O
        
        # Robust Logic: If Row 2 header is "Trades" OR if c_idx is even (for TF/Weekly pairs)
        # Actually, standardizing on Row 2 label is safer if labels are present.
        # Pairs are (6,7), (8,9), (10,11), (12,13), (14,15), (16,17), (18,19)...
        # Even column index (6, 8, 10...) = Trades
        # Odd column index (7, 9, 11...) = PnL
        
        is_trade = (val == "Trades") or (c_idx % 2 == 0)
        
        # --- COLOR LOGIC ---
        blue_hdr = {"red": 0.0, "green": 0.0, "blue": 0.8}
        black_txt = {"red": 0.0, "green": 0.0, "blue": 0.0}
        current_text_color = black_txt
        
        # P-AA: Timeframe Breakdown (Indices 16-27)
        if 16 <= c_idx <= 27:
            if 16 <= c_idx <= 17: # 5s
                current_color = color_tf
            else: # 10s, 15s, 30s, 45s, 1m
                current_color = color_a
            
            # All data cells (Row 3+) should have black text
            current_text_color = black_txt
        else:
            # AB onwards: Weekly Breakdown (Indices 28+) -> Alternating White/Gray
            if is_trade:
                current_color = color_a if week_pair_index % 2 == 0 else color_b
                week_pair_index += 1
            else:
                current_color = color_a if (week_pair_index - 1) % 2 == 0 else color_b
        
        col_range = {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1000, "startColumnIndex": c_idx-1, "endColumnIndex": c_idx}
        
        # Formatting request
        number_format = {"type": "NUMBER", "pattern": "#,##0"} if is_trade else {"type": "CURRENCY", "pattern": "$#,##0"}
        
        final_requests.append({
            "repeatCell": {
                "range": col_range,
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": number_format,
                        "horizontalAlignment": "RIGHT",
                        "backgroundColor": current_color,
                        "textFormat": {"fontSize": 9, "foregroundColor": current_text_color}
                    }
                },
                "fields": "userEnteredFormat(numberFormat,horizontalAlignment,backgroundColor,textFormat)"
            }
        })
        
        # --- ROW 2 LABEL STYLING (Trades / PnL) ---
        if 6 <= c_idx:
            # Row 2 labels should always be black text
            label_color = black_txt
                
            label_range = {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": c_idx-1, "endColumnIndex": c_idx}
            final_requests.append({
                "repeatCell": {
                    "range": label_range,
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "textFormat": {"fontSize": 8, "bold": True, "foregroundColor": label_color}
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,textFormat)"
                }
            })
        
        # --- ROW 1 HEADER STYLING (TF ONLY) ---
        if 16 <= c_idx <= 27 and val == "Trades": # val="Trades" or val="PnL" labels in Row 2, but we target Row 1 cell above it
            # Target the Merged Header in Row 1 (e.g., "5s", "10s")
            tf_header_range = {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": c_idx-1, "endColumnIndex": c_idx+1}
            final_requests.append({
                "repeatCell": {
                    "range": tf_header_range,
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "bold": True,
                                "foregroundColor": blue_hdr
                            }
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,textFormat)"
                }
            })
        
        if val == "PnL":
            pnl_range = {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": c_idx-1, "endColumnIndex": c_idx}
            final_requests.append({"addConditionalFormatRule": {"rule": {"ranges": [pnl_range], "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}}}, "index": current_cf_index}}); current_cf_index += 1
            final_requests.append({"addConditionalFormatRule": {"rule": {"ranges": [pnl_range], "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}}}, "index": current_cf_index}}); current_cf_index += 1

    # Auto-resize Column B (Strategy Name)
    final_requests.append({
        "autoResizeDimensions": {
            "dimensions": {
                "sheetId": ws.id,
                "dimension": "COLUMNS",
                "startIndex": 1,
                "endIndex": 2
            }
        }
    })

    if final_requests:
        ws.spreadsheet.batch_update({"requests": final_requests})
        print(f"✅ Successfully applied {len(final_requests)} layout/formatting updates.")
                
        try:
            ws.columns_auto_resize(0, len(headers_r2_final))
        except: pass
        
        print(f"✅ Analysis logged to '{ws.spreadsheet.title}' -> 'Analysis' (Row 3, Optimized Batch Updates)")

