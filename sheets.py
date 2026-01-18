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

def upload_to_sheets(csv_path_str):
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
            headers_r1 = ["Timestamp", "Strategy", "Win Rate %", "Total Trades", "Total PnL ($)"]
            ws.update(range_name="A1:E1", values=[headers_r1])
            ws.freeze(rows=2) # This was moved from later
        
        # --- DYNAMIC HEADER LOGIC ---
        
        # 1. Update Total PnL Header
        date_range = data.get('date_range', '')
        if date_range:
            pnl_header = f"Total PnL ($)\n{date_range}"
            ws.update_cell(1, 5, pnl_header)
        
        # 2. Handle Weekly Columns
        headers_r1 = ws.row_values(1)
        
        # Row Data Map (New columns will be added here)
        # 1-based indices
        row_data = {}
        row_data[1] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row_data[2] = data.get('strategy_name', 'Unknown')
        row_data[3] = float(data.get('win_rate', 0))/100.0
        row_data[4] = int(data.get('total_trades', 0))
        row_data[5] = float(data.get('total_pnl', 0))
        
        # 3. Timeframe Breakdown (F-Q)
        tf_breakdown = data.get('tf_breakdown', {})
        if tf_breakdown:
            print(f"📊 Processing Timeframe Breakdown for Sheets...")
            # Map TF to Start Column Index (1-based)
            tf_map = {'5s': 6, '10s': 8, '15s': 10, '30s': 12, '45s': 14, '1m': 16}
            
            for tf, stats in tf_breakdown.items():
                if tf in tf_map:
                    col_start = tf_map[tf]
                    row_data[col_start] = stats.get('trades_pct', 0.0)
                    row_data[col_start+1] = stats.get('pnl_pct', 0.0)

        weekly_stats = data.get('weekly_stats', [])
        new_cols_group = []
        next_col_idx = len(headers_r1) + 1
        # Ensure we don't overwrite TF columns if headers are missing
        if next_col_idx < 18:
             next_col_idx = 18
        
        for week in weekly_stats:
            label = week['label']
            trades = week['trades']
            pnl = week['pnl']
            
            # Find Label in Row 1
            found_col = -1
            for i, val in enumerate(headers_r1):
                if val == label:
                    found_col = i + 1
                    break
            
            # Formatted Trades Percentage (Float)
            total_trades_count = int(data.get('total_trades', 0))
            if total_trades_count > 0:
                trade_val = (trades / total_trades_count)
            else:
                trade_val = 0.0
                
            # Formatted PnL Percentage (Float)
            total_pnl_val_total = float(data.get('total_pnl', 0.0))
            if abs(total_pnl_val_total) > 0.000001:
                pnl_val = (pnl / total_pnl_val_total)
            else:
                pnl_val = 0.0
            
            if found_col != -1:
                # Found existing week: Trades at found_col, PnL at found_col+1
                row_data[found_col] = trade_val
                row_data[found_col+1] = pnl_val
            else:
                # New Week
                current_start = next_col_idx + (len(new_cols_group) * 2)
                row_data[current_start] = trade_val
                row_data[current_start+1] = pnl_val
                
                new_cols_group.append({
                    'label': label,
                    'start_col': current_start
                })
        
        # Create Headers for New Weeks
        if new_cols_group:
            print(f"✨ Adding {len(new_cols_group)} new weekly header groups...")
            for group in new_cols_group:
                c_idx = group['start_col']
                lbl = group['label']
                
                ws.update_cell(1, c_idx, lbl)
                ws.update_cell(2, c_idx, "Trades")
                ws.update_cell(2, c_idx+1, "PnL")
                
                # Merge Row 1 - REMOVED TO PREVENT ERRORS
                try:
                    # Write Label in the first cell
                    ws.update_cell(1, c_idx, lbl)
                    
                    # Also write empty string in next cell to be clean
                    ws.update_cell(1, c_idx+1, "")
                    
                    start_a1 = gspread.utils.rowcol_to_a1(1, c_idx)
                    
                    # Format Label
                    ws.format(start_a1, {
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE"
                    })
                    # Format Sub-headers
                    sub_rn = f"{gspread.utils.rowcol_to_a1(2, c_idx)}:{gspread.utils.rowcol_to_a1(2, c_idx+1)}"
                    ws.format(sub_rn, {
                         "textFormat": {"bold": True, "italic": True},
                         "horizontalAlignment": "CENTER"
                    })
                except Exception as merge_err:
                    print(f"⚠️ Warning: Could not format headers for {lbl}: {merge_err}")



        # Insert Data (At Row 3)
        max_idx = max(row_data.keys())
        final_values = [""] * max_idx
        for k, v in row_data.items():
            final_values[k-1] = v
            
        print(f"DEBUG: Inserting Row Data: {final_values}")
            
        ws.insert_row(final_values, index=3, value_input_option='USER_ENTERED')
        
        # --- FORMATTING & CF (Row 3+) ---
        ws.freeze(rows=2)
        
        # Hide Timestamp
        try:
            sheet.batch_update({
            "requests": [{"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser"
            }}]
            })
        except: pass

        # Static Formats (Row 3 to End)
        ws.format("C3:C1000", {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}, "horizontalAlignment": "CENTER"})
        ws.format("D3:D1000", {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}, "horizontalAlignment": "RIGHT"}) 
        ws.format("E3:E1000", {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0"}, "horizontalAlignment": "RIGHT"})

        
        # Weekly Columns Formats with Alternating Colors
        headers_r2_final = ws.row_values(2)
        week_pair_index = 0  # Track which week pair we're on
        
        # Define alternating background colors (White and Light Gray)
        color_a = {"red": 1.0, "green": 1.0, "blue": 1.0}        # White
        color_b = {"red": 0.93, "green": 0.93, "blue": 0.93}     # Light gray
        
        # Clear ANY existing Conditional Formatting to prevent conflicts
        # DISABLED FOR BATCH RUN
        try:
            # Delete top N CF rules to clear any old ones
            # delete_reqs = [{"deleteConditionalFormatRule": {"sheetId": ws.id, "index": 0}} for _ in range(10)]
            # try:
            #     sheet.batch_update({"requests": delete_reqs})
            #     print("🧹 Cleared old CF rules...")
            # except Exception: pass
            pass
        except Exception: pass

        # Collect CF requests for all columns
        cf_requests = []
        
        # CF for Column E (Total PnL) - Uses column index 4 (0-based)
        cf_requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": 4, "endColumnIndex": 5}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                        "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}
                    }
                },
                "index": 0
            }
        })
        cf_requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": 4, "endColumnIndex": 5}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                        "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}
                    }
                },
                "index": 1
            }
        })
        
        # CF for Weekly PnL columns
        cf_rule_index = 2
        for i, val in enumerate(headers_r2_final):
            c_idx = i + 1
            if c_idx <= 5: continue
            
            rng = f"{gspread.utils.rowcol_to_a1(3, c_idx)}:{gspread.utils.rowcol_to_a1(1000, c_idx)}"
            header_rng = f"{gspread.utils.rowcol_to_a1(1, c_idx)}:{gspread.utils.rowcol_to_a1(2, c_idx)}"
            
            # Determine color based on week pair (Trades starts a new pair)
            if val == "Trades":
                current_color = color_a if week_pair_index % 2 == 0 else color_b
                week_pair_index += 1
            else:
                # PnL uses same color as its Trades pair
                current_color = color_a if (week_pair_index - 1) % 2 == 0 else color_b
            
            if val == "Trades":
                ws.format(rng, {
                    "numberFormat": {"type": "PERCENT", "pattern": "0.0%"}, 
                    "horizontalAlignment": "RIGHT",
                    "backgroundColor": current_color
                })
                # Also color the header row
                ws.format(header_rng, {"backgroundColor": current_color})
            elif val == "PnL":
                # Format with percentage only (no color pattern)
                ws.format(rng, {
                    "numberFormat": {"type": "PERCENT", "pattern": "0.0%"}, 
                    "horizontalAlignment": "RIGHT",
                    "backgroundColor": current_color
                })
                # Also color the header row
                ws.format(header_rng, {"backgroundColor": current_color})
                
                # Add CF rules for this PnL column with SAME colors as Column E
                cf_requests.append({
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": c_idx-1, "endColumnIndex": c_idx}],
                            "booleanRule": {
                                "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                                "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}
                            }
                        },
                        "index": cf_rule_index
                    }
                })
                cf_rule_index += 1
                
                cf_requests.append({
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": c_idx-1, "endColumnIndex": c_idx}],
                            "booleanRule": {
                                "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                                "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}
                            }
                        },
                        "index": cf_rule_index
                    }
                })
                cf_rule_index += 1

        # Send Batch CF
        if cf_requests:
            try:
                sheet.batch_update({"requests": cf_requests})
                print(f"✅ Applied {len(cf_requests)} conditional formatting rules")
            except Exception as e:
                print(f"⚠️ CF application warning: {e}")
            
        ws.columns_auto_resize(0, len(headers_r2_final))
        print(f"✅ Analysis logged to '{sheet.title}' -> 'Analysis' (Row 3, Merged Headers)")
        
        # SKIP CF UPDATE DURING BATCH RUN TO SAVE QUOTA
        # We will run reset_and_apply_all.py at the end manually
        # if cf_requests:
        #    try:
        #        sheet.batch_update({"requests": cf_requests})
        #    except Exception: pass

    except Exception as e:
        print(f"❌ Failed to log analysis summary: {e}")
