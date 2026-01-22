import sys
import os
from pathlib import Path

# Add project root to path so we can import sheets.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sheets
from oauth2client.service_account import ServiceAccountCredentials
import gspread

def update_layout():
    """
    Connects to the 'Analysis' sheet and ensures layout/formatting is up to date.
    This includes:
    - Auto-resizing Column B
    - Applying Conditional Formatting (Side=Red/Green, Cond=Green/Red)
    - Coloring data columns (white/gray/blue)
    - Hiding Timestamp column
    """
    print("🚀 Connecting to Google Sheets to update layout...")
    
    try:
        creds_path = sheets.get_credentials_path()
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

        sheet = client.open_by_key(sheet_id)
        print(f"✅ Opened Sheet: {sheet.title}")
        
        ws = sheet.worksheet("backtest1")
        print(f"✅ Opened 'backtest1' tab")
        
        # --- EMOJI UPDATE FOR EMA COLUMN (Column E) ---
        print("🟢 Adding emojis and normalizing EMA column...")
        ema_col_values = ws.col_values(5)[2:] # Column E, from Row 3
        if ema_col_values:
            ema_emoji_map = {
                "big_bull": "🟢🟢",
                "big_bear": "🔴🔴",
                "all_bull": "🟢🟢🟢",
                "all_bear": "🔴🔴🔴",
                "small_bull": "🟢",
                "small_bear": "🔴",
                "none": "⚪",
                "big_bull_small_bear": "🟢🟢🔴",
                "big_bear_small_bull": "🔴🔴🟢"
            }
            new_ema_values = []
            for val in ema_col_values:
                # Extract the base text (e.g., "small_bull_big_bull" from "⚪ small_bull_big_bull")
                clean_val = val.split()[-1].lower() if " " in val else val.lower()
                
                # Normalize: Redundant combos to 'all_*'
                clean_val = clean_val.replace("big_bull_small_bull", "all_bull")
                clean_val = clean_val.replace("big_bear_small_bear", "all_bear")
                clean_val = clean_val.replace("small_bull_big_bull", "all_bull")
                clean_val = clean_val.replace("small_bear_big_bear", "all_bear")
                
                # Normalize: Big First for cross combos
                clean_val = clean_val.replace("small_bear_big_bull", "big_bull_small_bear")
                clean_val = clean_val.replace("small_bull_big_bear", "big_bear_small_bull")
                
                if clean_val in ema_emoji_map:
                    emoji = ema_emoji_map[clean_val]
                    new_val = f"{emoji} {clean_val}"
                    new_ema_values.append([new_val])
                else:
                    new_ema_values.append([val])
            
            # Batch update Column E
            if new_ema_values:
                ws.update(values=new_ema_values, range_name=f"E3:E{2 + len(new_ema_values)}", value_input_option='USER_ENTERED')
                print(f"✅ Updated {len(new_ema_values)} EMA entries (Emojis + Normalization).")

        # --- NORMALIZE PUMP (Col F) & DUMP (Col G) FOR DROPDOWN COMPATIBILITY ---
        # Dropdowns are "1.0", "2.0"... but existing data might be "1", "2".
        print("🟢 Normalizing Pump/Dump columns for data validation...")
        
        # Normalize Pump (Col 6 -> F)
        pump_col = ws.col_values(6)[2:] # Skip 2 headers
        new_pump_values = []
        for val in pump_col:
            try:
                # Force 1 decimal place "2" -> "2.0", "2.5" -> "2.5"
                new_pump_values.append([f"{float(val):.1f}"])
            except:
                new_pump_values.append([val])
        if new_pump_values:
            ws.update(range_name=f"F3:F{2 + len(new_pump_values)}", values=new_pump_values, value_input_option='USER_ENTERED')
            
        # Normalize Dump (Col 7 -> G)
        dump_col = ws.col_values(7)[2:] # Skip 2 headers
        new_dump_values = []
        for val in dump_col:
            try:
                new_dump_values.append([f"{float(val):.1f}"])
            except:
                new_dump_values.append([val])
        if new_dump_values:
            ws.update(range_name=f"G3:G{2 + len(new_dump_values)}", values=new_dump_values, value_input_option='USER_ENTERED')

        print("✅ Normalized Pump/Dump values.")

        # Force fix L2:N2 headers (Win Rate, Trades, PnL)
        # Because previous code wrote them to M2:O2
        print("🔧 Fixing L2:N2 headers...")
        ws.update(range_name="L2:N2", values=[["Win Rate", "Trades", "PnL ($)"]], value_input_option='USER_ENTERED')

        # Get Headers to determine column types
        headers_r2 = ws.row_values(2)
        
        print("🎨 Refreshing Dropdowns...")
        sheets.apply_data_validation(ws)
        
        print("🎨 Applying formatting rules...")
        sheets.apply_sheet_formatting(ws, headers_r2)
        
        print("\n✨ Layout update complete! Check the sheet.")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error updating layout: {e}")

if __name__ == "__main__":
    update_layout()
