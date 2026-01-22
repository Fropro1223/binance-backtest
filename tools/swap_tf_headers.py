#!/usr/bin/env python3
"""
Fix headers by swapping Trades/PnL order to match actual data.
Current data has: [PnL values, Trades values] under [Trades, PnL] headers.
Fix: Change headers to [PnL, Trades] to match data.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gspread
from oauth2client.service_account import ServiceAccountCredentials

def swap_tf_headers():
    print("🔧 Connecting to Google Sheets to swap TF headers...")
    
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    sheet = client.open_by_key(MASTER_SHEET_ID)
    ws = sheet.worksheet("Analysis")
    print("✅ Opened 'Analysis' tab")
    
    # Current headers are [Trades, PnL] but data is [PnL, Trades]
    # So swap to [PnL, Trades]
    
    tf_order = ['5s', '10s', '15s', '30s', '45s', '1m']
    
    header_r1 = []
    header_r2 = []
    
    for tf in tf_order:
        header_r1.extend([tf, ''])  # TF label, then empty
        header_r2.extend(['PnL', 'Trades'])  # SWAPPED order to match data
    
    print(f"📝 Setting Row 1 (TF labels) from O: {header_r1}")
    print(f"📝 Setting Row 2 (PnL/Trades - SWAPPED) from O: {header_r2}")
    
    ws.update(range_name=f"O1:Z1", values=[header_r1], value_input_option='USER_ENTERED')
    ws.update(range_name=f"O2:Z2", values=[header_r2], value_input_option='USER_ENTERED')
    
    print("✅ Headers swapped!")
    
    # Re-apply formatting with corrected logic
    print("🎨 Re-applying layout formatting...")
    import sheets
    
    headers_r2_final = ws.row_values(2)
    sheets.apply_sheet_formatting(ws, headers_r2_final)
    
    print("✨ Fix complete! Check the sheet.")

if __name__ == "__main__":
    swap_tf_headers()
