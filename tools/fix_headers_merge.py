#!/usr/bin/env python3
"""
Fixes the double header row issue by merging vertical cells for parameters
and horizontal cells for Timeframe labels.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import sheets # Import to verify we use consistent logic

def fix_headers():
    print("🚀 Connecting to Google Sheets to fix headers...")
    
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    sheet = client.open_by_key(MASTER_SHEET_ID)
    ws = sheet.worksheet("backtest1")
    
    # 1. Define Headers
    # Parameters (Cols A-N) -> Vertical Merge (Row 1 & 2)
    param_headers = [
        "Timestamp", "Strategy", "Side", "Cond", "Threshold%", "EMA", 
        "TP%", "SL%", "TSL%", "Maru", "Days", "Win Rate", "Trades", "PnL ($)"
    ] # 14 columns
    
    # TF Headers (Cols O-Z) -> Horizontal Merge in Row 1, Sub-headers in Row 2
    tf_labels = ["5s", "10s", "15s", "30s", "45s", "1m"] # 6 labels, 12 columns
    tf_subheaders = ["Trades", "PnL"] * 6 # [Trades, PnL, Trades, PnL...]
    
    # 2. Clear Headers
    print("🧹 Clearing Rows 1 & 2...")
    ws.batch_clear(["A1:AZ2"])
    
    # 3. Write Row 1
    # Parameters
    row1_data = param_headers + [] # Will extend with TF labels
    
    # TF Labels need to be spaced: "5s", "", "10s", ""
    tf_row1_part = []
    for label in tf_labels:
        tf_row1_part.append(label)
        tf_row1_part.append("") # Empty for merge
    
    full_row1 = row1_data + tf_row1_part
    ws.update(range_name="A1", values=[full_row1], value_input_option='USER_ENTERED')
    
    # 4. Write Row 2 (Subheaders only for TFs)
    # Parameters empty (for merge), TFs have PnL/Trades
    row2_data = [""] * len(param_headers) + tf_subheaders
    ws.update(range_name="A2", values=[row2_data], value_input_option='USER_ENTERED')
    
    # 5. Perform Merges
    print("🔗 Merging cells...")
    
    merge_requests = []
    
    # Merge Parameters Vertically (A1:A2, B1:B2 ... N1:N2)
    # Columns 0 to 13 (A to N)
    for col_idx in range(len(param_headers)):
        merge_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 0, "endRowIndex": 2, # Rows 1-2
                    "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1
                },
                "mergeType": "MERGE_ALL"
            }
        })
        
    # Merge TF Labels Horizontally (O1:P1, Q1:R1 ...)
    # Start Column index 14 (O)
    current_col = 14
    for _ in tf_labels:
        merge_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 0, "endRowIndex": 1, # Row 1 only
                    "startColumnIndex": current_col, "endColumnIndex": current_col + 2
                },
                "mergeType": "MERGE_ALL"
            }
        })
        current_col += 2
        
    # Apply Merges
    if merge_requests:
        ws.spreadsheet.batch_update({"requests": merge_requests})
        
    # 6. Apply Formatting (Center alignment for merged headers)
    print("🎨 Re-applying formatting...")
    # Get just the row 2 values effectively to pass to apply logic, 
    # though apply_sheet_formatting usually reads Row 2.
    # We should ensure Row 2 has the correct values for logic.
    # Our Row 2 has empty strings for parameters due to merge.
    # sheets.py logic relies on `headers_r2` for some things.
    # Let's check sheets.py... it skips A-N usually.
    
    # We need to ensure parameters are centered vertically and horizontally.
    fmt_requests = []
    # Center all headers (A1:AZ2)
    fmt_requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 50},
            "cell": {"userEnteredFormat": {
                "horizontalAlignment": "CENTER", 
                "verticalAlignment": "MIDDLE",
                "textFormat": {"bold": True}
            }},
            "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
        }
    })
    ws.spreadsheet.batch_update({"requests": fmt_requests})

    # Call main formatting to restore colors etc.
    # Pass a constructed header row that matches what sheets.py expects
    # sheets.py expects Row 2 to contain "Trades" or "PnL" for TFs.
    # Row 2 currently has ["", "", ... "PnL", "Trades"...] -> This works for TFs.
    # But it might be confused by empty parameters?
    # sheets.py: `if c_idx <= 14: continue` -> Skips A-N. Good.
    
    headers_r2 = ws.row_values(2)
    sheets.apply_sheet_formatting(ws, headers_r2)
    
    print("✅ Headers fixed and merged!")

if __name__ == "__main__":
    fix_headers()
