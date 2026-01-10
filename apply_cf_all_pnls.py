import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from pathlib import Path
import time

def apply_cf_batch():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    if not os.path.exists(creds_path):
        print(f"❌ Credentials not found at {creds_path}")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID')
    if not manual_sheet_id:
        print("❌ MANUAL_SHEET_ID not set.")
        return

    try:
        sheet = client.open_by_key(manual_sheet_id)
        ws = sheet.worksheet("Analysis")
        
        print(f"🎨 Analyzing '{ws.title}' for PnL columns...")
        
        current_headers = ws.row_values(1)
        pnl_col_indices = []
        
        # 1. Total PnL (Column E, Index 4) - Check header just in case
        if len(current_headers) >= 5 and "PnL" in current_headers[4]:
             pnl_col_indices.append(4)
        
        # 2. Weekly PnLs (Headers containing '($)')
        for i, header in enumerate(current_headers):
            if "($)" in header:
                pnl_col_indices.append(i)
                
        print(f"Found {len(pnl_col_indices)} PnL columns: {pnl_col_indices}")
        
        # Construct Batch Requests
        requests = []
        
        for col_idx in pnl_col_indices:
            # Rule 1: > 0 Green
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "startColumnIndex": col_idx, "endColumnIndex": col_idx+1}],
                        "booleanRule": {
                            "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                            "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}
                        }
                    },
                    "index": 0
                }
            })
            # Rule 2: < 0 Red
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "startColumnIndex": col_idx, "endColumnIndex": col_idx+1}],
                        "booleanRule": {
                            "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                            "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}
                        }
                    },
                    "index": 1
                }
            })
            
        if requests:
            print(f"🚀 Sending batch update with {len(requests)} rules...")
            sheet.batch_update({"requests": requests})
            print("✅ Conditional Formatting applied successfully!")
        else:
            print("⚠️ No PnL columns found to format.")
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    apply_cf_batch()
