#!/usr/bin/env python3
"""
Clears all conditional formatting rules from the 'backtest1' worksheet.
This is used to clean up orphaned rules after column swaps.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gspread
from oauth2client.service_account import ServiceAccountCredentials

def clear_cf_rules():
    print("🚀 Connecting to Google Sheets to clear CF rules...")
    
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    sheet = client.open_by_key(MASTER_SHEET_ID)
    ws = sheet.worksheet("backtest1")
    
    # In Google Sheets API, clearing conditional formatting involves a batch_update
    # with 'deleteConditionalFormatRule' for each rule index.
    # However, it's easier to just overwrite them or clear the entire range's formatting
    # via 'clearConditionalFormatting' if available in gspread.
    
    # gspread itself doesn't have a direct 'clear_conditional_formatting' method, 
    # so we use batch_update.
    
    # Fetch current rules to know how many to delete
    # or just use a dummy large number of deletes? 
    # Better to get the count.
    
    metadata = ws.spreadsheet.fetch_sheet_metadata()
    sheet_data = next(s for s in metadata['sheets'] if s['properties']['title'] == "backtest1")
    cf_rules = sheet_data.get('conditionalFormats', [])
    
    if not cf_rules:
        print("✅ No conditional formatting rules found.")
        return

    print(f"🧹 Found {len(cf_rules)} rules. Deleting...")
    
    requests = []
    # Deleting rules by index. When you delete index 0, the next rule shifts to 0.
    # So we can just send N delete requests for index 0.
    for _ in range(len(cf_rules)):
        requests.append({
            "deleteConditionalFormatRule": {
                "sheetId": ws.id,
                "index": 0
            }
        })
        
    if requests:
        ws.spreadsheet.batch_update({"requests": requests})
        print(f"✅ Successfully cleared {len(cf_rules)} conditional formatting rules.")

if __name__ == "__main__":
    clear_cf_rules()
