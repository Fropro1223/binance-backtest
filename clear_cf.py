import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def clear_all_cf():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    ss = client.open_by_key(MASTER_SHEET_ID)
    ws = ss.worksheet("backtest1")
    
    # 1. Get total count
    meta = ss.fetch_sheet_metadata({'includeGridData': False})
    sheet_meta = [s for s in meta['sheets'] if s['properties']['title'] == "backtest1"][0]
    cf_rules = sheet_meta.get('conditionalFormats', [])
    total = len(cf_rules)
    print(f"Found {total} conditional formatting rules. Clearing...")
    
    if total == 0:
        print("Done.")
        return

    # 2. Delete in batches of 500
    sheet_id = ws.id
    while total > 0:
        batch_size = min(total, 500)
        requests = []
        for _ in range(batch_size):
            # Always delete index 0
            requests.append({
                "deleteConditionalFormatRule": {
                    "index": 0,
                    "sheetId": sheet_id
                }
            })
        
        print(f"Sending batch delete of {batch_size} rules... (Remaining: {total-batch_size})")
        ss.batch_update({"requests": requests})
        total -= batch_size

    print("✅ All conditional formatting rules cleared.")

if __name__ == "__main__":
    clear_all_cf()
