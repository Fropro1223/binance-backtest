import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

def fix_green_text():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    ss = client.open_by_key(MASTER_SHEET_ID)
    ws = ss.worksheet("backtest1")
    
    print("🎨 AGGRESSIVELY Fixing ALL Text to BLACK (Columns O onwards)...")
    
    requests = []
    
    # Black Text Format
    black_text = {"textFormat": {"foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}}
    
    # Target EVERY column from O (index 14) to BZ (index 77) 
    # This covers all timeframe and weekly columns regardless of header detection
    for col_idx in range(14, 78):
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id, 
                    "startRowIndex": 2,  # Start from data rows (skip headers)
                    "endRowIndex": 5000, 
                    "startColumnIndex": col_idx, 
                    "endColumnIndex": col_idx + 1
                },
                "cell": {"userEnteredFormat": black_text},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        })
    
    # Also fix columns L, M, N (indices 11, 12, 13) for good measure
    for col_idx in [11, 12, 13]:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id, 
                    "startRowIndex": 2, 
                    "endRowIndex": 5000, 
                    "startColumnIndex": col_idx, 
                    "endColumnIndex": col_idx + 1
                },
                "cell": {"userEnteredFormat": black_text},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        })

    print(f"   Sending {len(requests)} formatting requests...")
    ss.batch_update({"requests": requests})
    print("✅ Done. ALL data columns (L onwards) set to black text.")

if __name__ == "__main__":
    fix_green_text()
