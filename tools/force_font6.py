
import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

def force_font_size():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    ss = client.open_by_key(MASTER_SHEET_ID)
    ws = ss.worksheet("backtest1")
    
    print("Trying ws.format() for Row 1-2...")
    try:
        # Use gspread's format method which is more robust
        ws.format("1:2", {
            "textFormat": {
                "fontSize": 6,
                "bold": True
            },
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
        })
        print("✅ ws.format() successful.")
    except Exception as e:
        print(f"❌ ws.format() failed: {e}")

if __name__ == "__main__":
    force_font_size()
