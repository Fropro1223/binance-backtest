import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def diagnose():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key("1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM")
    ws = sheet.worksheet("backtest1")
    
    r1 = ws.row_values(1)
    print(f"Row 1 length: {len(r1)}")
    print(f"Row 1 content: {r1}")
    
    r2 = ws.row_values(2)
    print(f"Row 2 length: {len(r2)}")
    
    # Check if there's anything in AA1 (index 27)
    if len(r1) >= 27:
        print(f"Value at AA1 (index 26): '{r1[26]}'")

if __name__ == "__main__":
    diagnose()
