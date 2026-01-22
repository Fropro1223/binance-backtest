import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sheets import get_credentials_path, apply_sheet_formatting

def update_formatting():
    creds_path = get_credentials_path()
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    sheet = client.open_by_key(MASTER_SHEET_ID)
    
    try:
        ws = sheet.worksheet("backtest1")
        print(f"Applying formatting to backtest1...")
        headers_r2 = ws.row_values(2)
        apply_sheet_formatting(ws, headers_r2)
        print("✅ Formatting applied successfully.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    update_formatting()
