import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def clear_cde_headers():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID', '1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM')
    sheet = client.open_by_key(manual_sheet_id)
    ws = sheet.worksheet("Analysis")
    
    print(f"🔄 Clearing C, D, E Headers for '{ws.title}'...")
    
    # Update Range A1:E1 to ensure C, D, E are empty
    # A=Timestamp, B=Strategy, C="", D="", E=""
    ws.update(range_name="C1:E1", values=[["", "", ""]])
    
    print("✅ C, D, E headers cleared successfully.")

if __name__ == "__main__":
    clear_cde_headers()
