import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import re

def fix_date_separator():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID', '1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM')
    sheet = client.open_by_key(manual_sheet_id)
    ws = sheet.worksheet("Analysis")
    
    print(f"🔄 Fixing Date Separators for '{ws.title}' Row 1...")
    
    headers = ws.row_values(1)
    updates = []
    
    # Pattern for DD.MM-DD.MM
    pattern = r'^(\d{2})\.(\d{2})-(\d{2})\.(\d{2})$'
    
    for i, val in enumerate(headers):
        match = re.match(pattern, val)
        if match:
            new_val = val.replace('.', '/')
            print(f"   [Col {i+1}] {val} -> {new_val}")
            updates.append({'range': gspread.utils.rowcol_to_a1(1, i+1), 'values': [[new_val]]})
    
    if updates:
        ws.batch_update(updates)
        print("✅ Date separators updated successfully.")
    else:
        print("ℹ️ No headers matched the '.' pattern.")

if __name__ == "__main__":
    fix_date_separator()
