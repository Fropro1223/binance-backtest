import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import re
from datetime import datetime

def update_week_numbers():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID', '1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM')
    sheet = client.open_by_key(manual_sheet_id)
    ws = sheet.worksheet("Analysis")
    
    print(f"🔄 Updating Week Numbers for '{ws.title}' Row 1...")
    
    headers = ws.row_values(1)
    new_headers = []
    updated = False
    
    # Pattern for DD.MM-DD.MM
    pattern = r'^(\d{2}\.\d{2})-(\d{2}\.\d{2})$'
    
    for i, val in enumerate(headers):
        match = re.match(pattern, val)
        if match:
            start_str = match.group(1)
            # Assume 2026 for now as the data is from Jan 2026
            try:
                # We need to be careful about the year. 
                # Since we are in Jan 2026, and data is recent.
                d = datetime.strptime(f"{start_str}.2026", "%d.%m.%Y")
                week_num = d.isocalendar()[1]
                new_val = f"W{week_num:02d} ({val})"
                
                if new_val != val:
                    print(f"   [Col {i+1}] {val} -> {new_val}")
                    ws.update_cell(1, i+1, new_val)
                    updated = True
            except Exception as e:
                print(f"   Error parsing {val}: {e}")
    
    if updated:
        print("✅ Week numbers updated successfully.")
    else:
        print("ℹ️ No headers matched the pattern or already updated.")

if __name__ == "__main__":
    update_week_numbers()
