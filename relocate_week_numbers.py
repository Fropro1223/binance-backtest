import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import re
from datetime import datetime

def update_week_numbers_relocate():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID', '1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM')
    sheet = client.open_by_key(manual_sheet_id)
    ws = sheet.worksheet("Analysis")
    
    print(f"🔄 Relocating Week Numbers for '{ws.title}' Row 1...")
    
    headers = ws.row_values(1)
    
    # Pattern for WXX (DD.MM-DD.MM)
    pattern_new = r'^W(\d{2}) \((\d{2}\.\d{2}-\d{2}\.\d{2})\)$'
    # Pattern for DD.MM-DD.MM
    pattern_old = r'^(\d{2}\.\d{2})-(\d{2}\.\d{2})$'
    
    updates = []
    
    for i, val in enumerate(headers):
        match_new = re.match(pattern_new, val)
        if match_new:
            wn = match_new.group(1)
            date_range = match_new.group(2)
            print(f"   [Col {i+1}] {val} -> Date: {date_range}, Week: W{wn}")
            # Col i+1: Date, Col i+2: Week
            updates.append({'range': gspread.utils.rowcol_to_a1(1, i+1), 'values': [[date_range]]})
            updates.append({'range': gspread.utils.rowcol_to_a1(1, i+2), 'values': [[f"W{wn}"]]})
        
        elif re.match(pattern_old, val):
            # Already split or needs split if we find W sitting elsewhere.
            # But let's assume we just need to calculate W if not there.
            try:
                d = datetime.strptime(f"{val.split('-')[0]}.2026", "%d.%m.%Y")
                week_num = d.isocalendar()[1]
                print(f"   [Col {i+1}] {val} -> Adding W{week_num:02d} to next cell")
                updates.append({'range': gspread.utils.rowcol_to_a1(1, i+2), 'values': [[f"W{week_num:02d}"]]})
            except: pass

    if updates:
        ws.batch_update(updates)
        print("✅ Week numbers relocated successfully.")
    else:
        print("ℹ️ No headers matched the patterns.")

if __name__ == "__main__":
    update_week_numbers_relocate()
