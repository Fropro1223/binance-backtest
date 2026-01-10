import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from pathlib import Path

def apply_format():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    if not os.path.exists(creds_path):
        print(f"❌ Credentials not found at {creds_path}")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID')
    if not manual_sheet_id:
        print("❌ MANUAL_SHEET_ID not set.")
        return

    try:
        sheet = client.open_by_key(manual_sheet_id)
        ws = sheet.worksheet("Analysis")
        
        print(f"🎨 Formatting '{ws.title}' in '{sheet.title}'...")
        
        # Freeze Row 1
        ws.freeze(rows=1)
        print("   ❄️  Row 1 frozen")
        
        # Bold Row 1
        ws.format("A1:Z1", {"textFormat": {"bold": True}})
        print("   bold Row 1 bolded")
        
        # Filter
        ws.set_basic_filter("A1:Z1000")
        print("   🔍 Filter enabled")
        
        print("✅ Formatting complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    apply_format()
