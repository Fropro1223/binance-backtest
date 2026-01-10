import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from pathlib import Path

def format_column():
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
        
        # Check Header
        header_d = ws.cell(1, 4).value
        print(f"   Column D Header: {header_d}")
        
        # Format Column D (Total Trades) with thousands separator
        print("   Formatting Column D as '#,##0'...")
        ws.format("D2:D1000", {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})
        
        print("✅ Formatting complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    format_column()
