import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from pathlib import Path

def format_pnl():
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
        
        print(f"🎨 Formatting PnL in '{ws.title}'...")
        
        # Check Header
        header_e = ws.cell(1, 5).value
        print(f"   Column E Header: {header_e}")
        
        # Format Column E (Total PnL) with thousands separator and right align
        # pattern "#,##0" displays integer
        print("   Formatting Column E as '#,##0' + Right Align...")
        ws.format("E2:E1000", {
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
            "horizontalAlignment": "RIGHT"
        })
        
        print("✅ Formatting complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    format_pnl()
