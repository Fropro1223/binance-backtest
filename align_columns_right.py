import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from pathlib import Path

def align_columns():
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
        
        print(f"🎨 Aligning columns in '{ws.title}'...")
        
        # Column D: Number + Right
        print("   Column D -> #,##0 + Right Align")
        ws.format("D2:D1000", {
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
            "horizontalAlignment": "RIGHT"
        })
        
        # Column E: Right Align
        print("   Column E -> Right Align")
        ws.format("E2:E1000", {
            "horizontalAlignment": "RIGHT"
        })
        
        print("✅ Alignment complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    align_columns()
