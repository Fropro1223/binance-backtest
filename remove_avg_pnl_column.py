import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from pathlib import Path

def remove_column():
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
        
        print(f"📄 Updating '{ws.title}' in '{sheet.title}'...")
        
        # Check if header matches expected "Avg PnL ($)"
        # Note: gspread uses 1-based indexing for rows/cols
        # Avg PnL was column 8 (H)
        
        try:
            val = ws.cell(1, 8).value
            if "Avg PnL" in val:
                print(f"🗑️  Deleting Column 8 (Header: {val})...")
                ws.delete_columns(8) # Deletes column H
                print("✅ Column deleted.")
            else:
                print(f"⚠️ Column 8 header is '{val}', not 'Avg PnL'. Skipping delete to be safe.")
        except Exception as e:
            print(f"⚠️ Could not check/delete column: {e}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    remove_column()
