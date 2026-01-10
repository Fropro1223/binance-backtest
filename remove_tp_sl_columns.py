import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from pathlib import Path

def remove_columns():
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

        # Structure: 1:Timestamp, 2:Strategy, 3:TP%, 4:SL%, 5:WinRate...
        # We want to remove 3 and 4.
        
        # Check Col 3
        try:
            val3 = ws.cell(1, 3).value
            if "TP" in str(val3):
                print(f"🗑️  Deleting Column 3 (Header: {val3})...")
                ws.delete_columns(3) 
                print("✅ 'TP %' deleted.")
                
                # Now SL% should be at Col 3 (shifted left)
                val3_new = ws.cell(1, 3).value
                if "SL" in str(val3_new):
                    print(f"🗑️  Deleting Column 3 (Header: {val3_new})...")
                    ws.delete_columns(3)
                    print("✅ 'SL %' deleted.")
                else:
                    print(f"⚠️ Expected 'SL %' at Col 3 but found '{val3_new}'. Check manually.")
            
            else:
                 print(f"⚠️ Column 3 is '{val3}', expected 'TP %'. Skipping.")

        except Exception as e:
            print(f"⚠️ Error checking headers: {e}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    remove_columns()
