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

        # Current structure after removing Avg PnL (Col 8):
        # 1:Timestamp, 2:Strategy, 3:TP%, 4:SL%, 5:WinRate, 6:Trades, 7:TotalPnL, 8:BestTrade, 9:WorstTrade
        # We want to remove 8 and 9.
        # It's safer to delete column 8 twice, or check headers.
        
        # Check header of Col 8
        try:
            val8 = ws.cell(1, 8).value
            if "Best Trade" in str(val8):
                print(f"🗑️  Deleting Column 8 (Header: {val8})...")
                ws.delete_columns(8) # Deletes 'Best Trade'
                print("✅ 'Best Trade' deleted.")
                
                # Now 'Worst Trade' should be at Col 8 (shifted left)
                val8_new = ws.cell(1, 8).value
                if "Worst Trade" in str(val8_new):
                    print(f"🗑️  Deleting Column 8 (Header: {val8_new})...")
                    ws.delete_columns(8) # Deletes 'Worst Trade'
                    print("✅ 'Worst Trade' deleted.")
                else:
                    print(f"⚠️ Expected 'Worst Trade' at Col 8 but found '{val8_new}'. Check manually.")
            
            else:
                 # Be defensive, maybe 'Worst Trade' is at 9 if something shifted weirdly.
                 print(f"⚠️ Column 8 is '{val8}', expected 'Best Trade'. Skipping specific delete.")
                 
                 # Check Col 9 just in case
                 val9 = ws.cell(1, 9).value
                 if "Worst Trade" in str(val9):
                     print(f"🗑️  Deleting Column 9 (Header: {val9})...")
                     ws.delete_columns(9)
                     print("✅ 'Worst Trade' deleted from Col 9.")

        except Exception as e:
            print(f"⚠️ Error checking headers: {e}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    remove_columns()
