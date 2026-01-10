import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from pathlib import Path

def final_polish():
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
        
        print(f"🎨 Polishing '{ws.title}'...")
        
        # 1. Center Headers
        print("   headers -> Bold + Center")
        ws.format("A1:Z1", {
            "textFormat": {"bold": True},
            "horizontalAlignment": "CENTER"
        })
        
        # 2. Center Win Rate (Col C)
        print("   Win Rate (Col C) -> Center")
        ws.format("C2:C1000", {"horizontalAlignment": "CENTER"})
        
        # 3. Hide Timestamp (Col A)
        print("   Timestamp (Col A) -> Hiding...")
        # FIX: Send request to SHEET, not WORKSHEET
        sheet.batch_update({
            "requests": [{
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": 1
                    },
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser"
                }
            }]
        })
        
        # 4. Auto Resize
        print("   Auto-resizing columns A-E...")
        try:
            ws.columns_auto_resize(0, 5)
        except AttributeError:
             print("   ⚠️ columns_auto_resize not supported in this gspread version, skipping.")
        except Exception as e:
             print(f"   ⚠️ Resize failed: {e}")
        
        print("✅ Polish complete!")
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    final_polish()
