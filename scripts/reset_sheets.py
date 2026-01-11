import sys
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path

DEFAULT_CREDS_PATH = os.path.expanduser("~/.secrets/binance-backtest-sa.json")

def get_credentials_path():
    env_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if env_path: return Path(env_path)
    return Path(DEFAULT_CREDS_PATH)

def reset_sheet():
    print("🧨 Starting Complete Sheet Reset...")
    
    creds_path = get_credentials_path()
    if not creds_path.exists():
        print(f"❌ Credentials not found at {creds_path}")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    # Open "Analysis" tab
    sheet_id = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    try:
        sheet = client.open_by_key(sheet_id)
    except Exception as e:
        print(f"❌ Could not open sheet: {e}")
        return

    # 1. DELETE 'Analysis' if exists
    try:
        ws = sheet.worksheet("Analysis")
        print("🗑️ Deleting existing 'Analysis' tab...")
        sheet.del_worksheet(ws)
    except gspread.WorksheetNotFound:
        print("ℹ️ 'Analysis' tab not found (already deleted?)")

    # 2. CREATE 'Analysis'
    print("✨ Creating new 'Analysis' tab...")
    ws = sheet.add_worksheet(title="Analysis", rows=1000, cols=50)

    # 3. SETUP HEADERS
    print("📝 Setting up base headers...")
    
    # Row 1: High Level Metrics
    headers_r1 = ["Timestamp", "Strategy", "Win Rate %", "Total Trades", "Total PnL ($)"]
    # Fix DeprecationWarning by using named args
    ws.update(range_name="A1:E1", values=[headers_r1])
    
    # 4. FORMATTING
    print("🎨 Applying Base Formatting...")
    
    try:
        # Row 1: Bold, Center, White
        ws.format("A1:AZ1", {
            "textFormat": {"bold": True},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE"
        })
        
        # Freeze 2 rows
        ws.freeze(rows=2)
    except Exception as e:
        print(f"⚠️ Formatting Warning: {e}")
    
    # Hide Timestamp (Col A)
    try:
        sheet.batch_update({
        "requests": [{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
            "properties": {"hiddenByUser": True},
            "fields": "hiddenByUser"
        }}]
        })
    except: pass

    # Column Widths
    try:
        # Try different methods or just skip if fails
        ws.set_column_width(1, 250) 
    except Exception as e:
        print(f"⚠️ Width Warning: {e}")
    
    print("✅ Reset Complete. Sheet is ready for fresh data.")

if __name__ == "__main__":
    reset_sheet()
