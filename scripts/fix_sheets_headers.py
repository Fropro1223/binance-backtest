import sys
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path

# Credentials setup
DEFAULT_CREDS_PATH = os.path.expanduser("~/.secrets/binance-backtest-sa.json")

def get_credentials_path():
    env_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if env_path: return Path(env_path)
    return Path(DEFAULT_CREDS_PATH)

def fix_headers():
    print("🔧 Starting Sheets Header Repair...")
    
    creds_path = get_credentials_path()
    if not creds_path.exists():
        print(f"❌ Credentials not found at {creds_path}")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    # Open "Analysis" tab
    sheet_id = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    sheet = client.open_by_key(sheet_id)
    ws = sheet.worksheet("Analysis")
    print(f"✅ Opened Sheet: {sheet.title} -> Analysis")
    
    # 1. Unmerge Entire Header Row (A1:AZ2)
    print("🧹 Unmerging Row 1 and 2...")
    ws.unmerge_cells("A1:AZ2")
    
    # 2. Reset Formatting
    print("🎨 Resetting format...")
    ws.format("A1:AZ2", {
        "textFormat": {"bold": True},
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE",
        "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0} 
    })
    
    # 3. CRITICAL: Clear all weekly columns (F onwards) to prevent shifting/debris
    print("🗑️ Clearing old weekly data (F1:AZ1000) to fix alignment...")
    ws.batch_clear(["F1:AZ1000"])
    
    # Reimpose basic headers if needed
    ws.update("A1:E1", [["Timestamp", "Strategy", "Win Rate %", "Total Trades", "Total PnL ($)"]])
    
    print("✅ Repair Complete. Headers unmerged and Weekly columns cleared.")

if __name__ == "__main__":
    fix_headers()
