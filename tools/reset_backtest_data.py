#!/usr/bin/env python3
"""
Resets backtest data and dynamic headers to fix column misalignment issues.
Clears A3:ZZ1000 (Data) and AA1:ZZ2 (Dynamic Headers).
Retains A1:Z2 (Static Headers).
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gspread
from oauth2client.service_account import ServiceAccountCredentials

def reset_data():
    print("🚀 Connecting to Google Sheets to reset data...")
    
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    sheet = client.open_by_key(MASTER_SHEET_ID)
    ws = sheet.worksheet("backtest1")
    
    print("🧹 Clearing all columns after Z (AA1:ZZ1000)...")
    ws.batch_clear(["AA1:ZZ1000"])
    
    print("🧹 Clearing Data (Rows 3+)...")
    ws.batch_clear(["A3:Z1000"])
    
    print("✅ backtest1 reset complete. Ready for fresh backtest.")

if __name__ == "__main__":
    reset_data()
