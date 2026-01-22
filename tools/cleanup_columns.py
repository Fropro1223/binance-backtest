#!/usr/bin/env python3
"""
Cleanup script to fix the duplicate 5s column issue.
Deletes column O (the extra 5s column) and shifts remaining data left.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gspread
from oauth2client.service_account import ServiceAccountCredentials

def cleanup_duplicate_column():
    print("🔧 Connecting to Google Sheets for column cleanup...")
    
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    sheet = client.open_by_key(MASTER_SHEET_ID)
    ws = sheet.worksheet("Analysis")
    print("✅ Opened 'Analysis' tab")
    
    # Check current headers
    headers_r1 = ws.row_values(1)
    headers_r2 = ws.row_values(2)
    
    print(f"📊 Current Row 1 headers (cols 14-20): {headers_r1[13:20]}")
    print(f"📊 Current Row 2 headers (cols 14-20): {headers_r2[13:20]}")
    
    # Find duplicate 5s columns
    # Expected: N(14)=5s, O(15)=5s PnL label or empty
    # Current: N(14)=5s Trades, O(15)=5s Trades (DUPLICATE)
    
    # Delete column O (index 14, which is column 15 in 1-indexed)
    # This will shift all subsequent columns left by 1
    
    print("🗑️ Deleting duplicate column O (5s duplicate)...")
    
    # Use batchUpdate to delete a column
    delete_request = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": 14,  # O column (0-indexed = 14)
                        "endIndex": 15
                    }
                }
            }
        ]
    }
    
    ws.spreadsheet.batch_update(delete_request)
    print("✅ Deleted duplicate column O")
    
    # Verify new structure
    headers_r1 = ws.row_values(1)
    headers_r2 = ws.row_values(2)
    print(f"📊 New Row 1 headers (cols 14-20): {headers_r1[13:20]}")
    print(f"📊 New Row 2 headers (cols 14-20): {headers_r2[13:20]}")
    
    print("✨ Cleanup complete!")

if __name__ == "__main__":
    cleanup_duplicate_column()
