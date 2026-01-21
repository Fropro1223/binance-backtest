import sys
import os
from pathlib import Path

# Add project root to path so we can import sheets.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sheets
from oauth2client.service_account import ServiceAccountCredentials
import gspread

def update_layout():
    """
    Connects to the 'Analysis' sheet and ensures layout/formatting is up to date.
    This includes:
    - Auto-resizing Column B
    - Applying Conditional Formatting (Side=Red/Green, Cond=Green/Red)
    - Coloring data columns (white/gray/blue)
    - Hiding Timestamp column
    """
    print("🚀 Connecting to Google Sheets to update layout...")
    
    try:
        creds_path = sheets.get_credentials_path()
        if not creds_path.exists():
             print(f"❌ Credentials not found at {creds_path}")
             return

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
        client = gspread.authorize(creds)
        
        # Use MANUAL_SHEET_ID env var if set, otherwise fallback to MASTER_SHEET_ID
        manual_sheet_id = os.environ.get('MANUAL_SHEET_ID')
        MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
        sheet_id = manual_sheet_id if manual_sheet_id else MASTER_SHEET_ID

        sheet = client.open_by_key(sheet_id)
        print(f"✅ Opened Sheet: {sheet.title}")
        
        ws = sheet.worksheet("Analysis")
        print(f"✅ Opened 'Analysis' tab")
        
        # Get Headers to determine column types
        headers_r2 = ws.row_values(2)
        
        print("🎨 Applying formatting rules...")
        sheets.apply_sheet_formatting(ws, headers_r2)
        
        print("\n✨ Layout update complete! Check the sheet.")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error updating layout: {e}")

if __name__ == "__main__":
    update_layout()
