import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def fix_formatting_final():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID', '1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM')
    sheet = client.open_by_key(manual_sheet_id)
    ws = sheet.worksheet("Analysis")
    
    print(f"🧼 EMERGENCY RESET for '{ws.title}'...")
    
    color_white = {"red": 1.0, "green": 1.0, "blue": 1.0}
    color_gray = {"red": 0.93, "green": 0.93, "blue": 0.93} # Light gray
    
    requests = []
    
    # 1. MAKE ALL DATA ROWS (Row 3+) PURE WHITE
    # Range: Row 3 to 2000, Col A (0) to AZ (52)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": 2, "endRowIndex": 2000, # Start from Row 3 (Index 2)
                "startColumnIndex": 0, "endColumnIndex": 60
            },
            "cell": {"userEnteredFormat": {"backgroundColor": color_white}},
            "fields": "userEnteredFormat.backgroundColor"
        }
    })
    
    # 2. APPLY COLUMN COLORS *ONLY* TO HEADERS (Row 1-2)
    # This keeps existing logic but limits vertical height
    headers_row2 = ws.row_values(2)
    week_pair_index = 0
    
    for i, val in enumerate(headers_row2):
        col_idx = i + 1
        if col_idx < 18:
            current_color = color_white
        else:
            if val == "Trades":
                current_color = color_white if week_pair_index % 2 == 0 else color_gray
                week_pair_index += 1
            elif val == "PnL":
                current_color = color_white if (week_pair_index - 1) % 2 == 0 else color_gray
            else:
                current_color = color_white
        
        # Apply ONLY to Row 1 and 2 (Indices 0 and 1)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 0, "endRowIndex": 2, # Rows 1 & 2 only
                    "startColumnIndex": i, "endColumnIndex": i+1
                },
                "cell": {"userEnteredFormat": {"backgroundColor": current_color}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    print(f"Sending {len(requests)} clean-up requests...")
    sheet.batch_update({"requests": requests})
    print("✅ DATA ROWS ARE WHITE. HEADERS COLORED.")

if __name__ == "__main__":
    fix_formatting_final()
