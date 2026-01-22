import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

def fix_dropdowns():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    ss = client.open_by_key(MASTER_SHEET_ID)
    ws = ss.worksheet("backtest1")
    
    print("🔧 Fixing Dropdowns for 'backtest1'...")
    
    # Dropdown configurations (0-indexed column -> options)
    DROPDOWNS = {
        2: {"name": "Side", "options": ["SHORT", "LONG"]},  # C
        3: {"name": "Cond", "options": ["pump", "dump"]},   # D
        4: {"name": "Threshold%", "options": ["3.0", "2.5", "2.0", "1.5", "1.0", "-1.0", "-1.5", "-2.0", "-2.5", "-3.0"]},  # E
        5: {"name": "EMA", "options": ["⚪ none", "🔴🔴🔴 all_bear", "🟢🟢🟢 all_bull", "🔴🔴 big_bear", "🟢🟢 big_bull", "🔴 small_bear", "🟢 small_bull", "🔴🔴🟢 big_bear_small_bull", "🟢🟢🔴 big_bull_small_bear"]},  # F
        6: {"name": "TP%", "options": [str(x) for x in range(10, 0, -1)]},  # G
        7: {"name": "SL%", "options": [str(x) for x in range(10, 0, -1)]},  # H
        8: {"name": "TSL%", "options": [str(x) for x in range(10, 0, -1)] + ["OFF"]},  # I
        9: {"name": "Maru", "options": ["0.9", "0.8", "0.7", "0.6", "0.5"]},  # J
    }
    
    requests = []
    
    for col_idx, config in DROPDOWNS.items():
        # Build data validation request
        requests.append({
            "setDataValidation": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 2,  # Row 3 (0-indexed)
                    "endRowIndex": 5000,
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": opt} for opt in config["options"]]
                    },
                    "showCustomUi": True,
                    "strict": False
                }
            }
        })
        print(f"   📝 {config['name']} (Column {chr(ord('A') + col_idx)})")
    
    # Send batch update
    ss.batch_update({"requests": requests})
    print("✅ Dropdowns Fixed.")

if __name__ == "__main__":
    fix_dropdowns()
