import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

def fix_black_columns():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    ss = client.open_by_key(MASTER_SHEET_ID)
    ws = ss.worksheet("backtest1")
    
    print("🎨 Force-Resetting Columns L, M, N, P to White Background & Black Text...")
    
    requests = []
    
    # Range: Columns L(11) to P(15) | Data: Row 3 onwards
    data_range = {
        "sheetId": ws.id,
        "startRowIndex": 2, # Row 3
        "endRowIndex": 5000,
        "startColumnIndex": 11, # Column L
        "endColumnIndex": 17    # Column P+1
    }
    
    # 1. Reset everything to standard White BG and Black Text
    requests.append({
        "repeatCell": {
            "range": data_range,
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                    "textFormat": {"foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
        }
    })
    
    # 2. Clear any conflicting Conditional Formatting for these columns
    # Actually, it's safer to clear ALL CF and re-run restore_visuals.py
    # But let's just re-apply gradients at the end of this script to be sure.
    
    print("🎨 Re-applying Safe Gradients...")
    
    # Column L: Win Rate (Red -> White -> Green)
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 5000, "startColumnIndex": 11, "endColumnIndex": 12}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 0.98, "green": 0.82, "blue": 0.82}, "type": "MIN"},
                    "midpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "NUMBER", "value": "0.5"},
                    "maxpoint": {"color": {"red": 0.85, "green": 0.94, "blue": 0.85}, "type": "MAX"}
                }
            },
            "index": 0
        }
    })
    
    # Column M: Trades (White -> Blue)
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 5000, "startColumnIndex": 12, "endColumnIndex": 13}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "MIN"},
                    "maxpoint": {"color": {"red": 0.85, "green": 0.9, "blue": 1.0}, "type": "MAX"}
                }
            },
            "index": 0
        }
    })
    
    # Column N: Gross PnL (Red -> White -> Green)
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 5000, "startColumnIndex": 13, "endColumnIndex": 14}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 0.98, "green": 0.82, "blue": 0.82}, "type": "MIN"},
                    "midpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "NUMBER", "value": "0"},
                    "maxpoint": {"color": {"red": 0.85, "green": 0.94, "blue": 0.85}, "type": "MAX"}
                }
            },
            "index": 0
        }
    })

    # Column P: Net PnL (Red -> White -> Green)
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 5000, "startColumnIndex": 15, "endColumnIndex": 16}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 0.98, "green": 0.82, "blue": 0.82}, "type": "MIN"},
                    "midpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "NUMBER", "value": "0"},
                    "maxpoint": {"color": {"red": 0.85, "green": 0.94, "blue": 0.85}, "type": "MAX"}
                }
            },
            "index": 0
        }
    })

    print("🚀 Sending Batch Update...")
    ss.batch_update({"requests": requests})
    print("✅ Sheet Visuals Fixed. Please check M and N columns.")

if __name__ == "__main__":
    fix_black_columns()
