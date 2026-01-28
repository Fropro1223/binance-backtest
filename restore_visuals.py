import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

def restore_visuals():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    ss = client.open_by_key(MASTER_SHEET_ID)
    ws = ss.worksheet("backtest1")
    
    print("🎨 Restoring Visuals (Conditional Formatting) for 'backtest1'...")

    # Define Gradients
    requests = []
    
    # 1. TP% (Column G / Index 6) - Green Scale
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 6, "endColumnIndex": 7}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 0.9, "green": 1.0, "blue": 0.9}, "type": "MIN"},
                    "maxpoint": {"color": {"red": 0.1, "green": 0.7, "blue": 0.2}, "type": "MAX"}
                }
            },
            "index": 0
        }
    })
    
    # 2. SL% (Column H / Index 7) - Red Scale
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 7, "endColumnIndex": 8}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 1.0, "green": 0.9, "blue": 0.9}, "type": "MIN"},
                    "maxpoint": {"color": {"red": 0.8, "green": 0.0, "blue": 0.0}, "type": "MAX"}
                }
            },
            "index": 0
        }
    })

    # 3. Win Rate (Column L / Index 11) - Red -> White -> Green
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 11, "endColumnIndex": 12}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 0.98, "green": 0.82, "blue": 0.82}, "type": "MIN"},
                    "midpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "PERCENTILE", "value": "50"},
                    "maxpoint": {"color": {"red": 0.85, "green": 0.94, "blue": 0.85}, "type": "MAX"}
                }
            },
            "index": 0
        }
    })

    # 4. Trades (Column M / Index 12) - White -> Blue
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 12, "endColumnIndex": 13}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "MIN"},
                    "maxpoint": {"color": {"red": 0.85, "green": 0.9, "blue": 1.0}, "type": "MAX"}
                }
            },
            "index": 0
        }
    })

    # 5. Gross PnL (Column N / Index 13) - Red -> White -> Green
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 13, "endColumnIndex": 14}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 0.98, "green": 0.82, "blue": 0.82}, "type": "MIN"},
                    "midpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "NUMBER", "value": "0"},
                    "maxpoint": {"color": {"red": 0.85, "green": 0.94, "blue": 0.85}, "type": "MAX"}
                }
            },
            "index": 0
        }
    })

    # 6. Net PnL (Column P / Index 15) - Red -> White -> Green
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 15, "endColumnIndex": 16}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 0.98, "green": 0.82, "blue": 0.82}, "type": "MIN"},
                    "midpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "NUMBER", "value": "0"},
                    "maxpoint": {"color": {"red": 0.85, "green": 0.94, "blue": 0.85}, "type": "MAX"}
                }
            },
            "index": 0
        }
    })
    
    # 7. Basic Text Styling for Headers (Extended to index 80)
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 80},
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                }
            },
            "fields": "userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment,backgroundColor)"
        }
    })

    # Send Batch
    ss.batch_update({"requests": requests})
    print("✅ Visuals Restored (Formatted & Colored).")

if __name__ == "__main__":
    restore_visuals()
