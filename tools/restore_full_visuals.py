import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

def restore_full_visuals():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    ss = client.open_by_key(MASTER_SHEET_ID)
    ws = ss.worksheet("backtest1")
    
    print("🎨 Restoring Full Visuals for L, M, N, P (Gradients + Text Colors)...")

    requests = []
    
    # 1. Clear existing CF rules for columns L, M, N, O, P (Columns 12 to 16 | Indices 11 to 15)
    # Since we can't easily filter by range in delete requests without fetching, 
    # and user asked to "reload", ideally we clear all and reload all to be safe.
    # But let's just fetch them to see if we can do a targeted clean.
    
    metadata = ss.fetch_sheet_metadata()
    sheet_data = next(s for s in metadata['sheets'] if s['properties']['title'] == "backtest1")
    existing_rules = sheet_data.get('conditionalFormats', [])
    
    # We will delete rules that overlap with L:P
    indices_to_delete = []
    for i, rule in enumerate(existing_rules):
        for r_range in rule.get('ranges', []):
            start_col = r_range.get('startColumnIndex', 0)
            end_col = r_range.get('endColumnIndex', 0)
            # If rule overlaps with L(11) to P+1(16)
            if (start_col >= 11 and start_col < 16) or (end_col > 11 and end_col <= 16):
                indices_to_delete.append(i)
                break
    
    # Delete from highest index to lowest to avoid shifting
    for idx in sorted(indices_to_delete, reverse=True):
        requests.append({"deleteConditionalFormatRule": {"sheetId": ws.id, "index": idx}})

    # --- NEW RULES ---
    
    # Win Rate (L / 11) - Gradient Red-White-Green
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 5000, "startColumnIndex": 11, "endColumnIndex": 12}],
                "gradientRule": {
                    "minpoint": {"color": {"red": 0.98, "green": 0.82, "blue": 0.82}, "type": "NUMBER", "value": "0"},
                    "midpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "NUMBER", "value": "0.5"},
                    "maxpoint": {"color": {"red": 0.85, "green": 0.94, "blue": 0.85}, "type": "NUMBER", "value": "1"}
                }
            },
            "index": 0
        }
    })

    # Trades (M / 12) - Gradient White-Blue
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

    # PnL Columns (N / 13 and P / 15)
    for col_idx in [13, 15]:
        # Gradient Background
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 5000, "startColumnIndex": col_idx, "endColumnIndex": col_idx+1}],
                    "gradientRule": {
                        "minpoint": {"color": {"red": 0.98, "green": 0.82, "blue": 0.82}, "type": "MIN"},
                        "midpoint": {"color": {"red": 1.0, "green": 1.0, "blue": 1.0}, "type": "NUMBER", "value": "0"},
                        "maxpoint": {"color": {"red": 0.85, "green": 0.94, "blue": 0.85}, "type": "MAX"}
                    }
                },
                "index": 0
            }
        })
        
        # Text Color: Green (>0)
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 5000, "startColumnIndex": col_idx, "endColumnIndex": col_idx+1}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                        "format": {"textFormat": {"foregroundColor": {"red": 0.0, "green": 0.6, "blue": 0.0}, "bold": True}}
                    }
                },
                "index": 0 # Add at top so it takes priority over background text color if any
            }
        })
        
        # Text Color: Red (<0)
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 5000, "startColumnIndex": col_idx, "endColumnIndex": col_idx+1}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                        "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0.0, "blue": 0.0}, "bold": True}}
                    }
                },
                "index": 0
            }
        })

    # 2. Reset base cell format (remove explicit black text color if set)
    # so conditional formatting can work properly.
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 5000, "startColumnIndex": 11, "endColumnIndex": 16},
            "cell": {
                "userEnteredFormat": {
                    # Removing foreground color allows CF to control it
                    # We can't "null" a field easily in repeatCell without clearing, 
                    # but we can set it to a default.
                    # Or just rely on CF priority if we set index 0.
                }
            },
            "fields": "userEnteredFormat.textFormat.foregroundColor" # This effectively resets it to default if not specified? 
            # Actually, to clear it we might need a different approach.
            # Let's just set them to black as base, and CF will override.
        }
    })

    if requests:
        ss.batch_update({"requests": requests})
        print(f"✅ Successfully restored {len(requests)} rules/formats.")

if __name__ == "__main__":
    restore_full_visuals()
