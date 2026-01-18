import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def apply_cf():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    if not os.path.exists(creds_path):
        print(f"❌ Credentials not found at {creds_path}")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID')
    if not manual_sheet_id:
        manual_sheet_id = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"

    try:
        sheet = client.open_by_key(manual_sheet_id)
        ws = sheet.worksheet("Analysis")
        
        print(f"🎨 Applying Conditional Formatting to '{ws.title}'...")
        
        # CLEAR EXISTING RULES FIRST
        # This is critical to remove old zebra striping or conflicting rules
        print("🧹 Clearing all existing CF rules...")
        try:
            # Try to delete top 50 rules (should cover everything)
            delete_reqs = [{"deleteConditionalFormatRule": {"sheetId": ws.id, "index": 0}} for _ in range(50)]
            # We send them in batches or all at once. If there are fewer rules, it might error on index out of bounds?
            # Actually, delete at index 0 repeated N times works if there are N rules.
            # But if we try to delete 50 and there are only 5, it will fail.
            # Safer way: just try one by one in a loop or verify count?
            # Google Sheets API: if index is out of bounds, it creates error.
            # Best approach: Just try to delete index 0 repeatedly, ignoring errors? No, batch will fail entirely.
            
            # Alternative: Assume max 50, and just send one "delete all" if we could? No "delete all" command.
            
            # Use a loop to delete index 0 until it fails? Slow but safe.
            # Or just send a few batch requests of size 1.
            
            # Let's try deleting index 0 up to 50 times individually for safety in this script
            for _ in range(50):
                try:
                    sheet.batch_update({"requests": [{"deleteConditionalFormatRule": {"sheetId": ws.id, "index": 0}}]})
                except Exception:
                    # Likely no more rules to delete
                    break
            print("   Existing rules cleared.")
        except Exception as e:
            print(f"   Warning during clearing: {e}")
        
        # Get headers to find PnL columns
        headers_row2 = ws.row_values(2)
        
        cf_requests = []
        rule_index = 0
        
        # CF for Column E (Total PnL) - 0-based index 4
        cf_requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": 4, "endColumnIndex": 5}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                        "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}
                    }
                },
                "index": rule_index
            }
        })
        rule_index += 1
        
        cf_requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": 4, "endColumnIndex": 5}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                        "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}
                    }
                },
                "index": rule_index
            }
        })
        rule_index += 1
        
        print(f"   Adding rules for Total PnL (Col E)...")
        
        # Find PnL columns (both 'PnL' and 'PnL%')
        pnl_count = 0
        for i, header in enumerate(headers_row2):
            if i < 5:  # Skip A-E
                continue
            
            col_idx = i  # 0-based
            
            if header in ["PnL", "PnL%"]:
                col_letter = chr(64 + col_idx + 1) if col_idx < 26 else f'{chr(64 + (col_idx)//26)}{chr(65 + (col_idx)%26)}'
                print(f"   -> Found PnL at Column {col_letter} (Index {col_idx})")
                
                # Green for positive
                cf_requests.append({
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1}],
                            "booleanRule": {
                                "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                                "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}
                            }
                        },
                        "index": rule_index
                    }
                })
                rule_index += 1
                
                # Red for negative
                cf_requests.append({
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1}],
                            "booleanRule": {
                                "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                                "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}
                            }
                        },
                        "index": rule_index
                    }
                })
                rule_index += 1
                pnl_count += 1
        
        print(f"   Adding rules for {pnl_count} PnL columns (plus Total PnL)...")
        print(f"   Total CF rules generated so far: {len(cf_requests)}")

        # Apply all rules
        sheet.batch_update({"requests": cf_requests})
        
        print("✅ Conditional Formatting complete!")
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    apply_cf()
