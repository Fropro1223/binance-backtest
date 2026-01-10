import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from pathlib import Path

def format_weekly():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    if not os.path.exists(creds_path):
        print(f"❌ Credentials not found at {creds_path}")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID')
    if not manual_sheet_id:
        print("❌ MANUAL_SHEET_ID not set.")
        return

    try:
        sheet = client.open_by_key(manual_sheet_id)
        ws = sheet.worksheet("Analysis")
        
        print(f"🎨 Formatting Weekly Columns in '{ws.title}'...")
        
        current_headers = ws.row_values(1)
        print(f"   Found {len(current_headers)} columns.")
        
        # Iterate from Index 5 (Col F) to end
        for i in range(5, len(current_headers)):
            header = current_headers[i]
            col_idx = i + 1
            cell_start = gspread.utils.rowcol_to_a1(2, col_idx)
            cell_end = gspread.utils.rowcol_to_a1(1000, col_idx)
            fmt_range = f"{cell_start}:{cell_end}"
            
            if "(T)" in header:
                print(f"   Col {col_idx} ({header.splitlines()[0]}) -> Number (Trade Count)")
                ws.format(fmt_range, {
                    "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                    "horizontalAlignment": "RIGHT"
                })
            elif "($)" in header:
                print(f"   Col {col_idx} ({header.splitlines()[0]}) -> Currency (PnL)")
                ws.format(fmt_range, {
                    "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0"},
                    "horizontalAlignment": "RIGHT"
                })
                
                # Apply Conditional Formatting to this PnL column
                rules = [
                    {
                        "addConditionalFormatRule": {
                            "rule": {
                                "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "startColumnIndex": col_idx-1, "endColumnIndex": col_idx}],
                                "booleanRule": {
                                    "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                                    "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}
                                }
                            },
                            "index": 0
                        }
                    },
                    {
                        "addConditionalFormatRule": {
                            "rule": {
                                "ranges": [{"sheetId": ws.id, "startRowIndex": 1, "startColumnIndex": col_idx-1, "endColumnIndex": col_idx}],
                                "booleanRule": {
                                    "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                                    "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}
                                }
                            },
                            "index": 1
                        }
                    }
                ]
                try:
                    sheet.batch_update({"requests": rules})
                except Exception as e:
                    print(f"     ⚠️ CF Error: {e}")

        # Auto Resize
        print("   Auto-resizing all columns...")
        ws.columns_auto_resize(0, len(current_headers))

        print("✅ Formatting complete!")
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    format_weekly()
