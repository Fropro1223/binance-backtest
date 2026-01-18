import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import gspread.utils

def restore_column_colors():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID', '1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM')
    sheet = client.open_by_key(manual_sheet_id)
    ws = sheet.worksheet("Analysis")
    
    print(f"🎨 Restoring COLUMN Striping (Vertical) for '{ws.title}'...")
    
    # Define colors
    color_white = {"red": 1.0, "green": 1.0, "blue": 1.0}
    color_gray = {"red": 0.93, "green": 0.93, "blue": 0.93} # Light gray
    
    # Get headers to identify columns
    headers_row2 = ws.row_values(2)
    
    # We want to apply this ONLY to Weekly columns usually, or all columns?
    # User said "Sütun renk atlamasını iptal ettin" -> implying they want it back.
    # Usually this applies to Weekly breakdown columns (R onwards).
    # Or maybe Timeframe breakdown columns too?
    # Let's apply standard logic:
    # A-E: White
    # F-Q: Timeframe Breakdown (Pairs) -> Let's keep these white or alternate? 
    # Usually we alternate weeks.
    
    # Let's look at headers starting from R (18).
    # Weekly headers are in Row 1 (R1, T1, etc.)
    # In Row 2, we have "Trades", "PnL", "Trades", "PnL"...
    
    # Create API requests
    requests = []
    
    # 1. Reset everything to White (Optional but safer)
    # requests.append({
    #     "repeatCell": {
    #         "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": 2000, "startColumnIndex": 0, "endColumnIndex": 50},
    #         "cell": {"userEnteredFormat": {"backgroundColor": color_white}},
    #         "fields": "userEnteredFormat.backgroundColor"
    #     }
    # })
    
    week_pair_index = 0
    
    for i, val in enumerate(headers_row2):
        col_idx = i + 1 # 1-based
        if col_idx < 18: # Skip A-Q
             # Force white for first columns
            current_color = color_white
        else:
            # Weekly columns R onwards
            if val == "Trades":
                current_color = color_white if week_pair_index % 2 == 0 else color_gray
                week_pair_index += 1
            elif val == "PnL":
                current_color = color_white if (week_pair_index - 1) % 2 == 0 else color_gray
            else:
                current_color = color_white
        
        # Add request for this column
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1, 
                    "endRowIndex": 2000,
                    "startColumnIndex": i, 
                    "endColumnIndex": i+1
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": current_color
                    }
                },
                "fields": "userEnteredFormat.backgroundColor"
            }
        })
        
        color_name = "White" if current_color == color_white else "Gray"
        # print(f"Col {col_idx}: {color_name}")

    if requests:
        print(f"Sending {len(requests)} formatting requests...")
        sheet.batch_update({"requests": requests})
        print("✅ Column Colors Restored!")

if __name__ == "__main__":
    restore_column_colors()
