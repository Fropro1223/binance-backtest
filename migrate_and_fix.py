import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import re

def migrate_sheet():
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
        
        print(f"🛠️  Migrating '{ws.title}' to Merged Header Structure...")
        
        # 1. Get Current Headers (Row 1)
        headers = ws.row_values(1)
        
        # Check if already migrated?
        # If Row 2 contains "Trades" and "PnL" it might be done.
        row2 = ws.row_values(2)
        if "Trades" in row2 and "PnL" in row2:
            print("⚠️  Sheet seems already migrated (Row 2 has sub-headers). Skipping structure change.")
        else:
            print("   -> Inserting Row 2 for sub-headers...")
            ws.insert_row([], index=2)
            
            # Re-read headers (Row 1) - they stay at Row 1
            # We need to parse them and update Row 1 and Row 2
            
            # Map of Index -> (Label, Subheader)
            # Index is 1-based
            
            # Standard Cols
            # 1: Timestamp
            # 2: Strategy
            # 3: Win Rate
            # 4: Total Trades
            # 5: Total PnL
            
            # Update Standard Subheaders?
            # Actually, we leave standard cols as 1-row effectively (Vertical Merge later if needed)
            # But we need to clean Row 1 headers for Weekly items.
            
            merge_ranges = []
            
            # iterate from Col 6
            # We expect T and $ pairs.
            
            i = 5 # 0-based index for Col 6
            while i < len(headers):
                h = headers[i]
                col_idx = i + 1
                
                # Check format "dd.mm-dd.mm (T)" or "($)"
                # Regex or string check
                if "(T)" in h:
                    label = h.replace("(T)", "").replace("\n", "").strip()
                    ws.update_cell(1, col_idx, label)
                    ws.update_cell(2, col_idx, "Trades")
                elif "($)" in h:
                    label = h.replace("($)", "").replace("\n", "").strip()
                    ws.update_cell(1, col_idx, label)
                    ws.update_cell(2, col_idx, "PnL")
                    
                    # Check if previous column was the Pair?
                    # If so, current label should match previous label
                    prev_h = headers[i-1]
                    if label in prev_h:
                        # Add Merge Range (Row 1, Col i -> Col i+1)
                        # Previous col was i (index i+1-1 = i)
                        start_a1 = gspread.utils.rowcol_to_a1(1, col_idx-1)
                        end_a1 = gspread.utils.rowcol_to_a1(1, col_idx)
                        merge_ranges.append(f"{start_a1}:{end_a1}")
                        
                        # Clear specific cell in Row 1 Col 2 (Merged cell second part should be empty?)
                        ws.update_cell(1, col_idx, "")
                
                i += 1
                
            print(f"   -> Merging {len(merge_ranges)} header groups...")
            for rng in merge_ranges:
                try:
                    ws.merge_cells(rng)
                except Exception as e:
                    print(f"     Merge Error {rng}: {e}")
                    
        # 2. Convert Data to Numbers (Row 3 to End)
        print("🧹 Cleaning Data (Converting Strings to Numbers)...")
        all_values = ws.get_all_values()
        # all_values[0] is R1, all_values[1] is R2. Data starts R3 (Index 2).
        
        # We need to construct a batch update to replace all data with raw numbers
        # But gspread `update` requires values.
        # We can iterate and clean in python then upload.
        
        cleaned_data = []
        
        # Start from Row 3
        start_row_index = 2
        if len(all_values) > start_row_index:
            data_rows = all_values[start_row_index:]
            
            for row in data_rows:
                new_row = []
                for val in row:
                    # Clean String
                    val_str = str(val).strip()
                    
                    if not val_str:
                        new_row.append("")
                        continue
                        
                    # Remove currency symbols and commas
                    clean = val_str.replace("$", "").replace(",", "").replace(" TL", "")
                    
                    # Try Convert to Int or Float
                    if clean.replace(".", "", 1).isdigit() or (clean.startswith("-") and clean[1:].replace(".", "", 1).isdigit()):
                        try:
                            if "." in clean:
                                new_row.append(float(clean))
                            else:
                                new_row.append(int(clean))
                        except:
                            new_row.append(val) # Fallback
                    elif "%" in val_str:
                        # Percentage parsing
                         try:
                             pct = float(val_str.replace("%", "")) / 100.0
                             new_row.append(pct)
                         except:
                             new_row.append(val)
                    else:
                        new_row.append(val)
                cleaned_data.append(new_row)
            
            # Upload Cleaned Data in Batch
            if cleaned_data:
                # Range A3:End
                end_row = start_row_index + len(cleaned_data)
                range_name = f"A3:ZZ{end_row}" 
                # Careful with ZZ, calculate max col
                # Or just A3 start
                print(f"   -> Uploading {len(cleaned_data)} cleaned rows...")
                ws.update(range_name=f"A3", values=cleaned_data, value_input_option='USER_ENTERED')
        
        # 3. Final Formatting & CF
        print("🎨 Applying Formats & CF...")
        
        # Freeze 2
        ws.freeze(rows=2)
        
        # Center Headers
        ws.format("A1:Z2", { # Approx range
            "textFormat": {"bold": True},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "wrapStrategy": "WRAP"
        })
        
        # Identify Columns for formatting
        row2 = ws.row_values(2)
        trade_cols = []
        pnl_cols = []
        
        for i, val in enumerate(row2):
            c = i + 1
            if val == "Trades": trade_cols.append(c)
            elif val == "PnL": pnl_cols.append(c)
        
        # Add Standard cols
        # WinRate (3), Trades(4), PnL(5)
        # Check standard headers too or just assume index
        trade_cols.append(4)
        pnl_cols.append(5)
        
        # Apply Formats
        for c in trade_cols:
            rng = f"{gspread.utils.rowcol_to_a1(3, c)}:{gspread.utils.rowcol_to_a1(1000, c)}"
            ws.format(rng, {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}, "horizontalAlignment": "RIGHT"})
            
        cf_requests = []
        for c in pnl_cols:
            rng = f"{gspread.utils.rowcol_to_a1(3, c)}:{gspread.utils.rowcol_to_a1(1000, c)}"
            ws.format(rng, {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0"}, "horizontalAlignment": "RIGHT"})
            
            # CF Rule
            cf_requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "startColumnIndex": c-1, "endColumnIndex": c}],
                        "booleanRule": {
                            "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                            "format": {"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}, "bold": True}}
                        }
                    },
                    "index": 0
                }
            })
            cf_requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": ws.id, "startRowIndex": 2, "startColumnIndex": c-1, "endColumnIndex": c}],
                        "booleanRule": {
                            "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                            "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}}
                        }
                    },
                    "index": 1
                }
            })
            
        if cf_requests:
            try:
                sheet.batch_update({"requests": cf_requests})
            except Exception as e:
                print(f"CF Error: {e}")
                
        # Percent Format for Col 3
        ws.format("C3:C1000", {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}, "horizontalAlignment": "CENTER"})

        # Hide A
        try:
            sheet.batch_update({
            "requests": [{"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser"
            }}]
            })
        except: pass
        
        print("✅ Migration Complete!")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    migrate_sheet()
