import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import time

def migrate_sheet_batch():
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
        
        print(f"🛠️  Migrating '{ws.title}' (Batch Mode)...")
        
        # 1. Inspect Current State
        # Fetch top 5 rows to be sure
        top_data = ws.get("A1:Z5")
        if not top_data:
            print("Sheet empty?")
            return
            
        row1 = top_data[0]
        row2 = top_data[1] if len(top_data) > 1 else []
        
        # Detect if we need to insert Row 2
        # If Row 2 has "Trades" or "PnL", we are migrated.
        if "Trades" in row2 and "PnL" in row2:
            print("✅ Row 2 appears to have sub-headers. Skipping Row Insertion.")
            # But we might need to fix format/merges? Let's proceed to header update logic anyway.
        else:
            # Check if Row 2 is empty (failed migration state)
            # If Row 2 is empty and Row 1 has "(T)", we can use it.
            is_row2_empty = not any(row2)
            
            if is_row2_empty and any("(T)" in c for c in row1):
                 print("⚠️ Row 2 is empty but Row 1 has tags. Assuming previous run inserted row. Using existing Row 2.")
            elif any("(T)" in c for c in row1):
                 # Row 1 has tags, Row 2 is NOT empty (likely data). Must Insert.
                 print("   -> Inserting Row 2 for sub-headers...")
                 ws.insert_row([], index=2)
                 row2 = [""] * len(row1) # Update local var
            else:
                 pass # Already clean?
        
        # Re-fetch Headers after potential insert
        headers = ws.row_values(1)
        new_row1 = list(headers)
        new_row2 = [""] * len(headers)
        
        # Populate Sub-headers logic
        if len(new_row2) < len(new_row1):
            new_row2.extend([""] * (len(new_row1) - len(new_row2)))
            
        merge_reqs = []
        
        i = 0
        while i < len(headers):
            h = headers[i]
            col_idx = i + 1 # 1-based
            
            if "(T)" in h:
                label = h.replace("(T)", "").replace("\n", "").strip()
                new_row1[i] = label
                new_row2[i] = "Trades"
            elif "($)" in h:
                label = h.replace("($)", "").replace("\n", "").strip()
                new_row1[i] = label
                new_row2[i] = "PnL"
                
                # Check Merge
                if i > 0 and label in new_row1[i-1]:
                    new_row1[i] = "" # Clear duplicate for cleanliness in merged cell
                    
                    # Merge Request
                    merge_reqs.append({
                        "mergeCells": {
                            "range": {
                                "sheetId": ws.id,
                                "startRowIndex": 0, "endRowIndex": 1,
                                "startColumnIndex": i-1, "endColumnIndex": i+1
                            },
                            "mergeType": "MERGE_ALL"
                        }
                    })
            i += 1
            
        # BATCH UPDATE HEADERS
        print("🚀 Updating Headers (A1:Z2)...")
        # Need to determine end column letter
        max_col = len(headers)
        end_char = gspread.utils.rowcol_to_a1(2, max_col)
        range_name = f"A1:{end_char}"
        
        ws.update(range_name=range_name, values=[new_row1, new_row2])
        
        # BATCH MERGE
        if merge_reqs:
             print(f"🔗 Sending {len(merge_reqs)} merge requests...")
             sheet.batch_update({"requests": merge_reqs})
        
        time.sleep(2) # Cooldown
        
        # 2. Convert Data to Numbers (Row 3 to End)
        print("🧹 Cleaning Data...")
        all_values = ws.get_all_values()
        
        cleaned_data = []
        start_row_index = 2 # Row 3 (Index 2)
        
        if len(all_values) > start_row_index:
            data_rows = all_values[start_row_index:]
            for row in data_rows:
                new_row = []
                for val in row:
                    val_str = str(val).strip()
                    if not val_str:
                        new_row.append("")
                        continue
                        
                    clean = val_str.replace("$", "").replace(",", "").replace(" TL", "")
                    
                    if clean.replace(".", "", 1).isdigit() or (clean.startswith("-") and clean[1:].replace(".", "", 1).isdigit()):
                        try:
                            if "." in clean:
                                new_row.append(float(clean))
                            else:
                                new_row.append(int(clean))
                        except:
                            new_row.append(val)
                    elif "%" in val_str:
                         try:
                             pct = float(val_str.replace("%", "")) / 100.0
                             new_row.append(pct)
                         except:
                             new_row.append(val)
                    else:
                        new_row.append(val)
                cleaned_data.append(new_row)
            
            if cleaned_data:
                print(f"   -> Uploading {len(cleaned_data)} cleaned rows...")
                ws.update(range_name=f"A3", values=cleaned_data, value_input_option='USER_ENTERED')
        
        # 3. Formatting
        print("🎨 Applying Final Formats...")
        ws.freeze(rows=2)
        
        # Standard Cols
        ws.format("C3:C1000", {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}, "horizontalAlignment": "CENTER"})
        
        # Identify Trade/PnL cols
        trade_cols = []
        pnl_cols = []
        
        for i, val in enumerate(new_row2):
            if val == "Trades": trade_cols.append(i+1)
            elif val == "PnL": pnl_cols.append(i+1)
        
        # Also Col 4, 5
        trade_cols.append(4)
        pnl_cols.append(5)
        
        # Batch Formats?
        # Using loop roughly OK for formats? We should group requests manually to avoid 429
        # But 'ws.format' is individual.
        # Let's create a big batch_update for CellFormat.
        
        format_reqs = []
        
        # Trades: Number, Right
        for c in trade_cols:
             format_reqs.append({
                 "repeatCell": {
                     "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": c-1, "endColumnIndex": c},
                     "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}, "horizontalAlignment": "RIGHT"}},
                     "fields": "userEnteredFormat(numberFormat,horizontalAlignment)"
                 }
             })
             
        # PnL: Currency, Right, CF
        cf_reqs = []
        for c in pnl_cols:
             format_reqs.append({
                 "repeatCell": {
                     "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 1000, "startColumnIndex": c-1, "endColumnIndex": c},
                     "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0"}, "horizontalAlignment": "RIGHT"}},
                     "fields": "userEnteredFormat(numberFormat,horizontalAlignment)"
                 }
             })
             # CF
             cf_reqs.append({
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
             cf_reqs.append({
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
             
        # Send Format Batch
        if format_reqs:
             print(f"🎨 Sending {len(format_reqs)} format requests...")
             sheet.batch_update({"requests": format_reqs})
             
        # Send CF Batch
        if cf_reqs:
             print(f"🚦 Sending {len(cf_reqs)} CF requests...")
             sheet.batch_update({"requests": cf_reqs})

        print("✅ Migration Complete!")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    migrate_sheet_batch()
