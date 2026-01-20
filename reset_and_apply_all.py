import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import gspread.utils

def reset_and_apply_all():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID', '1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM')
    sheet = client.open_by_key(manual_sheet_id)
    ws = sheet.worksheet("Analysis")
    
    print(f"🔄 FULL RESET AND RE-APPLY for '{ws.title}'...")
    
    # ---------------------------------------------------------
    # 1. CLEAR EVERYTHING (Formats + CF Rules)
    # ---------------------------------------------------------
    print("   1. Clearing ALL existing formats and rules...")
    
    # Clear formats (backgrounds, fonts, etc)
    # We use batch_update with repeatCell clearing formatting
    requests = []
    
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 2000, "startColumnIndex": 0, "endColumnIndex": 60},
            "cell": {"userEnteredFormat": {}}, # Empty format resets to default
            "fields": "userEnteredFormat"
        }
    })
    
    # Delete all Conditional Formatting rules (by deleting index 0 repeatedly)
    for _ in range(50):
        requests.append({"deleteConditionalFormatRule": {"sheetId": ws.id, "index": 0}})
    
    # Send clear requests first
    try:
        sheet.batch_update({"requests": requests})
    except Exception as e:
        print(f"   (Clear warning: {e})")
        
    print("   ✅ Formats cleared.")
    
    # ---------------------------------------------------------
    # 2. APPLY COLUMN STRIPING (HEADERS ONLY vs FULL COLUMN?)
    # ---------------------------------------------------------
    # "Sütun atlamayı yükle" -> User implies distinction between weeks.
    # Previous confusion was about "Rows jumping".
    # User likely wants full vertical columns colored freely (Vertical Stripes), BUT NOT Horizontal Stripes.
    # Let's apply Vertical Stripes (White/Gray) to the FULL column (Headers + Data).
    # Why? Because user said "satırları aç browseri bak satırlar atlamasın istiyorum".
    # This implies they dislike horizontal stripes but LIKE vertical distinction.
    
    print("   2. Applying Column Striping (Vertical)...")
    
    color_white = {"red": 1.0, "green": 1.0, "blue": 1.0}
    color_gray = {"red": 0.96, "green": 0.96, "blue": 0.96} # Light gray (Weeks)
    color_black = {"red": 0.0, "green": 0.0, "blue": 0.0} # Black (Metrics)
    color_light_blue = {"red": 0.95, "green": 0.97, "blue": 1.0} # Alice Blueish
    
    headers_row2 = ws.row_values(2)
    col_reqs = []
    
    # Trackers for alternating colors
    tf_pair_index = 0
    week_pair_index = 0
    
    # Columns F-Q are Timeframe Breakdown (Indices 5-16)
    # Columns R onwards are Weekly Breakdown (Indices 17+)
    
    for i, val in enumerate(headers_row2):
        col_idx = i # 0-based
        
        # A-B: Basic Info (Keep White) (Indices 0, 1)
        if col_idx < 2:
             current_color = color_white
        
        # C-D-E: Metrics (Indices 2, 3, 4) -> Solid Black
        elif 2 <= col_idx < 5:
            current_color = color_black
            # We also need white text for these, but this loop only does background.
            # We'll add textFormat in the next section where alignment is handled.
             
        # F-Q: Timeframe Breakdown (Indices 5-16)
        elif 5 <= col_idx < 17:
            # Pair logic: Trades% starts pair, PnL ends it.
            # Headers: '5s Trades%', 'PnL', '10s Trades%', 'PnL'...
            # We want '5s' pair (Trades+PnL) to be one color, '10s' pair next color?
            # Or just alternate every TF?
            # Yes, alternate every TF pair.
            
            # Identify if this is start of a pair or end
            # We can use index. (col_idx - 5) // 2 gives us the pair index (0, 1, 2...)
            pair_idx = (col_idx - 5) // 2
            
            # Alternate White / Light Blue
            current_color = color_white if pair_idx % 2 == 0 else color_light_blue
            
        # R onwards: Weekly Breakdown (Indices 17+)
        else:
            # Weekly logic (Trades | PnL)
            # Use separate counter or calculate from index
            # Index R=17.
            pair_idx = (col_idx - 17) // 2
            
            # Alternate White / Gray
            current_color = color_white if pair_idx % 2 == 0 else color_gray

        
        col_reqs.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 0, "endRowIndex": 2000, # Apply to full column
                    "startColumnIndex": col_idx, "endColumnIndex": col_idx+1
                },
                "cell": {"userEnteredFormat": {"backgroundColor": current_color}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })
        
    sheet.batch_update({"requests": col_reqs})
    print("   ✅ Column colors applied (Dual Zone).")

    # ---------------------------------------------------------
    # 3. APPLY NUMBER FORMATS & ALIGNMENT
    # ---------------------------------------------------------
    print("   3. Applying Number Formats & Alignment...")
    fmt_reqs = []
    
    # A: Timestamp (Font Size 7)
    fmt_reqs.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {"textFormat": {"fontSize": 7}}},
            "fields": "userEnteredFormat.textFormat.fontSize"
        }
    })
    
    # B: Strategy (Font Size 8)
    fmt_reqs.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 1, "endColumnIndex": 2},
            "cell": {"userEnteredFormat": {"textFormat": {"fontSize": 8}}},
            "fields": "userEnteredFormat.textFormat.fontSize"
        }
    })
    
    # C: Win Rate % (Size 9)
    fmt_reqs.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 2, "endColumnIndex": 3},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}, "horizontalAlignment": "CENTER", "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9}}},
            "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.textFormat"
        }
    })
    # D: Trades (Number) - Font Size 9
    fmt_reqs.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 3, "endColumnIndex": 4},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}, "horizontalAlignment": "RIGHT", "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9}}},
            "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.textFormat"
        }
    })
    # E: PnL (Currency) - Font Size 9
    fmt_reqs.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": 4, "endColumnIndex": 5},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0"}, "horizontalAlignment": "RIGHT", "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 9}}},
            "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.textFormat"
        }
    })
    
    # F-Q and R-End: Trades% / PnL
    
    for i, val in enumerate(headers_row2):
        if i < 5: continue
        col_idx = i
        
        # Determine format based on header name (Trades%, PnL)
        is_trade = "Trades" in val
        number_format = {"type": "NUMBER", "pattern": "#,##0"} if is_trade else {"type": "CURRENCY", "pattern": "$#,##0"}
        
        fmt_reqs.append({
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 2, "endRowIndex": 2000, "startColumnIndex": col_idx, "endColumnIndex": col_idx+1},
                "cell": {"userEnteredFormat": {"numberFormat": number_format, "horizontalAlignment": "RIGHT", "textFormat": {"fontSize": 9}}},
                "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment,userEnteredFormat.textFormat"
            }
        })

    sheet.batch_update({"requests": fmt_reqs})
    print("   ✅ Number formats applied.")

    # ---------------------------------------------------------
    # 4. APPLY CONDITIONAL FORMATTING (PnL only)
    # ---------------------------------------------------------
    print("   4. Applying Conditional Formatting (Green/Red)...")
    cf_requests = []
    rule_index = 0
    
    # Total PnL (E)
    cols_to_process = [4] # Start with E
    
    # Find all other PnL columns
    for i, val in enumerate(headers_row2):
        if val in ["PnL", "PnL%", "Total PnL ($)"]: # Strict check
            if i != 4: cols_to_process.append(i)
            
    for col_idx in cols_to_process:
        # Green
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
        # Red
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
        
    if cf_requests:
        sheet.batch_update({"requests": cf_requests})
        print(f"   ✅ {len(cf_requests)} CF rules applied.")
    
    # 5. Bold Headers and specific labels
    print("   5. Bolding Headers and naming Strategy...")
    # Explicitly set B1 to "Strategy"
    ws.update_cell(1, 2, "Strategy")
    
    sheet.batch_update({"requests": [{
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 5},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold"
        }
    }]})

    # TF Headers (F-Q) Row 1
    sheet.batch_update({"requests": [{
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 5, "endColumnIndex": 17},
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "bold": True, 
                        "foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.8}
                    }, 
                    "horizontalAlignment": "CENTER"
                }
            },
            "fields": "userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment"
        }
    }]})
    
    # Weekly & Generic Headers bolding (Remaining Row 2)
    sheet.batch_update({"requests": [{
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 5, "endColumnIndex": 100},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": 8}, "horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment"
        }
    }]})
    
    # Weekly Row 1 bolding and font size 8
    sheet.batch_update({"requests": [{
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 17, "endColumnIndex": 100},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": 8}, "horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat.textFormat.bold,userEnteredFormat.textFormat.fontSize,userEnteredFormat.horizontalAlignment"
        }
    }]})
    
    print("✅ FULL RESET COMPLETE!")

if __name__ == "__main__":
    reset_and_apply_all()
