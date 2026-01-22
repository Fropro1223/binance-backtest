import sys
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sheets

def migrate_layout():
    """
    Migrates the Google Sheet from separate Pump/Dump columns to a single Threshold% column.
    
    OLD Layout (Row 3+):
    Col 1 (A): Timestamp
    Col 2 (B): Strategy
    Col 3 (C): Side
    Col 4 (D): Cond
    Col 5 (E): EMA
    Col 6 (F): Pump
    Col 7 (G): Dump
    Col 8 (H): TP
    Col 9 (I): SL
    Col 10 (J): TSL
    Col 11 (K): Maru
    Col 12 (L): Days
    Col 13 (M): WinRate
    ...

    NEW Layout:
    Col 5 (E): Threshold% (Positive for Pump, Negative for Dump)
    Col 6 (F): EMA
    Col 7 (G): TP
    ...
    """
    print("🚀 Connecting to Google Sheets for MIGRATION...")
    
    try:
        creds_path = sheets.get_credentials_path()
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
        client = gspread.authorize(creds)
        
        # Use MANUAL_SHEET_ID env var if set, otherwise fallback to MASTER_SHEET_ID
        manual_sheet_id = os.environ.get('MANUAL_SHEET_ID')
        MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
        sheet_id = manual_sheet_id if manual_sheet_id else MASTER_SHEET_ID
        
        sheet = client.open_by_key(sheet_id)
        ws = sheet.worksheet("Analysis")
        print(f"✅ Opened 'Analysis' tab")
        
        # 1. Read All Current Data (Row 3 onwards)
        print("📥 Reading existing data...")
        all_values = ws.get_all_values()
        if len(all_values) < 3:
            print("⚠️ No data to migrate.")
            return

        headers_r1 = all_values[0]
        headers_r2 = all_values[1]
        data_rows = all_values[2:]
        
        print(f"📊 Found {len(data_rows)} rows to process.")
        
        new_data_rows = []
        
        for row in data_rows:
            # Pad row if incomplete
            while len(row) < 20: 
                row.append("")
                
            # Parse Existing Columns (0-indexed)
            # A=0, B=1, Side=2, Cond=3, EMA=4, Pump=5, Dump=6, TP=7, SL=8, TSL=9, Maru=10, Days=11, Metrics...
            
            # NOTE: Before THIS migration, EMA was Col 5 (Index 4), Pump was 6 (5), Dump 7 (6)...
            # Logic from OLD sheets.py:
            # Side=3(idx2), Cond=4(idx3), EMA=5(idx4), Pump=6(idx5), Dump=7(idx6)
            
            col_ts = row[0]
            col_strat = row[1]
            col_side = row[2]
            col_cond = row[3]
            col_ema = row[4]
            col_pump = row[5]
            col_dump = row[6]
            
            # Calculate Threshold
            threshold_val = ""
            if col_pump and col_pump.replace('.','',1).isdigit():
                try:
                    val = float(col_pump)
                    if val != 0:
                        threshold_val = f"{val:.1f}"
                except: pass
            
            if not threshold_val and col_dump and col_dump.replace('.','',1).isdigit():
                 try:
                    val = float(col_dump)
                    if val != 0:
                        threshold_val = f"{-val:.1f}"
                 except: pass

            # Reconstruct Row
            # New Structure: Side(2), Cond(3), Threshold(4), EMA(5), TP(6), SL(7)...
            new_row = [
                col_ts,       # A
                col_strat,    # B
                col_side,     # C
                col_cond,     # D
                threshold_val,# E (New Threshold)
                col_ema,      # F (Shifted EMA)
            ]
            
            # Append Remaining Columns starting from TP (Old Index 7)
            # Old: TP=7, SL=8, TSL=9, Maru=10, Days=11
            # New: TP=6, SL=7, TSL=8, Maru=9, Days=10
            new_row.extend(row[7:]) 
            
            new_data_rows.append(new_row)
            
        # 2. Clear Existing Data
        print("🧹 Clearing old data...")
        # Clear everything from Row 3 down
        ws.batch_clear(["A3:ZZ10000"])
        
        # 3. Write New Data
        print(f"📤 Writing {len(new_data_rows)} transformed rows...")
        if new_data_rows:
            # Use USER_ENTERED to parse numbers/formulas
            ws.update(range_name=f"A3:ZZ{2+len(new_data_rows)}", values=new_data_rows, value_input_option='USER_ENTERED')
            
        print("✅ Data Migration Complete.")
        
        # 4. Apply New Layout & Headers
        print("🎨 Applying updated headers and formatting...")
        sheets.apply_data_validation(ws)
        
        # We need headers for formatting function, but we just wrote data. 
        # Headers R2 are unchanged in content (Trades, PnL etc) but their *positions* relative to columns might shift?
        # Actually Metrics cols (WinRate etc) also shift left by 1 because we merged 2 cols (Pump, Dump) into 1 (Threshold).
        # WAIT! Pump(1) + Dump(1) -> Threshold(1). Net change = -1 column count. 
        # So yes, Metrics shift LEFT.
        
        # My row reconstruction `new_row.extend(row[7:])` handles the data shift.
        # But `sheets.py` formatting logic expects specific Metric columns relative to generic parameters.
        # Check `sheets.py`: `METRICS_START_COL = 12` (Col 13/M). 
        # Old: Pump(6), Dump(7)... Maru(11), Days(12), WinRate(13/M).
        # New: Threshold(5)... Maru(10), Days(11), WinRate(12/L).
        
        # Updated `sheets.py` defines `METRICS_START_COL = 12`.
        # So WinRate is now at Index 12 (Column 13/M)?
        # Let's count keys in PARAM_COLS:
        # 3(Side) 4(Cond) 5(Thresh) 6(EMA) 7(TP) 8(SL) 9(TSL) 10(Maru) 11(Days)
        # Last Param is 11. Next is 12. Correct.
        # Wait, usually 1-indexed for column names... 
        # A=1, B=2, C=3... L=12.
        # So WinRate should be at Col 13 (M) if it starts at 12? No.
        # `row_data[METRICS_START_COL]` -> row_data[12].
        # Since `upload_to_sheets` writes `list(row_data.values())`, and dict keys are sorted...
        # 1,2,3...11,12...
        # 12 is the 12th item (if 1-based keys match count).
        # A(1)..L(12). So it writes to Col L?
        # Let's check `sheets.py` `METRICS_START_COL`.
        # Old: `METRICS_START_COL = 13` (N? No idx 13 is N).
        # Param 3..12 (10 params). 
        # If I merge, I have 1 less param column.
        # So Metrics shift left by 1.
        
        # The script needs to handle the Headers too!
        # Row 1 and Row 2 headers need to be rewritten to match new alignment.
        # The `apply_sheet_formatting` *uses* headers to detect trades/pnl but doesn't *write* them except for Timeframe updates.
        # I need to explicitly REWRITE the main headers in Row 1.
        
        new_headers = [
            "Timestamp", "Strategy", "Side", "Cond", "Threshold%", "EMA", "TP%", "SL%", "TSL%", "Maru", "Days"
        ]
        # Append existing metric headers (shifted) or just fetch and write back?
        # Easier to just set the fixed param headers.
        
        print("📝 Updating Headers...")
        ws.update(range_name="C1:K1", values=[new_headers[2:]]) # C1 to K1
        
        # Call formatting to fix styles
        # Pass dummy headers for now or fetch new ones?
        # Fetching new R2 headers might be safer after data write if they align... 
        # But R2 headers (Trades/PnL) for weekly stats might now be misaligned with data!
        # The data shift `row[7:]` moves the content of weekly stats left by 1 column.
        # So we MUST move the headers left by 1 column too.
        
        # Header Migration:
        # Read R1, R2. Construct new R1, R2.
        
        # R1: A(Time), B(Strat), C(Side), D(Cond), E(EMA), F(Pump), G(Dump)...
        # New R1: ..., E(Threshold), F(EMA)...
        
        r1_new = headers_r1[:4] + ["Threshold%"] + headers_r1[4:] 
        # Wait, old E was EMA. New E is Threshold. Old F(Pump)/G(Dump) gone. Old E(EMA) moves to F.
        # Actually: A,B,C,D are same.
        # Old E (EMA) -> New F.
        # Old F (Pump) -> Moved to E (as values).
        # Old G (Dump) -> Merged to E.
        # Old H (TP) -> New G.
        
        # So we take [A..D], add [Threshold], add [EMA (Old E)], add [TP (Old H) onwards...]
        # Wait, Old E(EMA) is at index 4.
        # r1_new = h[0:4] + ["Threshold%"] + [h[4]] + h[7:]
        # h[4] is EMA. h[5] Pump, h[6] Dump. h[7] TP.
        # So yes: Head(4) + New + EMA(1) + Tail(from TP).
        
        r1_final = headers_r1[:4] + ["Threshold%"] + [headers_r1[4]] + headers_r1[7:]
        r2_final = headers_r2[:4] + [""] + [headers_r2[4]] + headers_r2[7:] # Weekly headers shift too
        
        ws.update(range_name="A1:ZZ1", values=[r1_final])
        ws.update(range_name="A2:ZZ2", values=[r2_final])
        
        sheets.apply_sheet_formatting(ws, r2_final)
        print("✨ Migration Complete!")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error during migration: {e}")

if __name__ == "__main__":
    migrate_layout()
