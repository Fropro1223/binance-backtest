#!/usr/bin/env python3
"""
Fix headers for Timeframe columns (N onwards).
Ensures proper Trades/PnL alternation.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gspread
from oauth2client.service_account import ServiceAccountCredentials

def fix_tf_headers():
    print("🔧 Connecting to Google Sheets to fix TF headers...")
    
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    sheet = client.open_by_key(MASTER_SHEET_ID)
    ws = sheet.worksheet("Analysis")
    print("✅ Opened 'Analysis' tab")
    
    # New correct structure (after Pump/Dump merge):
    # Cols 1-13: A(Timestamp), B(Strategy), C(Side), D(Cond), E(Threshold%), F(EMA), 
    #            G(TP%), H(SL%), I(TSL%), J(Maru), K(Days), L(WinRate), M(Trades), N(PnL)
    # Cols 14+ (O onwards): Timeframe Breakdown
    # O=5s Trades, P=5s PnL, Q=10s Trades, R=10s PnL, S=15s Trades, T=15s PnL, 
    # U=30s Trades, V=30s PnL, W=45s Trades, X=45s PnL, Y=1m Trades, Z=1m PnL
    
    # Row 1: TF labels (5s, 10s, etc) - span 2 cols each
    # Row 2: Trades, PnL alternating
    
    tf_order = ['5s', '10s', '15s', '30s', '45s', '1m']
    
    # Build header updates starting at Column O (index 15)
    start_col = 15  # O
    
    header_r1 = []
    header_r2 = []
    
    for tf in tf_order:
        header_r1.extend([tf, ''])  # TF label, then empty (merged look)
        header_r2.extend(['Trades', 'PnL'])
    
    # Update Row 1 and Row 2 from Column O onwards
    print(f"📝 Setting Row 1 (TF labels) from O: {header_r1}")
    print(f"📝 Setting Row 2 (Trades/PnL) from O: {header_r2}")
    
    # Calculate range: O1 to Z1 (6 TFs x 2 cols = 12 cols, O to Z)
    ws.update(range_name=f"O1:Z1", values=[header_r1], value_input_option='USER_ENTERED')
    ws.update(range_name=f"O2:Z2", values=[header_r2], value_input_option='USER_ENTERED')
    
    print("✅ Headers updated!")
    
    # Now re-apply formatting
    print("🎨 Re-applying layout formatting...")
    import sheets
    
    # Get fresh headers for formatting
    headers_r2_final = ws.row_values(2)
    sheets.apply_sheet_formatting(ws, headers_r2_final)
    
    print("✨ Fix complete! Check the sheet.")

if __name__ == "__main__":
    fix_tf_headers()
