#!/usr/bin/env python3
"""
Creates a new 'backtest1' worksheet with correct headers and formatting.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gspread
from oauth2client.service_account import ServiceAccountCredentials

def create_backtest1_sheet():
    print("🚀 Connecting to Google Sheets...")
    
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    sheet = client.open_by_key(MASTER_SHEET_ID)
    
    # Check if backtest1 already exists
    try:
        ws = sheet.worksheet("backtest1")
        print("⚠️ 'backtest1' already exists. Deleting and recreating...")
        sheet.del_worksheet(ws)
    except gspread.WorksheetNotFound:
        pass
    
    # Create new worksheet
    ws = sheet.add_worksheet(title="backtest1", rows=1000, cols=50)
    print("✅ Created 'backtest1' worksheet")
    
    # Set up Row 1 headers (Parameter columns + Metrics + TF breakdown)
    row1_headers = [
        "Timestamp",    # A
        "Strategy",     # B
        "Side",         # C
        "Cond",         # D
        "Threshold%",   # E
        "EMA",          # F
        "TP%",          # G
        "SL%",          # H
        "TSL%",         # I
        "Maru",         # J
        "Days",         # K
        "WinRate",      # L
        "Trades",       # M
        "PnL",          # N
        # TF Breakdown (O-Z): 6 TFs x 2 cols = 12 cols
        "5s", "",       # O, P
        "10s", "",      # Q, R
        "15s", "",      # S, T
        "30s", "",      # U, V
        "45s", "",      # W, X
        "1m", ""        # Y, Z
    ]
    
    # Set up Row 2 headers (sub-labels for TF breakdown)
    row2_headers = [
        "",  # A - Timestamp
        "",  # B - Strategy
        "",  # C - Side
        "",  # D - Cond
        "",  # E - Threshold%
        "",  # F - EMA
        "",  # G - TP%
        "",  # H - SL%
        "",  # I - TSL%
        "",  # J - Maru
        "",  # K - Days
        "",  # L - WinRate
        "",  # M - Trades
        "($)",  # N - PnL
        # TF Breakdown (O-Z): [PnL, Trades] per TF
        "PnL", "Trades",    # O, P (5s)
        "PnL", "Trades",    # Q, R (10s)
        "PnL", "Trades",    # S, T (15s)
        "PnL", "Trades",    # U, V (30s)
        "PnL", "Trades",    # W, X (45s)
        "PnL", "Trades"     # Y, Z (1m)
    ]
    
    print("📝 Writing headers...")
    ws.update(range_name="A1:Z1", values=[row1_headers], value_input_option='USER_ENTERED')
    ws.update(range_name="A2:Z2", values=[row2_headers], value_input_option='USER_ENTERED')
    
    print("🎨 Applying formatting...")
    import sheets
    
    # Apply data validation (dropdowns)
    sheets.apply_data_validation(ws)
    
    # Apply formatting
    headers_r2_final = ws.row_values(2)
    sheets.apply_sheet_formatting(ws, headers_r2_final)
    
    print("✨ 'backtest1' sheet created and formatted!")
    print("📌 Don't forget to update sheets.py to use 'backtest1' as target worksheet.")

if __name__ == "__main__":
    create_backtest1_sheet()
