import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

def repair_headers():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    ss = client.open_by_key(MASTER_SHEET_ID)
    ws = ss.worksheet("backtest1")
    
    print("🔧 Repairing Headers for 'backtest1' (Commission + Net PnL aligned)...")
    try:
        # PARAM STRUCTURE (C-K | Indices 2-10)
        PARAM_COLS = {
            2: "Side", 3: "Cond", 4: "Threshold%", 5: "EMA", 
            6: "TP%", 7: "SL%", 8: "TSL%", 9: "Maru", 10: "Days"
        }

        # Expand rows to cover Weekly Breakdown (AC starts at 29 -> Index 28)
        # 15 weeks * 2 = 30 columns. 28 + 30 = 58. Let's use 100.
        r1 = [""] * 100 
        r2 = [""] * 100
        
        # Row 1 - Main Categories
        r1[0] = "Timestamp"  # A
        r1[1] = "Strategy"   # B
        r1[2] = "Side"       # C
        r1[11] = "Results"   # L
        
        # Row 2 - Detailed Headers
        for i in range(2, 11):
            if i in PARAM_COLS:
                r2[i] = PARAM_COLS[i]
                
        r2[11] = "Win Rate"  # L
        r2[12] = "Trades"    # M
        r2[13] = "PnL ($)"   # N
        
        # New Metrics
        r1[14] = "Commission" # O
        r2[14] = "$"
        r1[15] = "Net PnL"   # P
        r2[15] = "$"

        # Timeframes: Q(16) onwards (0-indexed)
        # 5s(16), 10s(18), 15s(20), 30s(22), 45s(24), 1m(26)
        tf_map = {'5s': 16, '10s': 18, '15s': 20, '30s': 22, '45s': 24, '1m': 26}
        
        for tf, idx in tf_map.items():
            r1[idx] = tf
            r2[idx] = "Trades"
            r2[idx+1] = "PnL"
            
        # Weekly Breakdown: AC(28) onwards (0-indexed)
        # We'll generate them matching the main.py logic (Last 15 completed weeks)
        import pandas as pd
        today = pd.Timestamp.now(tz='Europe/Istanbul')
        
        # Anchor: Last Sunday 03:00
        current_sun_03 = today.replace(hour=3, minute=0, second=0, microsecond=0)
        while current_sun_03.weekday() != 6:
             current_sun_03 -= pd.Timedelta(days=1)
        if today.weekday() == 6 and today.hour < 3:
             current_sun_03 -= pd.Timedelta(days=7)
             
        # Shifting back by 7 days to match 'last completed' first column
        current_sun_03 -= pd.Timedelta(days=7)
        
        WEEKLY_START_INDEX = 28 # AC
        for i in range(15):
            ws_date = current_sun_03 - pd.Timedelta(days=7*i)
            we_date = ws_date + pd.Timedelta(days=7)
            
            label_date = f"{ws_date.strftime('%d/%m')}-{we_date.strftime('%d/%m')}"
            label_week = f"W{ws_date.isocalendar()[1]:02d}" # Use Wnn for alternating?
            
            idx = WEEKLY_START_INDEX + (i * 2)
            
            # Alternating headers as seen in screenshot: Date(AC), Week(AE), Date(AG)...
            if i % 2 == 0:
                r1[idx] = label_date
            else:
                r1[idx] = label_week
                
            r2[idx] = "Trades"
            r2[idx+1] = "PnL"

        # Update Sheet
        ws.update(range_name="A1:CV2", values=[r1, r2])
        print("   ✅ headers updated (A1:CV2).")
        
        print("✅ Headers Restored and Aligned Successfully.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Repair failed: {e}")

if __name__ == "__main__":
    repair_headers()
