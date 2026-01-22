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
    
    print("🔧 Repairing Headers for 'backtest1'...")
    try:
        # PARAM STRUCTURE
        PARAM_COLS = {
            2: "Side", 3: "Cond", 4: "Threshold%", 5: "EMA", 
            6: "TP%", 7: "SL%", 8: "TSL%", 9: "Maru", 10: "Days"
        }

        # 1. Clear Header Rows
        try:
            ws.batch_clear(["A1:Z2"])
            print("   Cleared old headers.")
        except Exception as e:
            print(f"   Warning during clear: {e}")
        
        # 2. Prepare Row 1 and Row 2 Arrays (Up to Z for now)
        # 0-indexed logic for python lists
        r1 = [""] * 26 
        r2 = [""] * 26
        
        # A=0: Timestamp
        r1[0] = "Timestamp"
        # B=1: Strategy
        r1[1] = "Strategy"
        # C=2: Side
        r1[2] = "Side"
        
        # L=11: Results
        r1[11] = "Results"
        
        # C2-K2: Params
        for i in range(2, 11):
            if i in PARAM_COLS:
                r2[i] = PARAM_COLS[i]
                
        # L2-N2: Metrics
        r2[11] = "Win Rate"
        r2[12] = "Trades"
        r2[13] = "PnL ($)"
        
        # Timeframes: O(14) - Y(24)
        # 5s(14), 10s(16), 15s(18), 30s(20), 45s(22), 1m(24)
        tf_map = {'5s': 14, '10s': 16, '15s': 18, '30s': 20, '45s': 22, '1m': 24}
        
        for tf, idx in tf_map.items():
            r1[idx] = tf
            r2[idx] = "Trades"
            r2[idx+1] = "PnL"
            
        # Write to Sheet
        ws.update(range_name="A1:Z2", values=[r1, r2])
        print("   headers updated.")
        
        # Unhide A/B just in case - commenting out to prevent crash if method missing
        # try:
        #     ws.show_columns(0, 1) # A and B
        # except:
        #     pass
        
        print("✅ Headers Restored Successfully.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Repair failed: {e}")

if __name__ == "__main__":
    repair_headers()
