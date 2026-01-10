import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import time
import re

def fix_headers_final():
    creds_path = os.path.expanduser("~/.secrets/binance-backtest-sa.json")
    if not os.path.exists(creds_path):
        print(f"❌ Credentials not found at {creds_path}")
        return

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    manual_sheet_id = os.environ.get('MANUAL_SHEET_ID')
    
    try:
        sheet = client.open_by_key(manual_sheet_id)
        ws = sheet.worksheet("Analysis")
        print(f"🔧 Fixing Headers on '{ws.title}'...")
        
        # 1. Get Current State
        current_data = ws.get("A1:AZ2")
        r1 = current_data[0]
        # Ensure r2 exists and matches length
        if len(current_data) > 1:
            r2 = current_data[1]
        else:
            r2 = []
        
        # Pad r2
        if len(r2) < len(r1):
            r2.extend([""] * (len(r1) - len(r2)))
            
        print(f"   Read {len(r1)} columns.")
        
        # 2. Reconstruct Desired State
        # Cols A-E (Indices 0-4) are Standard.
        # Cols F-End (Index 5+) are Weekly Pairs.
        
        new_r1 = list(r1)
        new_r2 = list(r2)
        
        # Fix Standard
        # new_r2[0:5] = ["", "", "", "", ""] # Keep empty or whatever
        
        # Fix Weekly (Index 5+)
        # We assume they come in PAIRS.
        # We iterate i from 5. If we find a Label, we assume it's for i and i+1.
        
        merge_reqs = []
        
        i = 5
        while i < len(new_r1):
            # Check if this col has a label
            label = new_r1[i].strip()
            
            # Use Regex to detect date-like label? (dd.mm or just not empty)
            # Or if it's empty, maybe it's the second part of a merge?
            
            # Logic: 
            # If R1[i] has text, it's the start of a pair.
            # If R1[i] is empty, check R1[i-1]. If i-1 was a start, this is the continuation.
            
            if label:
                # It's a label. Assume it starts a block.
                # Is it a duplicate? "30.12-05.01" appearing twice?
                # If next col has SAME label, clear next col.
                if i + 1 < len(new_r1) and new_r1[i+1].strip() == label:
                    new_r1[i+1] = ""
                
                # Setup Pair
                new_r2[i] = "Trades"
                if i + 1 < len(new_r2):
                    new_r2[i+1] = "PnL"
                else:
                    new_r2.append("PnL")
                    new_r1.append("")
                    
                # Setup Merge
                # Merge R1 i:i+1
                merge_reqs.append({
                    "mergeCells": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": 0, "endRowIndex": 1,
                            "startColumnIndex": i, "endColumnIndex": i+2
                        },
                        "mergeType": "MERGE_ALL"
                    }
                })
                
                # Move to next pair
                i += 2
            else:
                # Empty R1.
                # Might be debris or second half of existing merge we missed?
                # or just empty gap.
                i += 1
        
        # 3. Apply Update
        print("🚀 Updating Header Values...")
        
        # Ensure sheet has enough columns
        max_len = max(len(new_r1), len(new_r2))
        
        # Always check + buffer
        if ws.col_count < max_len + 2:
            print(f"   Resizing sheet to {max_len + 5} columns...")
            ws.resize(cols=max_len + 5)
            # RELOAD object to update dimensions cache
            ws = sheet.worksheet("Analysis")
            
        print(f"   R1 Length: {len(new_r1)}")
        print(f"   R2 Length: {len(new_r2)}")
        
        # Unconstrained Update (Let Sheets figure it out)
        ws.update(range_name="A1", values=[new_r1, new_r2])
        
        # 4. Apply Merges
        if merge_reqs:
            print(f"🔗 Re-applying {len(merge_reqs)} merges...")
            # We assume existing merges might conflict? 
            # batch_update usually writes over.
            try:
                sheet.batch_update({"requests": merge_reqs})
            except Exception as e:
                print(f"Merge Error (ignoring): {e}")

        # Recalculate end_char for formatting
        end_char = gspread.utils.rowcol_to_a1(2, max_len)
        
        # 5. Apply Formatting (Center, Bold) just in case
        print("🎨 Polishing Header Format...")
        ws.format(f"A1:{end_char}", {
            "textFormat": {"bold": True},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "wrapStrategy": "WRAP"
        })
        
        # Sub-header Italic
        # Range A2:End2
        ws.format(f"A2:{end_char}", {
             "textFormat": {"bold": True, "italic": True},
             "horizontalAlignment": "CENTER"
        })

        print("✅ Header Fix Complete!")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    fix_headers_final()
