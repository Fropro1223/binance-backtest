import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

def list_all_cf():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key("1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM")
    
    metadata = sheet.fetch_sheet_metadata()
    ws_data = next(s for s in metadata['sheets'] if s['properties']['title'] == "backtest1")
    cf_rules = ws_data.get('conditionalFormats', [])
    
    print(f"Total rules: {len(cf_rules)}")
    for i, rule in enumerate(cf_rules):
        print(f"Rule {i}: {json.dumps(rule, indent=2)}")

if __name__ == "__main__":
    list_all_cf()
