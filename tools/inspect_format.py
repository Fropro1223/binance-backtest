
import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
import json

def inspect_format():
    creds_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
    client = gspread.authorize(creds)
    
    MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
    ss = client.open_by_key(MASTER_SHEET_ID)
    ws = ss.worksheet("backtest1")
    
    # Get cell A1 metadata
    # We need to use the Developer Metadata or just the spreadsheet.get with ranges
    res = ss.fetch_sheet_metadata()
    # Actually fetch_sheet_metadata is quite large. Let's use the API directly via requests or a targeted gspread call if possible.
    # Standard gspread Doesn't easily give cell format in one call without 'includeGridData=True'
    
    # Targeted API call
    spreadsheet = client.open_by_key(MASTER_SHEET_ID)
    data = spreadsheet.values_get('backtest1!A1:A1', params={'valueRenderOption': 'FORMATTED_VALUE'})
    print(f"Formatted Value: {data}")
    
    # Let's try to get the actual format object
    # Using spreadsheet.get from the raw API (via gspread's client)
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{MASTER_SHEET_ID}?ranges=backtest1!A1:A1&includeGridData=true"
    response = client.request('get', url)
    metadata = response.json()
    
    cell_data = metadata['sheets'][0]['data'][0]['rowData'][0]['values'][0]
    print("Cell A1 Metadata:")
    print(json.dumps(cell_data.get('effectiveFormat', {}).get('textFormat', {}), indent=2))
    print(json.dumps(cell_data.get('userEnteredFormat', {}).get('textFormat', {}), indent=2))

if __name__ == "__main__":
    inspect_format()
