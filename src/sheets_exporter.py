import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from pathlib import Path
import sys
from src import config, utils

logger = utils.setup_logging("sheets_exporter")

def export_to_sheets(csv_path: Path, credentials_path: Path, sheet_name: str = "Binance Signals"):
    """
    Uploads a CSV file to a Google Sheet.
    Creates the sheet if it doesn't exist (requires permissions), or opens existing one.
    """
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return

    if not credentials_path.exists():
        logger.error(f"Credentials file not found: {credentials_path}")
        print(f"\n[!] Error: 'credentials.json' is missing.")
        print("    Please place your Google Cloud Service Account JSON key at:")
        print(f"    {credentials_path}")
        return

    try:
        logger.info("Authenticating with Google Sheets...")
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(credentials_path), scope)
        client = gspread.authorize(creds)

        # Read CSV
        logger.info(f"Reading CSV data from {csv_path}...")
        df = pd.read_csv(csv_path)
        # Handle NaN values (JSON doesn't like NaN)
        df = df.fillna("")
        
        # Convert to list of lists
        data = [df.columns.tolist()] + df.values.tolist()

        try:
            sheet = client.open(sheet_name)
            worksheet = sheet.sheet1 # First worksheet
            logger.info(f"Opened existing sheet: {sheet_name}")
        except gspread.SpreadsheetNotFound:
            logger.info(f"Sheet '{sheet_name}' not found. Creating new one...")
            sheet = client.create(sheet_name)
            worksheet = sheet.sheet1
            # Note: You need to share this sheet with your personal email to see it!
            logger.info(f"Created new sheet: {sheet.url}")
            print(f"\n[+] Created new sheet: {sheet.url}")
            print(f"    IMPORTANT: This sheet is currently owned by the Service Account.")
            print(f"    You must share it with your personal email address to view it.")
            
            # Optional: Share programmatically if user email is known
            # sheet.share('user@example.com', perm_type='user', role='writer')

        logger.info("Clearing existing data...")
        worksheet.clear()
        
        logger.info(f"Uploading {len(data)} rows...")
        worksheet.update(data)
        
        logger.info("Upload complete!")
        print(f"Successfully uploaded to: {sheet.url}")

    except Exception as e:
        logger.error(f"Failed to export to Sheets: {e}")
        # Print full exception for debugging
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    csv_file = config.DATA_DIR / "signals_report.csv"
    creds_file = Path("credentials.json") # Expected in root
    
    export_to_sheets(csv_file, creds_file)
