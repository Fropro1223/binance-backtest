import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import os

DEFAULT_CREDS_PATH = os.path.expanduser("~/Algo/credentials/google_service_account.json")

def check_quota():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    
    print(f"Using credentials: {DEFAULT_CREDS_PATH}")
    creds = ServiceAccountCredentials.from_json_keyfile_name(DEFAULT_CREDS_PATH, scope)
    access_token = creds.get_access_token().access_token
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Check About info for Quota
    print("🔍 Checking Drive Storage Quota...")
    params = {
        "fields": "storageQuota,user"
    }
    
    r = requests.get("https://www.googleapis.com/drive/v3/about", headers=headers, params=params)
    if r.status_code != 200:
        print(f"❌ Failed to get info: {r.text}")
        return

    data = r.json()
    quota = data.get('storageQuota', {})
    user_info = data.get('user', {})
    
    limit = int(quota.get('limit', 0))
    usage = int(quota.get('usage', 0))
    usage_drive = int(quota.get('usageInDrive', 0))
    usage_trash = int(quota.get('usageInDriveTrash', 0))
    
    def bytes_to_gb(b):
        return b / (1024**3)

    print(f"\n👤 Account Name: {user_info.get('displayName')} ({user_info.get('emailAddress')})")
    print("-" * 40)
    print(f"💾 Total Limit: {bytes_to_gb(limit):.2f} GB")
    print(f"📉 Total Used:  {bytes_to_gb(usage):.2f} GB")
    print(f"   - Drive:     {bytes_to_gb(usage_drive):.2f} GB")
    print(f"   - Trash:     {bytes_to_gb(usage_trash):.2f} GB")
    print("-" * 40)
    
    if limit > 0:
        percent = (usage / limit) * 100
        print(f"📊 Usage: {percent:.1f}%")
    else:
        print("📊 Usage: Unlimited or Unknown")

if __name__ == "__main__":
    check_quota()
