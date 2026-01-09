import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import os
from pathlib import Path

DEFAULT_CREDS_PATH = os.path.expanduser("~/Algo/credentials/google_service_account.json")

def cleanup():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    
    print(f"Using credentials: {DEFAULT_CREDS_PATH}")
    creds = ServiceAccountCredentials.from_json_keyfile_name(DEFAULT_CREDS_PATH, scope)
    access_token = creds.get_access_token().access_token
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # 1. List Files Owned by Me
    print("🔍 Searching for files owned by Service Account...")
    # q parameter: 'me' in owners AND not trashed
    params = {
        "q": "'me' in owners and trashed = false",
        "fields": "files(id, name, size, mimeType)"
    }
    
    r = requests.get("https://www.googleapis.com/drive/v3/files", headers=headers, params=params)
    r.raise_for_status()
    files = r.json().get('files', [])
    
    print(f"Found {len(files)} files owning.")
    
    for f in files:
        print(f"🗑 Deleting: {f['name']} ({f['id']})")
        # Delete (Move to trash? No, delete permanently or just delete)
        # requests.delete -> Permanently deletes? Or moves to trash?
        # Drive API v3 delete method permanently deletes.
        # But 'trash' method moves to trash.
        # Let's delete permanently to free space immediately.
        del_r = requests.delete(f"https://www.googleapis.com/drive/v3/files/{f['id']}", headers=headers)
        if del_r.status_code == 204:
             print("   ✅ Deleted.")
        else:
             print(f"   ❌ Failed: {del_r.text}")

    # 2. Empty Trash
    print("🗑 Emptying Trash...")
    r = requests.delete("https://www.googleapis.com/drive/v3/files/trash", headers=headers)
    if r.status_code == 204:
        print("✅ Trash emptied.")
    else:
        # It might return 200 or empty body
        print(f"Trash status: {r.status_code} {r.text}")

if __name__ == "__main__":
    cleanup()
