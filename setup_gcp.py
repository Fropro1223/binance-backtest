import subprocess
import random
import os
import sys
import json
import time

def run_cmd(cmd, check=True):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 and check:
        print(f"Error: {result.stderr}")
        raise Exception(f"Command failed: {cmd}\n{result.stderr}")
    return result.stdout.strip()

def setup():
    # 1. Generate Project ID
    suffix = random.randint(1000, 9999)
    project_id = f"binance-backtest-{suffix}"
    print(f"Proposed Project ID: {project_id}")

    # 2. Create Project
    try:
        run_cmd(f"gcloud projects create {project_id} --name='Binance Backtest'")
    except Exception as e:
        print("Failed to create project. You might have hit a quota or need to enable billing.")
        sys.exit(1)

    print(f"Success: Project {project_id} created.")
    
    # 3. Set Project
    run_cmd(f"gcloud config set project {project_id}")

    # 4. Enable APIs (Sheets, Drive)
    print("Enabling APIs... (this takes a moment)")
    run_cmd(f"gcloud services enable sheets.googleapis.com drive.googleapis.com --project={project_id}")

    # 5. Create Service Account
    sa_name = "binance-bot"
    print(f"Creating Service Account: {sa_name}")
    run_cmd(f"gcloud iam service-accounts create {sa_name} --display-name='Binance Bot' --project={project_id}")

    # 6. Generate Key
    sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
    key_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    os.makedirs(os.path.dirname(key_path), exist_ok=True)
    
    print(f"Generating key for {sa_email}...")
    run_cmd(f"gcloud iam service-accounts keys create {key_path} --iam-account={sa_email} --project={project_id}")
    
    print("\n" + "="*50)
    print("SETUP COMPLETE!")
    print(f"Project ID: {project_id}")
    print(f"Service Email: {sa_email}")
    print(f"Key File: {key_path}")
    print("="*50)
    
    # 7. Print email explicitly for capture
    print(f"SERVICE_EMAIL:{sa_email}")

if __name__ == "__main__":
    setup()
