import subprocess
import time
import os
import sys

def run_cmd(cmd):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    return True

def finish():
    project_id = "binance-backtest-5342"
    sa_email = f"binance-bot@{project_id}.iam.gserviceaccount.com"
    key_path = os.path.expanduser("~/Algo/credentials/google_service_account.json")
    
    print(f"Waiting for Service Account propagation (10s)...")
    time.sleep(10)
    
    print(f"Attempting to generate key for {sa_email}...")
    success = run_cmd(f"gcloud iam service-accounts keys create {key_path} --iam-account={sa_email} --project={project_id}")
    
    if success:
        print("\nSUCCESS!")
        print(f"SERVICE_EMAIL:{sa_email}")
    else:
        print("Failed again. Please check GCP Console.")

if __name__ == "__main__":
    finish()
