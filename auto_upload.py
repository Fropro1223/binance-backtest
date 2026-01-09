import os
import time
import subprocess
import pandas as pd
from pathlib import Path
import sys

# System Rule 3: Backtest result files will appear inside ~/Algo/**/results/
# Since we are running inside the repo, and the user said "inside ~/Algo/**/results/",
# we scan for 'results' directories recursively from the current working directory 
# or specific known locations. 
# Assuming this script runs in valid root.

SEARCH_ROOT = Path(".").resolve() 

def find_newest_result_file():
    """
    Finds the newest .csv or .parquet file inside any 'results' subdirectory.
    """
    candidates = []
    
    # Walk through directory
    for root, dirs, files in os.walk(SEARCH_ROOT):
        if 'results' in Path(root).parts:
            for file in files:
                if file.endswith('.csv') or file.endswith('.parquet'):
                    full_path = Path(root) / file
                    candidates.append(full_path)
    
    if not candidates:
        return None
        
    # Get newest by modification time
    newest_file = max(candidates, key=lambda p: p.stat().st_mtime)
    return newest_file

def convert_parquet_to_csv(parquet_path: Path):
    """ Converts parquet to csv and returns the new csv path """
    csv_path = parquet_path.with_suffix('.csv')
    # Check if CSV already exists and is newer than parquet? 
    # For now, just overwrite to be safe and ensure latest data.
    print(f"Converting Parquet to CSV: {parquet_path} -> {csv_path}")
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path, index=False)
    return csv_path

def main():
    print("Auto-Upload Service Started...")
    print(f"Scanning for results in: {SEARCH_ROOT}")
    
    last_processed_file = None
    
    try:
        while True:
            newest_file = find_newest_result_file()
            
            if newest_file and newest_file != last_processed_file:
                print(f"\n[+] New result file detected: {newest_file}")
                
                target_csv = newest_file
                if newest_file.suffix == '.parquet':
                    try:
                        target_csv = convert_parquet_to_csv(newest_file)
                    except Exception as e:
                        print(f"[-] Error converting parquet: {e}")
                        import traceback
                        traceback.print_exc()
                        continue

                # Run sheets.py
                # System Rule: Run: python sheets.py <path_to_latest_csv>
                cmd = [sys.executable, "sheets.py", str(target_csv)]
                print(f"Executing: {' '.join(cmd)}")
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                print("--- sheets.py Output ---")
                print(result.stdout)
                if result.stderr:
                    print("--- sheets.py Error ---")
                    print(result.stderr)
                
                if result.returncode == 0:
                    print("[✓] Auto-upload cycle complete.")
                    last_processed_file = newest_file
                else:
                    print("[!] Upload failed.")
                    # We don't update last_processed_file so we retry? 
                    # Or maybe we skip to avoid infinite loops on broken files.
                    # User said "Do not silently fail", we printed stderr.
                    # We'll update last_processed to avoid infinite retry loop for the same broken file.
                    last_processed_file = newest_file 

            time.sleep(5)  # Check every 5 seconds
            
    except KeyboardInterrupt:
        print("\nStopping auto-upload service.")

if __name__ == "__main__":
    main()
