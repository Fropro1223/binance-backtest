
import pandas as pd
import sys

# Sample date from CSV
date_str = "2025-10-10T20:30:30.000000"
print(f"Original String: {date_str}")

# Method 1: Naive -> int64
dt = pd.to_datetime(date_str)
ts1 = dt.value // 10**9
print(f"Naive -> int64: {ts1}")

# Method 2: Localize UTC -> timestamp()
dt_utc = pd.to_datetime(date_str).tz_localize('UTC')
ts2 = int(dt_utc.timestamp())
print(f"UTC -> timestamp(): {ts2}")

# Expected (from user example): 1760128230
if ts1 == 1760128230:
    print("Method 1 matches expected.")
else:
    print(f"Method 1 DIFFERS: {ts1 - 1760128230}s diff")

if ts2 == 1760128230:
    print("Method 2 matches expected.")
else:
    print(f"Method 2 DIFFERS: {ts2 - 1760128230}s diff")
