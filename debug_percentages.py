
import subprocess
import json
import re

def get_backtest_stats(tp, sl):
    cmd = [
        ".venv/bin/python", "main.py",
        "--strategy", "vectorized",
        "--side", "SHORT",
        "--pump", "2.0",
        "--tp", str(tp),
        "--sl", str(sl),
        "--marubozu", "0.8",
        "--ema", "none",
        "--no-sheets"
    ]
    print(f"Running backtest TP={tp}, SL={sl}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # Extract total trades and weekly counts from output
    total_trades = 0
    weekly_counts = []
    
    # regex for "Total Trades: (\d+)"
    total_match = re.search(r"Total Trades: (\d+)", result.stdout)
    if total_match:
        total_trades = int(total_match.group(1))
        
    # extract weekly table rows
    # format: "11/01-18/01     | 1562     | $8.82"
    lines = result.stdout.split('\n')
    for line in lines:
        if "/" in line and "|" in line:
            parts = line.split('|')
            if len(parts) >= 2:
                try:
                    count = int(parts[1].strip())
                    weekly_counts.append(count)
                except: pass
                
    return total_trades, weekly_counts

def main():
    t1, w1 = get_backtest_stats(3.0, 7.0)
    t2, w2 = get_backtest_stats(6.0, 4.0)
    
    print(f"\nResults Comparison:")
    print(f"BT 1 (3/7): Total={t1}, Weekly={w1}")
    print(f"BT 2 (6/4): Total={t2}, Weekly={w2}")
    
    if t1 > 0:
        p1 = [round(x/t1, 5) for x in w1]
        print(f"Percentages 1: {p1}")
    if t2 > 0:
        p2 = [round(x/t2, 5) for x in w2]
        print(f"Percentages 2: {p2}")

if __name__ == "__main__":
    main()
