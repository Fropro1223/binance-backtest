import subprocess
import time

def run_backtest(tp, sl):
    cmd = [
        ".venv/bin/python", "main.py",
        "--strategy", "vectorized",
        "--side", "SHORT",
        "--pump", "2.0",
        "--tp", str(tp),
        "--sl", str(sl),
        "--marubozu", "0.8",
        "--ema", "none"
    ]
    print(f"\n🚀 Running: TP={tp}% | SL={sl}%")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"❌ Error in run: TP={tp}, SL={sl}")
    return result.returncode == 0

def main():
    # 6 TP values x 5 SL values = 30 combinations
    tps = [5.0, 5.5, 6.0, 6.5, 7.0, 7.5]
    sls = [3.0, 3.5, 4.0, 4.5, 5.0]
    
    total = len(tps) * len(sls)
    count = 0
    
    print(f"🏁 Starting Grid Backtest: {total} combinations")
    
    for tp in tps:
        for sl in sls:
            count += 1
            print(f"[{count}/{total}]", end=" ")
            success = run_backtest(tp, sl)
            if success:
                # Small delay to ensure Google Sheets API doesn't hit weird locks 
                # even though we are running sequentially
                time.sleep(1) 
            else:
                print("⚠️ Skipping delay due to failure.")

if __name__ == "__main__":
    main()
