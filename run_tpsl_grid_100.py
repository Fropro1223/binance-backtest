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
    # 10 TP values x 10 SL values = 100 combinations
    tps = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    sls = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    
    total = len(tps) * len(sls)
    count = 0
    
    print(f"🏁 Starting Grid Backtest: {total} combinations")
    
    for tp in tps:
        for sl in sls:
            count += 1
            if count < 28:
                continue
                
            print(f"[{count}/{total}]", end=" ")
            success = run_backtest(tp, sl)
            if success:
                # Small delay to reduce API load
                time.sleep(1) 
            else:
                print("⚠️ Skipping delay due to failure.")

if __name__ == "__main__":
    main()
