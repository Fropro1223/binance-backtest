import subprocess
import time

def run_backtest(side, cond, tp, sl):
    cmd = [
        ".venv/bin/python", "main.py",
        "--strategy", "vectorized",
        "--side", side,
        "--cond", cond,
        "--pump", "2.0",
        "--dump", "2.0",
        "--tp", str(tp),
        "--sl", str(sl),
        "--marubozu", "0.8",
        "--ema", "none"
    ]
    print(f"\n🚀 Running: SIDE={side} | COND={cond} | TP={tp}% | SL={sl}%")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"❌ Error in run: {side}/{cond}")
    return result.returncode == 0

def main():
    # Test all variations
    sides = ["LONG", "SHORT"]
    conds = ["pump", "dump"]
    
    # Example TP/SL to use for this cross-study (can be adjusted)
    tps = [5.0]
    sls = [5.0]
    
    total = len(sides) * len(conds) * len(tps) * len(sls)
    count = 0
    
    print(f"🏁 Starting Cross-Study Backtest: {total} combinations")
    
    for side in sides:
        for cond in conds:
            for tp in tps:
                for sl in sls:
                    count += 1
                    print(f"[{count}/{total}]", end=" ")
                    success = run_backtest(side, cond, tp, sl)
                    if success:
                        time.sleep(1) # Small cooldown for API
                    else:
                        print("⚠️ Skipping delay.")

if __name__ == "__main__":
    main()
