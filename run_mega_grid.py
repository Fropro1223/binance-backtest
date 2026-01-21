import subprocess
import time
import sys
import os

def run_backtest(side, cond, ema, tp, sl):
    """
    Runs a single backtest variation using main.py
    """
    cmd = [
        ".venv/bin/python", "main.py",
        "--strategy", "vectorized",
        "--side", side,
        "--cond", cond,
        "--ema", ema,
        "--tp", str(tp),
        "--sl", str(sl),
        "--marubozu", "0.8",
        "--workers", "8"  # Use 8 cores for speed
    ]
    
    # Specific threshold based on condition
    if cond == "pump":
        cmd.extend(["--pump", "2.0"])
    else:
        cmd.extend(["--dump", "2.0"])

    print(f"\n🚀 Running: [{side}] {cond.upper()} | EMA:{ema} | TP:{tp}% | SL:{sl}%")
    
    try:
        result = subprocess.run(cmd)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Execution error: {e}")
        return False

def main():
    # 2 Sides x 2 Conds x 3 EMAs x 5 TP x 5 SL = 300 combinations
    sides = ["LONG", "SHORT"]
    conds = ["pump", "dump"]
    emas = ["none", "all_bull", "all_bear"]
    tps = [1.0, 3.0, 5.0, 8.0, 10.0]
    sls = [1.0, 3.0, 5.0, 8.0, 10.0]
    
    total = len(sides) * len(conds) * len(emas) * len(tps) * len(sls)
    count = 0
    
    print(f"🏁 Starting Mega Grid Backtest: {total} combinations")
    print(f"Parallelism: 8 cores per run | Execution: Sequential batches")
    
    start_time = time.time()

    for side in sides:
        for cond in conds:
            for ema in emas:
                for tp in tps:
                    for sl in sls:
                        count += 1
                        
                        # PROGRESS TRACKING
                        elapsed = time.time() - start_time
                        avg_time = elapsed / count if count > 0 else 0
                        remaining = avg_time * (total - count)
                        
                        print(f"\n--- [{count}/{total}] (Elapsed: {elapsed/60:.1f}m, Est. Remaining: {remaining/60:.1f}m) ---")
                        
                        success = run_backtest(side, cond, ema, tp, sl)
                        
                        if success:
                            # Small delay to reduce Google Sheets API quota pressure
                            time.sleep(1.5) 
                        else:
                            print(f"⚠️ Run failed for {side} {cond} {ema} {tp}/{sl}. Continuing...")
                            time.sleep(2)

    print(f"\n✅ Mega Grid Search Complete! Total time: {(time.time() - start_time)/60:.1f} minutes.")

if __name__ == "__main__":
    main()
