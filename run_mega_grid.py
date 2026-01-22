import subprocess
import time
import sys
import os

# Add project root to path so we can import sheets.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import sheets

def run_backtest(side, cond, ema, tp, sl, threshold):
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
    
    # Use specific threshold arg based on condition
    if cond == "pump":
        cmd.extend(["--pump", str(threshold)])
    else:
        cmd.extend(["--dump", str(threshold)])

    print(f"\n🚀 Running: [{side}] {cond.upper()} {threshold}% | EMA:{ema} | TP:{tp}% | SL:{sl}%")
    
    try:
        result = subprocess.run(cmd)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Execution error: {e}")
        return False

def generate_strategy_name(side, cond, ema, tp, sl, threshold):
    """
    Reconstructs the strategy name string to check against existing.
    Matches format in main.py: 
    f"[{side}] {cond.upper()} EMA:{ema} {cond.title()}:{threshold}% TP:{tp}% SL:{sl}% TSL:OFF M:0.8"
    """
    # Note: main.py format might vary slightly (spacing etc).
    # Best effort match or substring match.
    # Actually, main.py uses:
    # strategy_name = f"[{args.side}] {args.cond.upper()} EMA:{args.ema} {args.cond.title()}:{args.pump if args.cond=='pump' else args.dump}% TP:{args.tp}% SL:{args.sl}% TSL:{args.tsl}% M:{args.marubozu}"
    
    val = threshold
    return f"[{side}] {cond.upper()} EMA:{ema} {cond.title()}:{val}% TP:{tp}% SL:{sl}% TSL:OFF M:0.8"

def main():
    # 2 Sides x 2 Conds x 3 EMAs x 5 TP x 5 SL = 300 combinations
    sides = ["LONG", "SHORT"]
    conds = ["pump", "dump"]
    emas = ["none", "all_bull", "all_bear"]
    tps = [1.0, 3.0, 5.0, 8.0, 10.0]
    sls = [1.0, 3.0, 5.0, 8.0, 10.0]
    threshold = 2.0 # Fixed for this grid
    
    total = len(sides) * len(conds) * len(emas) * len(tps) * len(sls)
    
    print("🔍 Fetching existing completed runs from Google Sheets...")
    existing_strategies = sheets.get_existing_strategies()
    print(f"✅ Found {len(existing_strategies)} existing entries. Will skip duplicates.")
    
    count = 0
    skipped = 0
    
    print(f"🏁 Starting Smart Mega Grid Backtest: {total} combinations")
    print(f"Parallelism: 8 cores per run | Execution: Sequential batches")
    
    start_time = time.time()

    for side in sides:
        for cond in conds:
            for ema in emas:
                for tp in tps:
                    for sl in sls:
                        count += 1
                        
                        # Check duplication
                        # Construct expected name
                        # We need to be careful about float formatting. 2.0 vs 2 ?
                        # main.py args might be strings.
                        # user sheets.py logic force floats 1 decimal.
                        
                        # Let's simple check:
                        # Key params to check validity
                        # Instead of exact string match (fragile), let's rely on loose check?
                        # Or just reconstruct exact string.
                        
                        strat_name = generate_strategy_name(side, cond, ema, tp, sl, threshold)
                        
                        # Check if EXACT name exists
                        if strat_name in existing_strategies:
                             print(f"⏭️  Skipping [{count}/{total}] (Already exists): {strat_name}")
                             skipped += 1
                             continue

                        # Also check slight variations due to spacing or float formatting
                        # e.g. "2.0%" vs "2%"
                        # This is tricky.
                        # But since we just standardized the sheet, maybe it matches?
                        # Let's try running.
                        
                        # PROGRESS TRACKING
                        elapsed = time.time() - start_time
                        # Adjusted average based on actual runs
                        run_count = count - skipped
                        avg_time = elapsed / run_count if run_count > 0 else 0
                        remaining = avg_time * (total - count)
                        
                        print(f"\n--- [{count}/{total}] (Elapsed: {elapsed/60:.1f}m) ---")
                        
                        success = run_backtest(side, cond, ema, tp, sl, threshold)
                        
                        if success:
                            # Add to local cache so we don't re-run if we restart script
                            existing_strategies.add(strat_name)
                            # Small delay to reduce Google Sheets API quota pressure
                            time.sleep(1.5) 
                        else:
                            print(f"⚠️ Run failed. Continuing...")
                            time.sleep(2)

    print(f"\n✅ Mega Grid Search Complete! Total time: {(time.time() - start_time)/60:.1f} minutes.")
    print(f"Skipped {skipped} existing runs.")

if __name__ == "__main__":
    main()
