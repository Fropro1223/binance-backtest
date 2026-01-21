import subprocess
import time

def run_tsl_experiment():
    # Base parameters
    side = "SHORT"
    strategy = "vectorized"
    ema = "all_bull"
    pump = 2.0
    tp = 8.0
    sl = 8.0
    maru = 0.8
    
    # TSL Variations: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    tsl_values = range(1, 11)
    
    print(f"🚀 Starting TSL Experiment: {len(tsl_values)} variations")
    print(f"Params: {side} | Pump {pump}% | TP {tp}% | SL {sl}% | Maru {maru}")
    print("-" * 50)
    
    for tsl in tsl_values:
        print(f"\n▶️ Running variation: TSL {tsl}%")
        
        cmd = [
            ".venv/bin/python", "main.py",
            "--strategy", strategy,
            "--side", side,
            "--ema", ema,
            "--pump", str(pump),
            "--tp", str(tp),
            "--sl", str(sl),
            "--tsl", str(tsl),
            "--marubozu", str(maru),
            "--workers", "8"  # Use 8 cores for parallel pair processing
        ]
        
        try:
            # Running with shell=False for safety
            subprocess.run(cmd, check=True)
            print(f"✅ Variation TSL {tsl}% completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"❌ Variation TSL {tsl}% failed: {e}")
        
        # Short cooldown to avoid API spikes
        time.sleep(2)

    print("\n" + "="*50)
    print("🎯 TOTAL EXPERIMENT COMPLETE 🎯")
    print("="*50)

if __name__ == "__main__":
    run_tsl_experiment()
