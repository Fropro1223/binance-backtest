import pandas as pd
import sys

def verify_overlaps():
    csv_file = "backtest_results_pump.csv"
    print(f"🕵️‍♂️ Checking for overlaps in {csv_file}...")
    
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"❌ Could not read CSV: {e}")
        return

    if df.empty:
        print("⚠️ No trades to check.")
        return

    # Convert times to datetime
    # Assuming standard string format or epoch. 
    # Based on previous context, might be string. Let's try flexible parsing.
    try:
        df['entry_dt'] = pd.to_datetime(df['entry_time'])
        df['exit_dt'] = pd.to_datetime(df['exit_time'])
    except Exception as e:
        print(f"❌ Date parsing error: {e}")
        # Try inspecting first row
        print("Sample row:")
        print(df.iloc[0])
        return

    overlaps_found = 0
    symbols_with_overlaps = []

    # Check per symbol
    grouped = df.groupby('symbol')
    
    print(f"🔍 Analyzing {len(grouped)} symbols with trades...")
    
    for symbol, group in grouped:
        # Sort by entry time
        group = group.sort_values('entry_dt')
        
        # Check if next entry is before current exit
        # shift(-1) gives the *next* row's values
        # We want: Next_Entry >= Current_Exit
        # Overlap if: Next_Entry < Current_Exit
        
        entries = group['entry_dt'].values
        exits = group['exit_dt'].values
        
        # Compare Entry[i+1] with Exit[i]
        # entries[1:] < exits[:-1]
        
        if len(group) > 1:
            next_entries = entries[1:]
            current_exits = exits[:-1]
            
            # Use strict less than. If Entry == Exit, that's fine (sell then buy same candle/time? assuming resolution allows)
            # Actually, usually safer to say Entry > Exit.
            # But let's check for <
            
            overlap_mask = next_entries < current_exits
            if overlap_mask.any():
                overlaps_found += overlap_mask.sum()
                symbols_with_overlaps.append(symbol)
                
                # Print details of first overlap
                idx = overlap_mask.argmax() # first True
                print(f"❌ Overlap detected in {symbol}:")
                print(f"   Trade A Exit:  {current_exits[idx]}")
                print(f"   Trade B Entry: {next_entries[idx]}")

    if overlaps_found == 0:
        print("\n✅ VERIFICATION SUCCESSFUL: No overlaps found.")
        print("   Every trade for a pair strictly waits for the previous one to close.")
    else:
        print(f"\n❌ VERIFICATION FAILED: {overlaps_found} overlaps detected!")
        print(f"   Symbols: {symbols_with_overlaps[:5]}...")

if __name__ == "__main__":
    verify_overlaps()
