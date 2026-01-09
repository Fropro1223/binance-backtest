import polars as pl
from pathlib import Path
from tqdm import tqdm
from typing import List, Dict
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

from src import config, strategy, utils

logger = utils.setup_logging("scanner")

def analyze_file(file_path: Path) -> List[Dict]:
    """
    Analyzes a single parquet file for signals.
    Returns a list of signal events (dicts).
    """
    try:
        df = pl.read_parquet(file_path)
        if df.is_empty():
            return []
            
        df = strategy.detect_signals(df)
        
        # Filter for signals
        signal_df = df.filter(pl.col("signal_entry"))
        
        if signal_df.is_empty():
            return []
            
        # Parse Symbol and Timeframe from filename (e.g., BTCUSDT_5s.parquet)
        stem = file_path.stem # BTCUSDT_5s
        parts = stem.split('_')
        symbol = parts[0]
        tf = parts[1]
        
        results = []
        for row in signal_df.iter_rows(named=True):
            results.append({
                "symbol": symbol,
                "timeframe": tf,
                "timestamp": row['ts_1s'], # datetime
                "open": row['open'],
                "close": row['close'],
                "change_pct": row['change_pct'],
                "marubozu_ratio": row['marubozu_ratio']
            })
            
        return results
        
    except Exception as e:
        logger.error(f"Error scanning {file_path}: {e}")
        return []

def run_scanner(parallel: bool = True):
    """
    Scans all files in data/processed and reports signals.
    """
    files = list(config.PROCESSED_DATA_DIR.glob("*.parquet"))
    logger.info(f"Found {len(files)} files to scan.")
    
    all_signals = []
    
    if parallel:
        # CPU bound task -> ProcessPoolExecutor
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(analyze_file, f) for f in files]
            for future in tqdm(as_completed(futures), total=len(files), desc="Scanning"):
                res = future.result()
                if res:
                    all_signals.extend(res)
    else:
        for f in tqdm(files, desc="Scanning"):
            res = analyze_file(f)
            if res:
                all_signals.extend(res)
                
    logger.info(f"Scan complete. Found {len(all_signals)} potential entry signals.")
    
    if all_signals:
        # Convert to DataFrame for nice display/saving
        result_df = pl.DataFrame(all_signals)
        # Sort by timestamp
        result_df = result_df.sort("timestamp")
        
        output_csv = config.DATA_DIR / "signals_report.csv"
        result_df.write_csv(output_csv)
        logger.info(f"Report saved to {output_csv}")
        
        # Print sample
        print("\nTop 10 Recent Signals:")
        print(result_df.tail(10))
    else:
        print("No signals found matching criteria.")

if __name__ == "__main__":
    run_scanner()
