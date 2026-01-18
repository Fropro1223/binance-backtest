#!/usr/bin/env python3
"""
Data Migration Script
=====================
Consolidates weekly parquet data from /Users/firat/Algo/backtest_data/raw/
into the format expected by backtest scripts (data/processed/).

Structure:
  SOURCE: /Users/firat/Algo/backtest_data/raw/SYMBOL/TIMEFRAME/WEEK.parquet
  TARGET: data/processed/SYMBOL_TF.parquet

Usage:
    python migrate_data.py
    python migrate_data.py --dry-run
    python migrate_data.py --workers 8
"""

import argparse
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple
import polars as pl
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SOURCE_DIR = Path("/Users/firat/Algo/backtest_data/raw")
TARGET_DIR = Path("data/processed")


def get_symbol_timeframe_pairs() -> List[Tuple[str, str]]:
    """
    Scans SOURCE_DIR and returns list of (symbol, timeframe) pairs.
    
    Returns:
        List of tuples: [(symbol, timeframe), ...]
    """
    pairs = []
    
    if not SOURCE_DIR.exists():
        logger.error(f"Source directory not found: {SOURCE_DIR}")
        return pairs
    
    for symbol_dir in SOURCE_DIR.iterdir():
        if not symbol_dir.is_dir():
            continue
            
        symbol = symbol_dir.name
        
        for tf_dir in symbol_dir.iterdir():
            if not tf_dir.is_dir():
                continue
                
            timeframe = tf_dir.name
            pairs.append((symbol, timeframe))
    
    return pairs


def migrate_symbol_timeframe(symbol: str, timeframe: str, dry_run: bool = False) -> dict:
    """
    Migrates one symbol-timeframe combination.
    
    Args:
        symbol: Symbol name (e.g., 'BTCUSDT')
        timeframe: Timeframe (e.g., '45s')
        dry_run: If True, only simulate without writing files
    
    Returns:
        dict with status info: {'success': bool, 'rows': int, 'error': str}
    """
    result = {
        'symbol': symbol,
        'timeframe': timeframe,
        'success': False,
        'rows': 0,
        'error': None
    }
    
    try:
        # Find all weekly parquet files
        tf_dir = SOURCE_DIR / symbol / timeframe
        
        if not tf_dir.exists():
            result['error'] = 'Directory not found'
            return result
        
        week_files = sorted(tf_dir.glob("*.parquet"))
        
        if not week_files:
            result['error'] = 'No parquet files found'
            return result
        
        # Read and concatenate all weeks
        dfs = []
        for week_file in week_files:
            try:
                df = pl.read_parquet(week_file)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read {week_file}: {e}")
                continue
        
        if not dfs:
            result['error'] = 'No valid data frames'
            return result
        
        # Concatenate and sort
        combined_df = pl.concat(dfs)
        combined_df = combined_df.sort("ts_1s")
        
        # Remove duplicates (just in case)
        # Keep first occurrence of each timestamp
        combined_df = combined_df.unique(subset=["ts_1s"], keep="first")
        
        result['rows'] = len(combined_df)
        
        if not dry_run:
            # Save to target directory
            TARGET_DIR.mkdir(parents=True, exist_ok=True)
            
            target_file = TARGET_DIR / f"{symbol}_{timeframe}.parquet"
            combined_df.write_parquet(target_file)
            
            logger.debug(f"Written {target_file}: {len(combined_df):,} rows")
        
        result['success'] = True
        
    except Exception as e:
        result['error'] = str(e)
        logger.error(f"Error migrating {symbol}_{timeframe}: {e}")
    
    return result


def run_migration(workers: int = 4, dry_run: bool = False):
    """
    Runs the migration process.
    
    Args:
        workers: Number of parallel workers
        dry_run: If True, simulates without writing files
    """
    logger.info("Starting data migration...")
    logger.info(f"Source: {SOURCE_DIR}")
    logger.info(f"Target: {TARGET_DIR}")
    logger.info(f"Workers: {workers}")
    logger.info(f"Dry run: {dry_run}")
    
    # Get all symbol-timeframe pairs
    pairs = get_symbol_timeframe_pairs()
    
    if not pairs:
        logger.error("No symbol-timeframe pairs found!")
        return
    
    logger.info(f"Found {len(pairs)} symbol-timeframe combinations")
    
    # Process in parallel
    results = []
    successful = 0
    failed = 0
    total_rows = 0
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all tasks
        future_to_pair = {
            executor.submit(migrate_symbol_timeframe, symbol, tf, dry_run): (symbol, tf)
            for symbol, tf in pairs
        }
        
        # Process results with progress bar
        for future in tqdm(as_completed(future_to_pair), total=len(pairs), desc="Migrating"):
            result = future.result()
            results.append(result)
            
            if result['success']:
                successful += 1
                total_rows += result['rows']
            else:
                failed += 1
                logger.warning(
                    f"Failed: {result['symbol']}_{result['timeframe']} - {result['error']}"
                )
    
    # Summary
    print("\n" + "=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    print(f"Total combinations: {len(pairs)}")
    print(f"Successful:         {successful}")
    print(f"Failed:             {failed}")
    print(f"Total rows:         {total_rows:,}")
    
    if dry_run:
        print("\n⚠️  DRY RUN - No files were written")
    else:
        print(f"\n✅ Files written to: {TARGET_DIR}")
    
    # Show date range from a sample file
    if successful > 0 and not dry_run:
        try:
            # Get first successful result
            sample = next(r for r in results if r['success'])
            sample_file = TARGET_DIR / f"{sample['symbol']}_{sample['timeframe']}.parquet"
            
            df = pl.read_parquet(sample_file)
            min_date = df["ts_1s"].min()
            max_date = df["ts_1s"].max()
            
            print(f"\nSample date range ({sample['symbol']}_{sample['timeframe']}):")
            print(f"  From: {min_date}")
            print(f"  To:   {max_date}")
        except Exception as e:
            logger.warning(f"Could not read sample file: {e}")
    
    print("=" * 60)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Migrate weekly parquet data to backtest format",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate migration without writing files'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    try:
        run_migration(workers=args.workers, dry_run=args.dry_run)
    except KeyboardInterrupt:
        print("\n\n⚠️  Migration interrupted by user")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


if __name__ == "__main__":
    main()
