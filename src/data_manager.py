import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

import polars as pl
from tqdm import tqdm

from src import config, downloader, processor, utils_date
from src.utils import setup_logging

logger = logging.getLogger("data_manager")

MANIFEST_FILE = config.META_DIR / "manifest.json"

def load_manifest() -> Dict:
    if MANIFEST_FILE.exists():
        try:
            with open(MANIFEST_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load manifest: {e}")
            return {}
    return {}

def save_manifest(manifest: Dict):
    try:
        with open(MANIFEST_FILE, 'w') as f:
            json.dump(manifest, f, indent=4, default=str)
    except Exception as e:
        logger.error(f"Failed to save manifest: {e}")

def get_target_weeks() -> List[tuple]:
    """
    Determines which weeks need to be downloaded.
    Returns list of (start_dt, end_dt) tuples.
    """
    manifest = load_manifest()
    last_completed_str = manifest.get('last_completed_week_end')
    
    current_completed = utils_date.get_last_completed_week_end()
    
    if last_completed_str:
        # Resume from last checkpoint
        last_completed = datetime.fromisoformat(last_completed_str)
        # However, if system was just set up, we might want to check gaps?
        # For simplicity, we assume continuous history from last checkpoint.
        start_scan = last_completed
    else:
        # First run: 90 days lookback (~13 weeks)
        # Start from 13 weeks ago relative to current completed week
        start_scan = current_completed - timedelta(weeks=13)
        logger.info(f"First run detected. Backfilling from {start_scan}")

    weeks = list(utils_date.generate_weekly_ranges(start_scan, current_completed))
    return weeks

def process_symbol_week(symbol: str, start_dt: datetime, end_dt: datetime) -> bool:
    """
    Downloads and processes ONE week for ONE symbol.
    Target file: raw/<SYMBOL>/<START>_to_<END>.parquet
    """
    # 1. Prepare Directory
    symbol_dir = config.RAW_DATA_DIR / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    
    # 2. Construct Filename (UTC+3 based naming)
    # Format: YYYY-MM-DD_HH-mm
    fname_start = utils_date.format_filename_ts(start_dt)
    fname_end = utils_date.format_filename_ts(end_dt)
    filename = f"{fname_start}_to_{fname_end}.parquet"
    target_path = symbol_dir / filename
    
    # 3. SAFETY CHECK: EXISTS?
    if target_path.exists():
        logger.debug(f"Skipping {symbol} week {filename} - Already exists.")
        return True # Considered success (already done)
        
    # 4. Identify Daily Dats (UTC)
    # Week is Sun 03:00 UTC+3 -> Sun 03:00 UTC+3
    # This equals Sun 00:00 UTC -> Sun 00:00 UTC
    # So we need exactly 7 daily files: Sun, Mon, Tue, Wed, Thu, Fri, Sat.
    
    # Calculate days
    # start_dt is Sun 03:00 IST = Sun 00:00 UTC
    # So we can just take the date part of start_dt (if converted to UTC)
    # But start_dt IS timezone aware IST.
    
    start_utc = start_dt.astimezone(pytz.utc)
    
    daily_dfs = []
    failed = False
    
    # Iterate 7 days
    for i in range(7):
        target_day = start_utc + timedelta(days=i)
        date_str = target_day.strftime("%Y-%m-%d") # UTC date string
        
        # Download
        zip_name = f"{symbol}-aggTrades-{date_str}.zip"
        zip_path = config.DATA_DIR / "temp" / zip_name # Use a temp dir!
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        
        url = downloader.construct_binance_vision_url(symbol, date_str)
        
        if downloader.download_file(url, zip_path):
            csv_path = downloader.extract_zip(zip_path, zip_path.parent)
            if csv_path:
                try:
                    df = processor.process_single_day(csv_path)
                    if df is not None:
                        daily_dfs.append(df)
                    else:
                        # Empty data for day?
                        pass
                except Exception as e:
                    logger.error(f"Error processing {symbol} {date_str}: {e}")
                    failed = True
                finally:
                    if csv_path.exists(): csv_path.unlink()
            else:
                failed = True
            
            if zip_path.exists(): zip_path.unlink()
        else:
            # Download failed (404?)
            # Valid scenario for new symbols or delisted
            # If 404, we assume no data for that day.
            pass

    if failed:
        logger.warning(f"Failed incompletely for {symbol} week {filename}")
        return False

    if not daily_dfs:
        # No data at all for this week? (Maybe symbol didn't exist yet)
        return True # Not an error, just empty. Don't create file.

    # 5. Concat and Save
    try:
        full_df = pl.concat(daily_dfs)
        full_df = full_df.sort("ts_1s")
        
        # User Optimization: Pre-calculate timeframes (5s, 10s... 1m)
        # Structure: raw/SYMBOL/TF/WEEK.parquet
        
        # Base 1s data is used for resampling
        # We can optionally save 1s data too if needed, but config.TIMEFRAMES usually excludes 1s now?
        # If user wants 1s, add it to config.TIMEFRAMES.
        
        for tf in config.TIMEFRAMES:
            # Create TF directory
            tf_dir = symbol_dir / tf
            tf_dir.mkdir(parents=True, exist_ok=True)
            
            tf_target_path = tf_dir / filename
            
            if tf_target_path.exists():
                continue

            # Resample
            # Use processor.resample_from_1s (Polars based)
            # We need to ensure processor has this function visible or implement here.
            # processor.save_parquet was used before.
            # Let's use processor.resample_from_1s if available, otherwise inline.
            # processor.py generally works with Polars now.
            
            try:
                resampled_df = processor.resample_from_1s(full_df, tf)
                resampled_df.write_parquet(tf_target_path)
            except Exception as e:
                logger.error(f"Error resampling {symbol} {tf}: {e}")
                
        return True
        
    except Exception as e:
        logger.error(f"Error saving {symbol} week: {e}")
        return False

def run_manager():
    # 1. Init
    logger.info("Starting Data Manager")
    manifest = load_manifest()
    
    # 2. Get Weeks
    weeks = get_target_weeks()
    if not weeks:
        logger.info("No new completed weeks to download.")
        return

    symbols = downloader.get_params_usdt_futures_symbols()
    
    # 3. Process
    import pytz
    for start, end in weeks:
        logger.info(f"Processing Week: {start} to {end}")
        
        # Parallel Execution
        # Adjusted to 4 workers to prevent memory issues with large 1s dataframes
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_symbol = {executor.submit(process_symbol_week, sym, start, end): sym for sym in symbols}
            
            for future in tqdm(as_completed(future_to_symbol), total=len(symbols), desc=f"Week {start.strftime('%Y-%m-%d')}"):
                sym = future_to_symbol[future]
                try:
                    success = future.result()
                except Exception as e:
                    logger.error(f"Symbol {sym} failed: {e}")
        
        # 4. Update Manifest after successful week
        # (Technically we should check if ALL succeeded? 
        # But some symbols always fail/don't exist.
        # We assume if the loop finished, the week is 'done'.)
        
        manifest['last_completed_week_end'] = end.isoformat()
        save_manifest(manifest)
        logger.info(f"Week completed and manifest updated: {end}")

if __name__ == "__main__":
    setup_logging("data_manager")
    run_manager()
