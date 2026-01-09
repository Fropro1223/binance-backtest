import logging
import polars as pl
from tqdm import tqdm
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

from src import config, downloader, processor, utils

logger = utils.setup_logging("main")

def run_pipeline(dry_run_symbol=None, dry_run_days=None):
    """
    Main pipeline entry point.
    :param dry_run_symbol: If set, only process this specific symbol.
    :param dry_run_days: If set, only process the last N days.
    """
    logger.info("Starting Binance Futures Backtest Data Pipeline")
    
    # 1. Fetch Symbols
    if dry_run_symbol:
        symbols = [dry_run_symbol]
        logger.info(f"DRY RUN MODE: Processing only {dry_run_symbol}")
    else:
        symbols = downloader.get_params_usdt_futures_symbols()
    
    # 2. Determine Date Range
    from datetime import timezone
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    
    target_days = dry_run_days if dry_run_days else config.TARGET_DAYS
    start_date = end_date - timedelta(days=target_days - 1)
    
    logger.info(f"Targeting range: {start_date} to {end_date} ({target_days} days)")
    
    date_list = list(utils.generate_date_range(datetime.combine(start_date, datetime.min.time()), 
                                              datetime.combine(end_date, datetime.min.time())))
    date_list = [d.date() for d in date_list]
    
    # 3. Process each symbol
    for symbol in tqdm(symbols, desc="Processing Symbols"):
        logger.info(f"Start processing {symbol}")
        
        daily_dfs = []
        
        for date_obj in tqdm(date_list, desc=f"Days for {symbol}", leave=False):
            date_str = utils.format_date_to_string(date_obj)
            url = downloader.construct_binance_vision_url(symbol, date_str)
            zip_filename = f"{symbol}-aggTrades-{date_str}.zip"
            zip_path = config.RAW_DATA_DIR / zip_filename
            
            # Download
            # Optimization: check if we already processed? No, we delete raw files.
            # Only download if we don't have it.
            if not downloader.download_file(url, zip_path):
                # Download failed (e.g. 404 for new listing), skip this day
                continue
            
            # Extract
            csv_path = downloader.extract_zip(zip_path, config.RAW_DATA_DIR)
            if not csv_path:
                continue
            
            # Process to 1s OHLCV
            try:
                df = processor.process_single_day(csv_path)
                if df is not None:
                    daily_dfs.append(df)
            except Exception as e:
                logger.error(f"Error processing {csv_path}: {e}")
            finally:
                # Cleanup CSV
                if csv_path.exists():
                    csv_path.unlink()

        # Check if we have data
        if not daily_dfs:
            logger.warning(f"No data collected for {symbol}. Skipping.")
            continue
            
        # Concat all days
        logger.info(f"Concatenating {len(daily_dfs)} days for {symbol}...")
        try:
            full_df = pl.concat(daily_dfs)
            full_df = full_df.sort("ts_1s")
            
            
            # Resample and Save
            for tf in ["5s", "10s", "15s", "30s", "45s"]:
                logger.info(f"Resampling {symbol} to {tf}...")
                resampled = processor.resample_from_1s(full_df, tf)
                processor.save_parquet(resampled, symbol, tf)
                
            logger.info(f"Finished {symbol}")
            
        except Exception as e:
            logger.error(f"Failed to aggregate/save {symbol}: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="Run for specific symbol only")
    parser.add_argument("--days", type=int, help="Number of days to process")
    args = parser.parse_args()
    
    run_pipeline(dry_run_symbol=args.symbol, dry_run_days=args.days)
