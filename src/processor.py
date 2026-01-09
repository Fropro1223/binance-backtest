import pandas as pd
import polars as pl
from pathlib import Path
from typing import Dict, List
import logging
from src import config

logger = logging.getLogger("processor")

def load_agg_trades(csv_path: Path) -> pl.DataFrame:
    """
    Reads aggTrades CSV.
    Columns: [agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker]
    """
    # Use Polars for speed
    try:
        # Check for header
        with open(csv_path, 'r') as f:
            first_line = f.readline()
        
        has_header = False
        # If the first extracted token is not a digit, assume header
        # agg_trade_id is numeric.
        if first_line and not first_line.split(',')[0].strip().isdigit():
             has_header = True

        df = pl.read_csv(
            csv_path,
            has_header=has_header,
            new_columns=["id", "price", "qty", "first_id", "last_id", "time", "is_buyer_maker"]
        )
        return df
    except Exception as e:
        logger.error(f"Failed to read CSV {csv_path}: {e}")
        raise

def compute_1s_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    """
     aggregations:
    - Open: first price
    - High: max price
    - Low: min price
    - Close: last price
    - Volume: sum(qty)
    """
    # Convert ms timestamp to datetime
    df = df.with_columns(
        pl.col("time").cast(pl.Int64),
        pl.col("price").cast(pl.Float64),
        pl.col("qty").cast(pl.Float64)
    )

    df = df.with_columns(
        (pl.col("time") * 1000).cast(pl.Datetime).alias("datetime")
    )
    
    # Truncate to 1s
    df = df.with_columns(
        pl.col("datetime").dt.truncate("1s").alias("ts_1s")
    )

    # Group by 1s
    ohlcv = df.group_by("ts_1s").agg([
        pl.col("price").first().alias("open"),
        pl.col("price").max().alias("high"),
        pl.col("price").min().alias("low"),
        pl.col("price").last().alias("close"),
        pl.col("qty").sum().alias("volume")
    ]).sort("ts_1s")
    
    return ohlcv

def resample_from_1s(df_1s: pl.DataFrame, timeframe_str: str) -> pl.DataFrame:
    """
    Resamples 1s OHLCV to higher timeframes (e.g. 5s, 10s).
    df_1s must have 'ts_1s', 'open','high','low','close','volume'.
    """
    # Polars 'dynamic' or 'truncate' grouping
    # parse "5s", "15s" etc.
    if timeframe_str == "1s":
        return df_1s

    # use dynamic or resample
    # set index? Polars doesn't use index like pandas.
    # utilize sort and group_by_dynamic
    
    resampled = df_1s.group_by_dynamic("ts_1s", every=timeframe_str).agg([
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.col("volume").sum()
    ])
    
    return resampled

def save_parquet(df: pl.DataFrame, symbol: str, timeframe: str, output_dir: Path = config.PROCESSED_DATA_DIR):
    """Saves DataFrame to parquet."""
    filename = f"{symbol}_{timeframe}.parquet"
    out_path = output_dir / filename
    df.write_parquet(out_path)
    logger.info(f"Saved {out_path}") 

def process_single_day(csv_path: Path) -> pl.DataFrame:
    """Complete flow for one CSV file to 1s OHLCV."""
    raw = load_agg_trades(csv_path)
    if raw.is_empty():
        return None
    ohlcv_1s = compute_1s_ohlcv(raw)
    return ohlcv_1s
