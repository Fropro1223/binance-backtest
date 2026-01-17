import pandas as pd
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional
import logging
from datetime import datetime, timedelta
import pytz

from src import config

logger = logging.getLogger("processor")

# CONSTANT TIMEZONE
TZ_IST = 'Europe/Istanbul'

def load_agg_trades(csv_path: Path) -> pl.DataFrame:
    """
    Reads aggTrades CSV.
    Columns: [agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker]
    """
    try:
        # Check for header
        has_header = False
        with open(csv_path, 'r') as f:
            first_line = f.readline()
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
    Aggregates raw trades to 1s candles.
    CRITICAL: Converts to UTC+3 (Europe/Istanbul) BEFORE aggregation.
    """
    # 1. Cast types
    df = df.with_columns(
        pl.col("time").cast(pl.Int64),
        pl.col("price").cast(pl.Float64),
        pl.col("qty").cast(pl.Float64)
    )

    # 2. Convert timestamp (ms) to datetime (UTC) then to ISTANBUL
    # Binance timestamps are UTC.
    df = df.with_columns(
        (pl.col("time") * 1000).cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
            .dt.convert_time_zone(TZ_IST)
            .alias("datetime")
    )
    
    # 3. Truncate to 1s
    df = df.with_columns(
        pl.col("datetime").dt.truncate("1s").alias("ts_1s")
    )

    # 4. Group by 1s (Standard OHLCV)
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
    Resamples 1s OHLCV to higher timeframes strictly adhering to anchor rules.
    df_1s must have 'ts_1s' (datetime with TZ), and OHLCV cols.
    """
    if timeframe_str == "1s":
        return df_1s

    # Custom handling for weird TFs if needed, but Polars 'every' usually handles 10s, 1m well.
    # 45s is the tricky one.
    # Polars 'group_by_dynamic' will anchor at epoch (1970-01-01 00:00:00).
    # Since 45s divides 3 minutes (180s) evenly, it aligns with 00:00:00 if period is handled correctly.
    # Anchors: xx:xx:00, xx:xx:45.
    # This means modulo 45s? 
    # :00 -> :45 (45s)
    # :45 -> :30 (45s) -> 1m30s
    # :30 -> :15 (45s) -> 2m15s
    # :15 -> :00 (45s) -> 3m00s
    # It seems Polars default aligns to Epoch. 
    # Epoch is 1970-01-01 00:00:00 UTC. 
    # We are in UTC+3. 
    # 00:00:00 IST is 21:00:00 UTC prev day.
    # Epoch checks out.
    
    # Validation of 'every' argument
    # If timeframe_str is "45s", Polars uses 45 seconds intervals.
    
    try:
        resampled = df_1s.group_by_dynamic("ts_1s", every=timeframe_str).agg([
            pl.col("open").first(),
            pl.col("high").max(),
            pl.col("low").min(),
            pl.col("close").last(),
            pl.col("volume").sum()
        ])
        
        # Verify anchors for strict compliance?
        # User requirement: 45s anchors: xx:xx:00, xx:xx:45.
        # Wait, xx:xx:00 and xx:xx:45 implies a 45s bar starting at 00 ends at 45.
        # Next bar starts at 45.
        # If user implies ONLY 00 and 45 are allowed starts, then period must be restricted to minute boundary?
        # "1m: xx:xx:00", "45s: xx:xx:00, xx:xx:45".
        # This literally implies 2 bars per minute? 
        # 00->45 (45s duration), 45->?? (15s duration?) OR 45->90?
        # If 45->90, then 90 is 1m30s (xx:xx:30).
        # User listed "xx:xx:00, xx:xx:45" for 45s.
        # He did NOT list xx:xx:30 or xx:xx:15.
        # This suggests he wants 45s bars that strictly align to 00 and 45 of a minute?
        # That is impossible if duration is constant 45s.
        # UNLESS he means he wants bars AT 00 and AT 45.
        # If strict constant duration 45s: 00:00, 00:45, 01:30, 02:15, 03:00.
        # User list: "45s: xx:xx:00, xx:xx:45".
        # This list misses 30 and 15.
        # Maybe he means "starts at 00 or 45"?
        # Let's assume standard rolling 45s (0, 45, 1:30, 2:15...).
        # IF he means "TradingView style", TV does rolling 45s.
        # TV 45s: 09:30:00, 09:30:45, 09:31:30, 09:32:15, 09:33:00.
        # So my assumption of rolling is correct. User's list "xx:xx:00, xx:xx:45" was likely incomplete examples, not exhaustive.
        
        return resampled
    except Exception as e:
        logger.error(f"Resampling error for {timeframe_str}: {e}")
        raise

def process_single_day(csv_path: Path) -> Optional[pl.DataFrame]:
    """Complete flow for one CSV file to 1s OHLCV (IST)."""
    raw = load_agg_trades(csv_path)
    if raw is None or raw.is_empty():
        return None
    ohlcv_1s = compute_1s_ohlcv(raw)
    return ohlcv_1s
