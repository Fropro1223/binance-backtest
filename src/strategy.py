import polars as pl

def detect_signals(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds signal columns to the DataFrame based on:
    1. Candle Change > 2% (High Volatility)
    2. Marubozu Ratio > 80% (Body / Range)
    """
    # Create necessary calculations
    # Polars is lazy-friendly, but we'll do eager for simplicity in this context or use expressions
    
    # Avoid division by zero: if Open is 0 (unlikely) or High==Low
    
    df = df.with_columns([
        (pl.col("close") - pl.col("open")).abs().alias("body_size"),
        (pl.col("high") - pl.col("low")).alias("range_size"),
        ((pl.col("close") - pl.col("open")) / pl.col("open")).abs().alias("change_pct")
    ])
    
    # Calculate Marubozu Ratio safely
    df = df.with_columns(
        pl.when(pl.col("range_size") > 0)
        .then(pl.col("body_size") / pl.col("range_size"))
        .otherwise(0.0)
        .alias("marubozu_ratio")
    )
    
    # Define Signal Condition
    # 2% change -> 0.02
    # 80% marubozu -> 0.8
    
    df = df.with_columns(
        ((pl.col("change_pct") > 0.02) & (pl.col("marubozu_ratio") > 0.80)).alias("signal_entry")
    )
    
    return df
