import polars as pl
import numpy as np

class PolarsEmaChain:
    """
    Turbo-Charged EMA Chain Strategy using Polars.
    Replaces both 'conditions' and 'actions' files.
    """
    def __init__(self, tp=0.04, sl=0.04, trend='BULLISH', pump_threshold=0.02, **kwargs):
        self.tp = tp
        self.sl = sl
        self.trend = trend.upper() # BULLISH, BEARISH, NONE
        self.pump_threshold = pump_threshold
        self.ema_periods = [9, 20, 50, 100, 200, 300, 500, 1000, 2000, 5000]

    def process_file(self, filepath: str):
        """
        Reads parquet, calculates indicators, generates signals.
        Returns a list of Trade Dicts (Simulated).
        """
        try:
            # 1. Read Data (Scan Parquet is faster)
            df = pl.read_parquet(filepath)
            if df.is_empty(): return []
            
            # Ensure sorting
            if 'open_time' in df.columns:
                df = df.sort('open_time')
            elif 'ts_1s' in df.columns:
                df = df.sort('ts_1s')
                
            # 2. Add EMA Columns
            # Polars ewm_mean is available in recent versions
            exprs = []
            for p in self.ema_periods:
                exprs.append(pl.col("close").ewm_mean(span=p, adjust=False, min_periods=p).alias(f"ema_{p}"))
            
            df = df.with_columns(exprs)
            
            # 3. Define Logic Expressions
            
            trend_condition = None
            
            if self.trend == 'BULLISH':
                # Small Bull: 9 > 20 > 50 > 100 > 200
                small_bull = (
                    (pl.col("ema_9") > pl.col("ema_20") * 1.0000001) &
                    (pl.col("ema_20") > pl.col("ema_50") * 1.0000001) &
                    (pl.col("ema_50") > pl.col("ema_100") * 1.0000001) &
                    (pl.col("ema_100") > pl.col("ema_200") * 1.0000001)
                )
                # Big Bull: 300 > 500 > 1000 > 2000 > 5000
                big_bull = (
                    (pl.col("ema_300") > pl.col("ema_500") * 1.000001) &
                    (pl.col("ema_500") > pl.col("ema_1000") * 1.000001) &
                    (pl.col("ema_1000") > pl.col("ema_2000") * 1.000001) &
                    (pl.col("ema_2000") > pl.col("ema_5000") * 1.000001)
                )
                trend_condition = small_bull & big_bull
                
            elif self.trend == 'BEARISH':
                 # Small Bear: 9 < 20 < 50 < 100 < 200
                small_bear = (
                    (pl.col("ema_9") < pl.col("ema_20") * 0.9999999) &
                    (pl.col("ema_20") < pl.col("ema_50") * 0.9999999) &
                    (pl.col("ema_50") < pl.col("ema_100") * 0.9999999) &
                    (pl.col("ema_100") < pl.col("ema_200") * 0.9999999)
                )
                # Big Bear: 300 < 500 < 1000 < 2000 < 5000
                big_bear = (
                    (pl.col("ema_300") < pl.col("ema_500") * 0.999999) &
                    (pl.col("ema_500") < pl.col("ema_1000") * 0.999999) &
                    (pl.col("ema_1000") < pl.col("ema_2000") * 0.999999) &
                    (pl.col("ema_2000") < pl.col("ema_5000") * 0.999999)
                )
                trend_condition = small_bear & big_bear
                
            else: # NONE or any other string
                # Always True (No trend filter)
                trend_condition = pl.lit(True)

            
            # --- Pump & Marubozu ---
            # Pump > Threshold
            # Open != 0
            pump_pct = (pl.col("close") - pl.col("open")) / pl.col("open")
            is_pump = (pump_pct > self.pump_threshold)
            
            # Marubozu > 80%
            body = (pl.col("close") - pl.col("open")).abs()
            rng = pl.col("high") - pl.col("low")
            marubozu_ratio = body / rng
            is_marubozu = (rng > 0) & (marubozu_ratio >= 0.80)
            
            # --- FINAL TRIGGER ---
            # Trend + Pump + Marubozu -> ENTRY
            signal = trend_condition & is_pump & is_marubozu
            
            # Add signal column to DF
            df = df.with_columns(signal.alias("entry_signal"))
            
            # Return Full DF (needed for Backtest Engine to simulate exits)
            return df
            
        except Exception as e:
            # print(f"Error {filepath}: {e}")
            return None
