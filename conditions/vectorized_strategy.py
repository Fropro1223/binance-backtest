"""
Vectorized EMA + Pump + Marubozu Strategy
=========================================
Polars/Pandas vektörizasyonu ile hızlı backtest.
Eski turbo_bull.py mantığı, parametreli hale getirildi.
"""

from backtest_framework import Strategy
import pandas as pd
import numpy as np
import polars as pl


class VectorizedStrategy(Strategy):
    """
    Vectorized strategy that combines:
    - EMA Chain (All Bull or All Bear)
    - Pump detection (> threshold %)
    - Marubozu detection (body >= threshold of range)
    
    Supports both LONG and SHORT sides.
    """
    
    def __init__(self, tp=0.04, sl=0.02, bet_size=7.0, side="SHORT",
                 pump_threshold=0.02, marubozu_threshold=0.80, **kwargs):
        super().__init__(bet_size=bet_size)
        self.tp = tp
        self.sl = sl
        self.side = side
        self.pump_threshold = pump_threshold
        self.marubozu_threshold = marubozu_threshold
        
        # EMA Settings
        self.periods = [9, 20, 50, 100, 200, 300, 500, 1000, 2000, 5000]
        
    def process_file(self, filepath):
        """Legacy wrapper."""
        try:
            df = pd.read_parquet(filepath)
            return self.process_data(df)
        except Exception:
            return None

    def process_data(self, df):
        """
        Vectorized processing - returns Polars DataFrame with 'entry_signal' column.
        """
        try:
            # 1. Read with Pandas (for fast EWM)
            # df is already DataFrame
            if df.empty:
                return None

            # 2. VECTORIZED EMA CALCULATION
            closes = df['close']
            opens = df['open']
            highs = df['high']
            lows = df['low']
            
            ema_dict = {}
            for p in self.periods:
                ema_dict[p] = closes.ewm(span=p, adjust=False, min_periods=p).mean()
            
            # 3. VECTORIZED CHAIN CHECK
            def check_chain(period_list, threshold_pct, bullish=True):
                mask = pd.Series(True, index=df.index)
                for i in range(len(period_list) - 1):
                    fast_p = period_list[i]
                    slow_p = period_list[i + 1]
                    
                    val_fast = ema_dict[fast_p]
                    val_slow = ema_dict[slow_p]
                    
                    if bullish:
                        threshold = val_slow * (1 + threshold_pct)
                        mask = mask & (val_fast > threshold)
                    else:
                        threshold = val_slow * (1 - threshold_pct)
                        mask = mask & (val_fast < threshold)
                return mask

            small_periods = [9, 20, 50, 100, 200]
            big_periods = [300, 500, 1000, 2000, 5000]
            
            # All Bull: small bull + big bull
            is_small_bull = check_chain(small_periods, 0.00001/100.0, bullish=True)
            is_big_bull = check_chain(big_periods, 0.0001/100.0, bullish=True)
            all_bull = is_small_bull & is_big_bull
            
            # All Bear: small bear + big bear
            is_small_bear = check_chain(small_periods, 0.0001/100.0, bullish=False)
            is_big_bear = check_chain(big_periods, 0.001/100.0, bullish=False)
            all_bear = is_small_bear & is_big_bear
            
            # 4. PUMP CALCULATION
            pump_pct = (closes - opens) / opens
            is_pump_up = pump_pct > self.pump_threshold    # Green candle pump
            is_pump_down = pump_pct < -self.pump_threshold  # Red candle dump
            
            # 5. MARUBOZU CALCULATION
            body_size = (closes - opens).abs()
            total_range = highs - lows
            marubozu_ratio = pd.Series(0.0, index=df.index)
            valid_range = total_range > 0
            marubozu_ratio[valid_range] = body_size[valid_range] / total_range[valid_range]
            is_marubozu = marubozu_ratio >= self.marubozu_threshold
            
            # 6. COMBINE SIGNALS BASED ON SIDE
            if self.side == "SHORT":
                # SHORT: All Bear + Pump Up + Marubozu (continuation short after pump in bear trend)
                final_signal = all_bear & is_pump_up & is_marubozu
            else:  # LONG
                # LONG: All Bull + Pump Up + Marubozu (continuation long in bull trend)
                final_signal = all_bull & is_pump_up & is_marubozu
            
            if not final_signal.any():
                return None
                
            # 7. Return Polars DataFrame
            df['entry_signal'] = final_signal
            
            pl_df = pl.from_pandas(df)
            
            return pl_df
            
        except Exception as e:
            # Silent fail for individual files
            return None

    def on_candle(self, timestamp, open, high, low, close):
        # Not used in vectorized mode
        pass
