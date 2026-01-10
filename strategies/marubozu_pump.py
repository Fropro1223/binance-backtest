from backtest_framework import Strategy

class MarubozuPumpStrategy(Strategy):
    def __init__(self, pump_threshold=0.02, marubozu_threshold=0.80, tp=0.04, sl=0.04, bet_size=7.0, side="SHORT", **kwargs):
        super().__init__(bet_size=bet_size)
        self.pump_threshold = pump_threshold
        self.marubozu_threshold = marubozu_threshold
        self.tp = tp
        self.sl = sl
        self.side = side.upper()

    def on_candle(self, timestamp, open, high, low, close):
        """
        Trigger if:
        1. (Close - Open) / Open > pump_threshold (2%)
        2. (Close - Open) / (High - Low) >= marubozu_threshold (80%)
        """
        # Avoid division by zero
        if open == 0: return None
        if (high - low) == 0: return None
        
        body_size = close - open
        total_range = high - low
        
        # We only care about Green candles for this specific "Pump" Short strategy
        pump_pct = body_size / open
        
        if pump_pct <= self.pump_threshold:
            return None

        # Check Marubozu Condition
        marubozu_ratio = abs(body_size) / total_range
        
        if marubozu_ratio < self.marubozu_threshold:
            return None
            
        # Entry Logic (Short at Close)
        entry_price = close
        
        if self.side == "SHORT":
            tp_price = entry_price * (1 - self.tp)
            sl_price = entry_price * (1 + self.sl)
        else: # LONG
            tp_price = entry_price * (1 + self.tp)
            sl_price = entry_price * (1 - self.sl)
        
        return {
            'action': self.side,
            'entry_price': entry_price,
            'tp': tp_price,
            'sl': sl_price,
            'pump_percent': pump_pct,
            'check_current_candle': False # Do not check SL/TP on the entry candle itself
        }
