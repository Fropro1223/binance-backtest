from backtest_framework import Strategy

class PumpShortStrategy(Strategy):
    def __init__(self, pump_threshold=0.02, tp=0.04, sl=0.02, bet_size=7.0, **kwargs):
        super().__init__(bet_size=bet_size)
        self.pump_threshold = pump_threshold
        self.tp = tp
        self.sl = sl

    def on_candle(self, timestamp, open, high, low, close):
        """
        Check if candle High spiked > Open * (1 + threshold)
        """
        threshold_price = open * (1 + self.pump_threshold)
        
        if high >= threshold_price:
            # Entry Logic: We assume we enter exactly AT the threshold price
            # because we are placing a limit order or market entering as soon as it crosses.
            entry_price = threshold_price
            
            pump_at_close = (close - open) / open if open != 0 else 0
            
            return {
                'action': 'SHORT',
                'entry_price': entry_price,
                'tp': entry_price * (1 - self.tp),
                'sl': entry_price * (1 + self.sl),
                'pump_percent': pump_at_close,
                'check_current_candle': True
            }
        
        return None
