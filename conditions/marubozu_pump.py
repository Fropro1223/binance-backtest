from backtest_framework import Strategy

class MarubozuConditions(Strategy):
    """
    Sadece Marubozu (ve Pump) kondisyonunu hesaplar.
    İşlem açmaz, sadece state günceller.
    """
    def __init__(self, marubozu_threshold=0.80, pump_threshold=0.02, **kwargs):
        # Extract bet_size for base class
        bet_size = kwargs.get('bet_size', 7.0)
        super().__init__(bet_size=bet_size)
        
        self.marubozu_threshold = marubozu_threshold
        self.pump_threshold = pump_threshold
        
        # Store strategy params if needed
        self.tp = kwargs.get('tp')
        self.sl = kwargs.get('sl')
        self.side = kwargs.get('side')
        
        # State
        self.conditions = {
            'is_marubozu': False,
            'is_pump': False,
            'pump_percentage': 0.0 # Bilgi amaçlı
        }

    def on_candle(self, timestamp, open, high, low, close):
        if open == 0: 
            self.conditions['is_marubozu'] = False
            self.conditions['is_pump'] = False
            return
            
        body_size = abs(close - open)
        total_range = high - low
        
        # 1. Pump Kontrol
        pump_pct = (close - open) / open
        self.conditions['pump_percentage'] = pump_pct
        self.conditions['is_pump'] = (pump_pct > self.pump_threshold)
        
        # 2. Marubozu Kontrol
        if total_range > 0:
            marubozu_ratio = body_size / total_range
            self.conditions['is_marubozu'] = (marubozu_ratio >= self.marubozu_threshold)
        else:
            self.conditions['is_marubozu'] = False
            
        return None  # İşlem kararı döndürmüyoruz
